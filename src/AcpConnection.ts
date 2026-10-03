import * as ResultValue from "effect/Result"
import * as Json from "./internal/json.ts"
/**
 * Role-neutral ACP JSON-RPC peer over the `AcpTransport` service.
 *
 * **Details**
 *
 * Either side may send requests and notifications while others are pending.
 * Responses correlate by ID per direction, so an incoming and an outgoing
 * request may share an ID. Incoming requests run in their own scoped fibers;
 * incoming notifications are handled one at a time in arrival order.
 */
import * as Context from "effect/Context"
import * as Layer from "effect/Layer"
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as FiberSet from "effect/FiberSet"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import {
  AcpCapacityError,
  AcpConnectionClosed,
  AcpProtocolError,
  AcpRemoteError,
  type AcpRequestError,
  AcpTimeoutError,
  type AcpTransportError
} from "./AcpError.ts"
import { ErrorCode, type NotificationMethod, type Params, type RequestId, type RequestMethod, type Result } from "./AcpSchema.ts"
import { AcpTransport } from "./AcpTransport.ts"
import * as JsonRpc from "./internal/jsonRpc.ts"

// -----------------------------------------------------------------------------
// Incoming handlers
// -----------------------------------------------------------------------------

/**
 * Remote request identity supplied to an incoming request handler.
 *
 * @category models
 */
export interface RequestContext {
  /**
   * The remote peer's ID for this request.
   */
  readonly id: RequestId
  /**
   * Runs an acceptance boundary whose successful value becomes the authoritative RPC result.
   *
   * Use for an irreversible insertion that must remain acknowledged after cancellation. The
   * supplied effect remains interruptible by connection shutdown. Request cancellation waits
   * until acceptance finishes, because an opaque transaction may already have inserted the
   * message. Keep cancellable preparation and foreground work outside this boundary. Later
   * handler failures or request cancellation cannot replace the committed result.
   */
  readonly commitResult: <A, R>(effect: Effect.Effect<A, AcpRemoteError, R>) => Effect.Effect<A, AcpRemoteError, R>
}

/**
 * Raw incoming dispatch. Returning `undefined` means the method is not supported: requests receive
 * Method not found, notifications are ignored. A request handler fails with `AcpRemoteError` to
 * send that error; defects become Internal error responses without their details.
 *
 * @category models
 */
export interface Handlers {
  /**
   * Dispatches an incoming request; undefined means the method is unsupported.
   */
  readonly request?: (
    method: string,
    params: unknown,
    context: RequestContext
  ) => Effect.Effect<unknown, AcpRemoteError> | undefined
  /**
   * Dispatches an incoming notification; undefined means it is ignored.
   */
  readonly notification?: (method: string, params: unknown) => Effect.Effect<void> | undefined
}

/**
 * Decoded request or notification route used to build incoming dispatch.
 *
 * @category models
 */
export type Route =
  | {
    readonly _tag: "Request"
    readonly method: string
    readonly run: (params: unknown, context: RequestContext) => Effect.Effect<unknown, AcpRemoteError>
  }
  | { readonly _tag: "Notification"; readonly method: string; readonly run: (params: unknown) => Effect.Effect<void> }

/**
 * Creates an incoming request route that decodes parameters and encodes the handler's result.
 *
 * **Details**
 *
 * The handler receives decoded parameters and the remote request identity. Its typed
 * `AcpRemoteError` is sent to the peer.
 *
 * **Gotchas**
 *
 * Invalid parameters produce an Invalid params response. A result that fails encoding produces an
 * Internal error response.
 *
 * @see {@link handlers} for combining routes into incoming dispatch.
 * @category routing
 */
export const onRequest = <D extends RequestMethod>(
  method: D,
  handler: (params: Params<D>, context: RequestContext) => Effect.Effect<Result<D>, AcpRemoteError>
): Route => ({
  _tag: "Request",
  method: method.method,
  run: (params, context) =>
    Schema.decodeUnknownEffect(method.params)(params).pipe(
      Effect.mapError((error) =>
        new AcpRemoteError({ code: ErrorCode.InvalidParams, message: "Invalid params", data: { details: error.message } })
      ),
      Effect.flatMap((decoded) => handler(decoded, context)),
      Effect.flatMap((result) => Schema.encodeUnknownEffect(method.result)(result).pipe(
        Effect.mapError(() => new AcpRemoteError({ code: ErrorCode.InternalError, message: "Internal error" }))
      ))
    )
})

/**
 * Handles a declared notification method. Notifications with invalid params are dropped and logged.
 *
 * @category routing
 */
export const onNotification = <D extends NotificationMethod>(
  method: D,
  handler: (params: Params<D>) => Effect.Effect<void>
): Route => ({
  _tag: "Notification",
  method: method.method,
  run: (params) =>
    Schema.decodeUnknownEffect(method.params)(params).pipe(
      Effect.matchEffect({
        onFailure: (error) => Effect.logWarning(`Dropped ${method.method} notification with invalid params`, error.message),
        onSuccess: handler
      })
    )
})

/**
 * Combines typed routes with optional raw fallback handlers.
 *
 * **Details**
 *
 * Fallback dispatch is used only when no route matches the method and message kind.
 *
 * **Gotchas**
 *
 * When multiple routes share the same method and kind, the last one wins. A matched route that
 * fails validation does not fall back.
 *
 * @see {@link onRequest} for request parameter decoding and result encoding.
 * @see {@link onNotification} for notification parameter decoding.
 * @category routing
 */
export const handlers = (routes: ReadonlyArray<Route>, fallback?: Handlers): Handlers => {
  const requests = new Map<string, Extract<Route, { _tag: "Request" }>>()
  const notifications = new Map<string, Extract<Route, { _tag: "Notification" }>>()
  for (const route of routes) {
    if (route._tag === "Request") requests.set(route.method, route)
    else notifications.set(route.method, route)
  }
  return {
    request: (method, params, context) =>
      requests.get(method)?.run(params, context) ?? fallback?.request?.(method, params, context),
    notification: (method, params) => notifications.get(method)?.run(params) ?? fallback?.notification?.(method, params)
  }
}

// -----------------------------------------------------------------------------
// Connection
// -----------------------------------------------------------------------------

/**
 * Incoming dispatch and bounded request and notification capacities for a JSON-RPC connection.
 *
 * @category configuration
 */
export interface Options {
  /**
   * Incoming dispatch. Incoming messages wait until handlers are installed.
   */
  readonly handlers?: Handlers | undefined
  /**
   * Outgoing requests awaiting a response. Default 1024.
   */
  readonly maxPendingRequests?: number | undefined
  /**
   * Incoming requests (including invalid entries) whose responses are not yet written. Exceeding it
   * terminates the connection. Default 256.
   */
  readonly maxIncomingRequests?: number | undefined
  /**
   * Notifications buffered before reading pauses. Default 256.
   */
  readonly notificationBuffer?: number | undefined
}

/**
 * Local deadline for sending a request and awaiting its response.
 *
 * **Gotchas**
 *
 * A timeout does not send cancellation to the peer and cannot confirm whether remote work stopped.
 *
 * @category configuration
 */
export interface RequestOptions {
  /**
   * Local deadline covering sending and waiting. Elapsing fails with `AcpTimeoutError` ; no
   * cancellation is sent to the peer.
   */
  readonly timeout?: Duration.Input | undefined
}

/**
 * An outgoing request that has been written.
 *
 * @category models
 */
export interface PendingRequest {
  /**
   * Local request identifier retained until the peer answers or the connection closes.
   */
  readonly id: RequestId
  /**
   * Wire method name of the request already sent.
   */
  readonly method: string
  /**
   * The raw result. Interrupting this wait only stops waiting: the request stays correlated (and
   * counts against capacity) until answered or closed.
   */
  readonly response: Effect.Effect<unknown, AcpRemoteError | AcpProtocolError | AcpConnectionClosed>
}

/**
 * Role-neutral JSON-RPC peer with typed and raw requests, notifications, and incoming dispatch.
 *
 * **When to use**
 *
 * Use when building protocol integrations that need direct control of methods and handlers. Session
 * lifecycle operations are available on the application client API.
 *
 * @category models
 */
export interface Service {
  /**
   * Sends a declared request and decodes its result.
   */
  readonly request: <D extends RequestMethod>(
    method: D,
    params: Params<D>,
    options?: RequestOptions
  ) => Effect.Effect<Result<D>, AcpRequestError | AcpTimeoutError>
  /**
   * Sends a raw request and waits for its raw result. A `timeout` covers both sending (which may
   * wait on backpressure) and waiting; if it elapses before the request is sent, nothing is sent
   * and `requestId` is null.
   */
  readonly requestRaw: (
    method: string,
    params?: unknown,
    options?: RequestOptions
  ) => Effect.Effect<unknown, AcpRequestError | AcpTimeoutError>
  /**
   * Writes a raw request and returns once it is sent.
   */
  readonly send: (
    method: string,
    params?: unknown
  ) => Effect.Effect<PendingRequest, AcpConnectionClosed | AcpCapacityError | AcpProtocolError>
  /**
   * Sends a declared notification.
   */
  readonly notify: <D extends NotificationMethod>(
    method: D,
    params: Params<D>
  ) => Effect.Effect<void, AcpConnectionClosed | AcpProtocolError>
  /**
   * Sends a raw notification.
   */
  readonly notifyRaw: (method: string, params?: unknown) => Effect.Effect<void, AcpConnectionClosed | AcpProtocolError>
  /**
   * Asks the peer to cancel an outgoing request (`$/cancel_request`). The request remains pending
   * until the peer responds, typically with a Request cancelled error.
   */
  readonly cancelRequest: (id: RequestId) => Effect.Effect<void, AcpConnectionClosed | AcpProtocolError>
  /**
   * Installs or replaces incoming handlers and releases waiting incoming messages.
   */
  readonly setHandlers: (handlers: Handlers) => Effect.Effect<void>
  /**
   * Waits for notifications already queued to finish dispatching. Never call from a notification
   * handler.
   */
  readonly drainNotifications: Effect.Effect<void, AcpConnectionClosed>
  /**
   * Number of outgoing requests awaiting a response.
   */
  readonly pendingRequests: Effect.Effect<number>
  /**
   * Completes with the terminal reason once the connection has terminated.
   */
  readonly closed: Effect.Effect<AcpConnectionClosed>
}

const closedByTransport = (error: AcpTransportError) =>
  new AcpConnectionClosed({ message: `Transport failed: ${error.message}`, cause: error })

/**
 * Starts a peer on `transport` . Closing the scope terminates the connection: pending calls fail
 * with `AcpConnectionClosed` and handler fibers are interrupted. The transport itself is released
 * by its own scope.
 *
 * @category constructors
 */
export const make = Effect.fnUntraced(function*(options: Options = {}) {
  const transport = yield* AcpTransport
  const scope = yield* Scope.Scope
  const maxPending = options.maxPendingRequests ?? 1024
  const maxIncoming = options.maxIncomingRequests ?? 256

  let nextId = 0
  let terminated: AcpConnectionClosed | undefined
  let current = options.handlers
  let inFlight = 0
  const pending = new Map<RequestId, Deferred.Deferred<unknown, AcpRemoteError | AcpProtocolError | AcpConnectionClosed>>()
  const active = new Map<RequestId, Deferred.Deferred<void>>()
  const done = yield* Deferred.make<AcpConnectionClosed>()
  const ready = yield* Deferred.make<void>()
  if (current) yield* Deferred.succeed(ready, undefined)
  const fibers = yield* FiberSet.make<void, never>()
  const notifications = yield* Queue.bounded<
    | { readonly _tag: "Notification"; readonly method: string; readonly params: unknown }
    | { readonly _tag: "Barrier"; readonly completed: Deferred.Deferred<void> }
  >(
    options.notificationBuffer ?? 256
  )

  const terminate = (reason: AcpConnectionClosed) =>
    Effect.uninterruptibleMask((restore) => Effect.suspend(() => {
      if (terminated) return Effect.void
      terminated = reason
      const waiting = [...pending.values()]
      pending.clear()
      return Effect.forEach(waiting, (deferred) => Deferred.fail(deferred, reason), { discard: true }).pipe(
        Effect.andThen(Deferred.succeed(done, reason)),
        Effect.andThen(Queue.shutdown(notifications)),
        Effect.andThen(restore(FiberSet.clear(fibers)))
      )
    }))
  // Used from fibers inside `fibers`, which must not interrupt themselves.
  const terminateLater = (reason: AcpConnectionClosed) => Effect.asVoid(Effect.forkIn(terminate(reason), scope))

  const write = (message: JsonRpc.Outgoing | ReadonlyArray<JsonRpc.Outgoing>): Effect.Effect<void, AcpConnectionClosed | AcpProtocolError> =>
    Effect.suspend((): Effect.Effect<void, AcpConnectionClosed | AcpProtocolError | AcpTransportError> =>
      terminated ? Effect.fail(terminated) : Json.encode(message).pipe(
        Effect.mapError((cause) => new AcpProtocolError({ message: "Cannot encode JSON-RPC message", cause })),
        Effect.flatMap(transport.send)
      )
    ).pipe(
      Effect.catchTag("AcpTransportError", (error) => {
        const reason = closedByTransport(error)
        return Effect.andThen(terminateLater(reason), Effect.fail(reason))
      })
    )

  // --- outgoing ---------------------------------------------------------------

  const send = (method: string, params?: unknown): Effect.Effect<PendingRequest, AcpConnectionClosed | AcpCapacityError | AcpProtocolError> =>
    Effect.suspend((): Effect.Effect<PendingRequest, AcpConnectionClosed | AcpCapacityError | AcpProtocolError> => {
      if (terminated) return Effect.fail(terminated)
      if (pending.size >= maxPending) {
        return Effect.fail(new AcpCapacityError({ resource: "pendingRequests", limit: maxPending }))
      }
      const id = nextId++
      const deferred = Deferred.makeUnsafe<unknown, AcpRemoteError | AcpProtocolError | AcpConnectionClosed>()
      pending.set(id, deferred)
      return write(JsonRpc.request(id, method, params)).pipe(
        Effect.onExit((exit) =>
          Exit.isSuccess(exit) ? Effect.void : Effect.sync(() => {
            if (pending.get(id) === deferred) pending.delete(id)
          })
        ),
        Effect.as<PendingRequest>({ id, method, response: Deferred.await(deferred) })
      )
    })

  const requestRaw = (method: string, params?: unknown, requestOptions?: RequestOptions) =>
    Effect.suspend(() => {
      let sentId: RequestId = null
      const exchange = Effect.flatMap(send(method, params), (sent) => {
        sentId = sent.id
        return sent.response
      })
      // The deadline covers a send blocked on backpressure as well as the wait.
      return requestOptions?.timeout === undefined ? exchange : Effect.timeoutOrElse(exchange, {
        duration: requestOptions.timeout,
        orElse: () => Effect.fail(new AcpTimeoutError({ method, requestId: sentId }))
      })
    })

  const request = <M extends string, P, A>(method: RequestMethod<M, P, A>, params: P, requestOptions?: RequestOptions) =>
    Effect.gen(function*() {
      const encoded = yield* Schema.encodeUnknownEffect(method.params)(params).pipe(
        Effect.mapError((error) => new AcpProtocolError({ message: `Invalid ${method.method} params: ${error.message}` }))
      )
      const raw = yield* requestRaw(method.method, encoded, requestOptions)
      return yield* Schema.decodeUnknownEffect(method.result)(raw).pipe(
        Effect.mapError((error) =>
          new AcpProtocolError({ message: `Invalid ${method.method} result: ${error.message}`, cause: error })
        )
      )
    })

  const notifyRaw = (method: string, params?: unknown) => write(JsonRpc.notification(method, params))

  const notify = <D extends NotificationMethod>(method: D, params: Params<D>) =>
    Schema.encodeUnknownEffect(method.params)(params).pipe(
      Effect.mapError((error) => new AcpProtocolError({ message: `Invalid ${method.method} params: ${error.message}` })),
      Effect.flatMap((encoded) => notifyRaw(method.method, encoded))
    )

  // --- incoming ---------------------------------------------------------------

  const settle = (id: RequestId, settleWith: (deferred: Deferred.Deferred<unknown, AcpRemoteError | AcpProtocolError | AcpConnectionClosed>) => Effect.Effect<boolean>) =>
    Effect.suspend(() => {
      const deferred = pending.get(id)
      if (!deferred) return Effect.logWarning("Ignored response for unknown request id", id)
      pending.delete(id)
      return Effect.asVoid(settleWith(deferred))
    })

  /**
   * Registers an incoming request and returns the effect producing its response.
   */
  const admitRequest = (id: RequestId, method: string, params: unknown): Effect.Effect<JsonRpc.Outgoing> => {
    if (active.has(id)) {
      return Effect.succeed(JsonRpc.failure(id, ErrorCode.InvalidRequest, "Duplicate request id"))
    }
    const cancelled = Deferred.makeUnsafe<void>()
    active.set(id, cancelled)
    let committed: JsonRpc.Outgoing | undefined
    let accepting = false
    let acceptanceStarted = false
    const acceptanceDone = Deferred.makeUnsafe<void>()
    const handlerStarted = Deferred.makeUnsafe<void>()
    const context: RequestContext = {
      id,
      commitResult: (effect) => Effect.uninterruptibleMask((restore) => Effect.suspend(() => {
        if (acceptanceStarted) return Effect.fail(new AcpRemoteError({ code: ErrorCode.InternalError, message: "Result already committed" }))
        acceptanceStarted = true
        accepting = true
        return restore(effect).pipe(
          Effect.flatMap((result) => {
            const message = JsonRpc.success(id, result)
            return Json.encode(message).pipe(
              Effect.mapError(() => new AcpRemoteError({ code: ErrorCode.InternalError, message: "Internal error" })),
              Effect.andThen(Effect.sync(() => { committed = message })),
              Effect.as(result)
            )
          }),
          Effect.ensuring(Effect.andThen(Effect.sync(() => { accepting = false }), Deferred.succeed(acceptanceDone, undefined)))
        )
      }))
    }
    // Handler construction happens inside the protected effect, so a handler
    // that throws synchronously still yields an Internal error response.
    const handle = Deferred.await(ready).pipe(
      Effect.andThen(Effect.suspend(() => {
        const effect = current?.request?.(method, params, context)
        return effect === undefined
          ? Effect.succeed(JsonRpc.failure(id, ErrorCode.MethodNotFound, "Method not found"))
          : Effect.map(effect, (result) => JsonRpc.success(id, result))
      })),
      Effect.catchTag("AcpRemoteError", (error) => Effect.succeed(Number.isInteger(error.code)
        ? JsonRpc.failure(id, error.code, error.message, error.data)
        : JsonRpc.failure(id, ErrorCode.InternalError, "Internal error"))),
      Effect.flatMap((message) => Json.encode(message).pipe(
        Effect.as(message),
        Effect.catchTag("SchemaError", (error) => Effect.logError(`Handler for ${method} returned non-serializable data`, error).pipe(
          Effect.as(JsonRpc.failure(id, ErrorCode.InternalError, "Internal error"))
        ))
      ))
    )
    // A handler can terminate with interruption without closing the
    // connection. Observe its exit as a value so the response fiber survives.
    const completed = Effect.gen(function*() {
      const fiber = yield* Effect.forkChild(handle, { startImmediately: true })
      yield* Deferred.succeed(handlerStarted, undefined)
      return yield* Fiber.await(fiber).pipe(
        Effect.flatMap((exit) => {
          if (Exit.isSuccess(exit)) return Effect.succeed(exit.value)
          if (Cause.hasInterruptsOnly(exit.cause)) {
            return Effect.succeed(JsonRpc.failure(id, ErrorCode.RequestCancelled, "Request cancelled"))
          }
          return Effect.as(Effect.logError(`Handler for ${method} failed`, exit.cause),
            JsonRpc.failure(id, ErrorCode.InternalError, "Internal error"))
        }),
        Effect.ensuring(Effect.asVoid(Effect.forkDetach(Fiber.interrupt(fiber))))
      )
    })
    const cancel = Effect.gen(function*() {
      yield* Deferred.await(cancelled)
      yield* Deferred.await(handlerStarted)
      if (accepting) {
        yield* Deferred.await(acceptanceDone)
      }
      return committed ?? JsonRpc.failure(id, ErrorCode.RequestCancelled, "Request cancelled")
    })
    return Effect.raceFirst(completed, cancel).pipe(
      Effect.map((message) => committed ?? message),
      Effect.ensuring(Effect.sync(() => active.delete(id)))
    )
  }

  /**
   * Handles one message; returns the effect producing its response, if one is owed.
   */
  const handleEntry = (entry: Schema.Json): Effect.Effect<Effect.Effect<JsonRpc.Outgoing> | undefined> =>
    Effect.suspend(() => {
      const message = JsonRpc.classify(entry)
      switch (message._tag) {
        case "Response":
          return Effect.as(settle(message.id, (d) => Deferred.succeed(d, message.result)), undefined)
        case "ErrorResponse": {
          const { code, message: text, data } = message.error
          return Effect.as(settle(message.id, (d) => Deferred.fail(d, new AcpRemoteError({ code, message: text, data }))), undefined)
        }
        case "InvalidResponse":
          return message.id === undefined
            ? Effect.as(Effect.logWarning("Ignored malformed response", message.reason), undefined)
            : Effect.as(
              settle(message.id, (d) => Deferred.fail(d, new AcpProtocolError({ message: `Malformed response: ${message.reason}` }))),
              undefined
            )
        case "Notification": {
          if (message.method === "$/cancel_request") {
            const target = typeof message.params === "object" && message.params !== null && "requestId" in message.params ? message.params.requestId : undefined
            const cancelled = JsonRpc.isRequestId(target) ? active.get(target) : undefined
            return Effect.as(cancelled ? Deferred.succeed(cancelled, undefined) : Effect.void, undefined)
          }
          return Effect.as(Queue.offer(notifications, { _tag: "Notification", method: message.method, params: message.params }), undefined)
        }
        case "Invalid":
          return Effect.succeed(Effect.succeed(JsonRpc.failure(null, ErrorCode.InvalidRequest, "Invalid Request")))
        case "Request":
          return Effect.succeed(terminated ? undefined : admitRequest(message.id, message.method, message.params))
      }
    })

  /**
   * Forks the production and writing of owed responses, bounded by `maxIncoming`.
   */
  const respond = (responses: ReadonlyArray<Effect.Effect<JsonRpc.Outgoing>>, batch: boolean) =>
    Effect.suspend(() => {
      if (terminated || responses.length === 0) return Effect.void
      if (inFlight + responses.length > maxIncoming) {
        return terminate(new AcpConnectionClosed({ message: `Incoming request capacity (${maxIncoming}) exceeded` }))
      }
      inFlight += responses.length
      const work = Effect.all(responses, { concurrency: "unbounded" }).pipe(
        Effect.flatMap((outgoing) => write(batch ? outgoing : outgoing[0]!)),
        Effect.ignore,
        Effect.ensuring(Effect.sync(() => {
          inFlight -= responses.length
        }))
      )
      return Effect.asVoid(FiberSet.run(fibers, work))
    })

  const onFrame = (frame: string): Effect.Effect<void> =>
    Effect.suspend(() => {
      if (terminated) return Effect.void
      const decoded = Json.decodeResult(frame)
      if (ResultValue.isFailure(decoded)) {
        return respond([Effect.succeed(JsonRpc.failure(null, ErrorCode.ParseError, "Parse error"))], false)
      }
      const parsed = decoded.success
      if (!Array.isArray(parsed)) {
        return Effect.flatMap(handleEntry(parsed), (response) => respond(response ? [response] : [], false))
      }
      if (parsed.length === 0) {
        return respond([Effect.succeed(JsonRpc.failure(null, ErrorCode.InvalidRequest, "Invalid Request"))], false)
      }
      return Effect.forEach(parsed, handleEntry).pipe(
        Effect.flatMap((responses) => respond(responses.filter((r) => r !== undefined), true))
      )
    })

  const dispatchNotifications = Queue.take(notifications).pipe(
    Effect.flatMap((entry) => {
      if (entry._tag === "Barrier") return Deferred.succeed(entry.completed, undefined)
      const { method, params } = entry
      return Deferred.await(ready).pipe(
        Effect.andThen(Effect.gen(function*() {
          const handler = Effect.suspend(() => current?.notification?.(method, params) ?? Effect.void)
          const fiber = yield* Effect.forkChild(handler, { startImmediately: true })
          const exit = yield* Fiber.await(fiber)
          if (Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause)) {
            yield* Effect.logWarning(`Notification handler for ${method} failed`, exit.cause)
          }
        }))
      )
    }),
    Effect.forever,
    Effect.ignore
  )

  const reader = transport.incoming.pipe(
    // Reading stops as soon as termination begins.
    Stream.interruptWhen(Deferred.await(done)),
    Stream.runForEach(onFrame),
    Effect.matchCauseEffect({
      onSuccess: () => terminate(new AcpConnectionClosed({ message: "Transport closed" })),
      onFailure: (cause) => terminate(new AcpConnectionClosed({ message: "Transport failed", cause: Cause.squash(cause) }))
    })
  )

  yield* Effect.forkScoped(reader)
  // In `fibers` so terminal cleanup interrupts a running notification handler.
  yield* FiberSet.run(fibers, dispatchNotifications)
  // Added last so it runs first: pending calls fail before fibers are interrupted.
  yield* Scope.addFinalizer(scope, terminate(new AcpConnectionClosed({ message: "Connection closed" })))

  const connection: Service = {
    request,
    requestRaw,
    send,
    notify,
    notifyRaw,
    cancelRequest: (id) => notifyRaw("$/cancel_request", { requestId: id }),
    setHandlers: (handlers) =>
      Effect.suspend(() => {
        current = handlers
        return Effect.asVoid(Deferred.succeed(ready, undefined))
      }),
    drainNotifications: Effect.raceFirst(Effect.gen(function*() {
      const completed = yield* Deferred.make<void>()
      yield* Queue.offer(notifications, { _tag: "Barrier", completed })
      yield* Deferred.await(completed)
    }), Effect.flatMap(Deferred.await(done), Effect.fail)),
    pendingRequests: Effect.sync(() => pending.size),
    closed: Deferred.await(done)
  }
  return connection
})

/**
 * The role-neutral JSON-RPC connection capability.
 *
 * @category services
 */
export class AcpConnection extends Context.Service<AcpConnection, Service>()("effect-acp/AcpConnection") {}

/**
 * Starts a connection over the supplied scoped transport service.
 *
 * @category layers
 */
export const layer = (options?: Options): Layer.Layer<AcpConnection, never, AcpTransport> =>
  Layer.effect(AcpConnection, make(options))
