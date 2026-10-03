import { InteractionRequest, type InteractionOutcome } from "./AcpApp.ts"
import * as V1 from "./protocol/v1/Schema.ts"
import * as V2 from "./protocol/v2/Schema.ts"
import * as Result from "effect/Result"
import * as Json from "./internal/json.ts"
import * as Option from "effect/Option"
/**
 * `AcpClient` over a direct `AcpConnector` transport.
 *
 * **Details**
 *
 * This module owns everything the pure reducer deliberately does not: event
 * ordering, publication, request correlation, interaction fibers, and
 * resource bounds. The rules it enforces are the ones that are impossible to
 * get right at the call site:
 *
 * - Session routing is installed *before* `session/new`/`session/resume` is
 *   dispatched, so updates that arrive ahead of the response are retained
 *   (bounded) and replayed into the established session.
 * - A submission registers its correlation before the prompt is written, so
 *   an update referencing it cannot arrive "too early".
 * - Interaction handlers park on a deferred, never on the connection reader,
 *   so a human taking their time does not stall unrelated traffic.
 * - Observers get bounded delivery; falling behind fails that observer with
 *   `AcpSubscriptionOverflow` rather than growing memory without bound or
 *   stalling the reader.
 */
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Scope from "effect/Scope"
import * as Schema from "effect/Schema"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import type {
  Capabilities,
  ContentLimits,
  InteractionSnapshot,
  ProvisionalLimits,
  SessionSnapshot,
  SubmissionFailure,
  SubmissionSnapshot
} from "./AcpApp.ts"
import { defaultContentLimits, defaultProvisionalLimits } from "./AcpApp.ts"
import type {
  AcpAgentConnection,
  AcpSession,
  ConnectOptions,
  InteractionResolution,
  NewSessionOptions,
  Observation,
  Observed,
  OperationError,
  ResumeSessionOptions,
  SessionListEntry,
  Submission,
} from "./AcpClient.ts"
import { AcpClient } from "./AcpClient.ts"
import * as AcpConnection from "./AcpConnection.ts"
import { AcpConnector } from "./AcpConnector.ts"
import { AcpConnectionClosed, AcpProtocolError, AcpRemoteError } from "./AcpError.ts"
import * as AcpProtocol from "./AcpProtocol.ts"
import { ErrorCode } from "./AcpSchema.ts"
import {
  AcpCancellationUnconfirmed,
  AcpCapabilityUnsupported,
  AcpHistoryUnavailable,
  AcpInteractionAlreadyResolved,
  AcpInteractionExpired,
  AcpProvisionalOverflow,
  AcpSessionBusy,
  AcpSubscriptionOverflow
} from "./AcpSessionError.ts"
import * as Capability from "./internal/capabilities.ts"
import * as State from "./AcpSessionState.ts"

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)

/** Maps a request failure onto the snapshot's failure record shape. */
const toFailure = (error: unknown): SubmissionFailure => {
  if (!isRecord(error)) return { _tag: "protocol", message: String(error) }
  switch (error["_tag"]) {
    case "AcpRemoteError":
      return {
        _tag: "remote",
        code: Number(error["code"]),
        message: String(error["message"]),
        data: error["data"]
      }
    case "AcpConnectionClosed":
      return { _tag: "closed", message: String(error["message"]) }
    case "AcpTimeoutError":
      return { _tag: "timeout" }
    case "AcpCapacityError":
      return { _tag: "capacity", resource: String(error["resource"]), limit: Number(error["limit"]) }
    default:
      return { _tag: "protocol", message: typeof error["message"] === "string" ? error["message"] : "Unknown failure" }
  }
}

// -----------------------------------------------------------------------------
// Observers
// -----------------------------------------------------------------------------

/**
 * One subscriber's bounded delivery slot.
 *
 * Delivery never suspends the publisher: when the queue is full the observer
 * is marked lost and its stream is failed. That is the trade the spec asks
 * for — an explicit resynchronization condition in exchange for never letting
 * a slow UI backpressure the protocol reader.
 */
interface Observer {
  readonly queue: Queue.Queue<Observation, AcpSubscriptionOverflow | Cause.Done>
  lost: boolean
}

// -----------------------------------------------------------------------------
// Session runtime
// -----------------------------------------------------------------------------

interface Runtime {
  readonly sessionId: string
  /** Applies one event and publishes the resulting snapshot. Serialized. */
  readonly apply: (event: State.Event) => Effect.Effect<SessionSnapshot>
  readonly current: Effect.Effect<SessionSnapshot>
  readonly observe: Effect.Effect<Observed, never, Scope.Scope>
  /** Settles a pending interaction; `false` when it was already settled. */
  readonly settle: (
    interactionId: string,
    status: "resolved" | "cancelled" | "expired",
    outcome: InteractionOutcome
  ) => Effect.Effect<boolean>
  readonly registerInteraction: (
    interaction: InteractionSnapshot,
    respond: Deferred.Deferred<InteractionOutcome>
  ) => Effect.Effect<void>
  /** Completes when the session's foreground work next ends. */
  readonly awaitIdle: Effect.Effect<void>
  readonly noteIdle: Effect.Effect<void>
}

const makeRuntime = Effect.fnUntraced(function*(
  sessionId: string,
  version: 1 | 2,
  cwd: string | null,
  limits: ContentLimits,
  observerCapacity: number
) {
  const scope = yield* Scope.Scope
  let snapshot = State.empty(sessionId, version, cwd)
  const observers = new Set<Observer>()
  const pendingInteractions = new Map<string, Deferred.Deferred<InteractionOutcome>>()
  let idleWaiters: Array<Deferred.Deferred<void>> = []
  // One reducer per session: every event is applied in arrival order, and no
  // observer can see a torn intermediate state.
  const lock = Semaphore.makeUnsafe(1)

  /**
   * Fans one snapshot out to every observer without ever suspending.
   *
   * Delivery is `offerUnsafe`, so a full queue fails immediately instead of
   * backpressuring the reducer. That observer is then terminated with
   * `AcpSubscriptionOverflow`; failing the queue works even when it is full,
   * whereas offering a marker value into it would block exactly when the
   * condition needs reporting.
   */
  const publish = (next: SessionSnapshot) =>
    Effect.sync(() => {
      const event: Observation = { _tag: "snapshot", snapshot: next }
      for (const observer of observers) {
        if (observer.lost) continue
        if (!Queue.offerUnsafe(observer.queue, event)) {
          observer.lost = true
          Queue.failCauseUnsafe(
            observer.queue,
            Cause.fail(
              new AcpSubscriptionOverflow({
                message:
                  `Observer of session ${sessionId} fell behind its delivery capacity (${observerCapacity}); acquire a new snapshot boundary`
              })
            )
          )
        }
      }
    })

  const apply = (event: State.Event): Effect.Effect<SessionSnapshot> =>
    Semaphore.withPermit(
      lock,
      Effect.suspend(() => {
        const before = snapshot
        snapshot = State.reduce(before, event, limits)
        const releasedIdle = (before.foreground.state !== "idle" && snapshot.foreground.state === "idle") ||
          (before.activeSubmissionId !== null && snapshot.activeSubmissionId === null)
        const waiters = releasedIdle ? idleWaiters : []
        if (releasedIdle) idleWaiters = []
        return publish(snapshot).pipe(
          Effect.andThen(Effect.forEach(waiters, (waiter) => Deferred.succeed(waiter, undefined), { discard: true })),
          Effect.as(snapshot)
        )
      })
    )

  const observe: Effect.Effect<Observed, never, Scope.Scope> = Effect.gen(function*() {
    const observerScope = yield* Scope.Scope
    const queue = yield* Queue.bounded<Observation, AcpSubscriptionOverflow | Cause.Done>(observerCapacity)
    // Registration and the snapshot read happen under the same permit, so
    // nothing can be applied between the two and go unobserved.
    return yield* Semaphore.withPermit(
      lock,
      Effect.gen(function*() {
        const observer: Observer = { queue, lost: false }
        observers.add(observer)
        yield* Scope.addFinalizer(
          observerScope,
          Effect.sync(() => {
            observers.delete(observer)
          }).pipe(Effect.andThen(Queue.shutdown(queue)))
        )
        return { snapshot, changes: Stream.fromQueue(queue) } satisfies Observed
      })
    )
  })

  const settle = (
    interactionId: string,
    status: "resolved" | "cancelled" | "expired",
    outcome: InteractionOutcome
  ): Effect.Effect<boolean> =>
    Effect.suspend(() => {
      const deferred = pendingInteractions.get(interactionId)
      if (deferred === undefined) return Effect.succeed(false)
      // Removed before completing, so a concurrent caller sees it gone.
      pendingInteractions.delete(interactionId)
      return Deferred.succeed(deferred, outcome).pipe(
        Effect.andThen(apply({ _tag: "interactionSettled", interactionId, status, outcome })),
        Effect.as(true)
      )
    })

  const registerInteraction = (interaction: InteractionSnapshot, respond: Deferred.Deferred<InteractionOutcome>) =>
    Effect.suspend(() => {
      pendingInteractions.set(interaction.interactionId, respond)
      return Effect.asVoid(apply({ _tag: "interactionCreated", interaction }))
    })

  const awaitIdle = Effect.suspend(() =>
    Semaphore.withPermit(
      lock,
      Effect.suspend(() => {
        if (snapshot.activeSubmissionId === null && ["unknown", "idle"].includes(snapshot.foreground.state)) return Effect.succeed(Effect.void)
        const waiter = Deferred.makeUnsafe<void>()
        idleWaiters.push(waiter)
        return Effect.succeed(Deferred.await(waiter).pipe(Effect.ensuring(Effect.sync(() => { idleWaiters = idleWaiters.filter((item) => item !== waiter) })) ))
      })
    )
  ).pipe(Effect.flatten)

  yield* Scope.addFinalizer(
    scope,
    Effect.suspend(() => {
      const waiting = [...pendingInteractions.values()]
      pendingInteractions.clear()
      const waiters = idleWaiters
      idleWaiters = []
      return Effect.all([
        ...waiting.map((deferred) => Deferred.interrupt(deferred)),
        ...waiters.map((waiter) => Deferred.succeed(waiter, undefined)),
        ...[...observers].map((observer) => Queue.end(observer.queue))
      ], { discard: true })
    })
  )

  return {
    sessionId,
    apply,
    current: Effect.sync(() => snapshot),
    observe,
    settle,
    registerInteraction,
    awaitIdle,
    noteIdle: Effect.void
  } satisfies Runtime
})

// -----------------------------------------------------------------------------
// Connection-level routing
// -----------------------------------------------------------------------------

/**
 * Where an incoming update for a session id should go.
 *
 * A session is `provisional` between installing its route and receiving its
 * lifecycle response: updates are buffered up to a configured bound and
 * replayed once the runtime exists. Exceeding the bound is an explicit
 * failure, never a silent drop.
 */
interface Provisional {
  readonly _tag: "provisional"
  readonly buffered: Array<{ sessionId: string; update: unknown }>
  readonly replayHistory: boolean
  previous?: LiveRoute
  overflowed: boolean
  bytes: number
}

type LiveRoute = { readonly _tag: "live"; readonly runtime: Runtime }
type Route = Provisional | LiveRoute

// -----------------------------------------------------------------------------
// Local client
// -----------------------------------------------------------------------------

const connect = Effect.fnUntraced(function*(options: ConnectOptions) {
  const scope = yield* Scope.Scope
  const limits: ContentLimits = { ...defaultContentLimits, ...options.limits }
  const provisionalLimits: ProvisionalLimits = { ...defaultProvisionalLimits, ...options.provisional }
  const observerCapacity = options.observerCapacity ?? 256
  for (const [name, value] of Object.entries({ ...limits, ...provisionalLimits, observerCapacity })) {
    if (!Number.isSafeInteger(value) || value <= 0) return yield* new AcpProtocolError({ message: `Invalid positive resource limit: ${name}` })
  }
  const lifecycleLock = Semaphore.makeUnsafe(1)
  const routingLock = Semaphore.makeUnsafe(1)
  const cancelTimeout = options.cancelTimeout ?? Duration.seconds(10)

  const routes = new Map<string, Route>()
  // Sessions the connection owns; released with the connection's scope.
  const sessionScopes = new Map<string, Scope.Closeable>()
  const requestElicitations = new Map<string, { readonly elicitationId?: string; readonly cancelled: Deferred.Deferred<void> }>()
  let nextLocalId = 0
  const localId = (prefix: string) => `${prefix}-${nextLocalId++}`

  const v1Handlers = options.v1Handlers ?? {}
  const installed: Capability.Installed = {
    filesystem: v1Handlers.readTextFile !== undefined || v1Handlers.writeTextFile !== undefined,
    terminal: [v1Handlers.createTerminal, v1Handlers.terminalOutput, v1Handlers.waitForTerminalExit, v1Handlers.killTerminal, v1Handlers.releaseTerminal].every((handler) => handler !== undefined)
  }

  /**
   * Provisional routes for `session/new` calls in flight.
   *
   * A new session has no id until the agent answers, but the agent may
   * already be sending updates under the id it has chosen. Those cannot be
   * matched by id, so while a `session/new` is outstanding any update for an
   * otherwise unknown session is buffered here and replayed once the id is
   * known. Concurrent `session/new` calls are disambiguated on arrival: with
   * more than one in flight the target is genuinely ambiguous, so the update
   * is dropped with a warning rather than attributed to the wrong session.
   */
  const pendingNew = new Set<Provisional>()

  const buffer = (route: Provisional, sessionId: string, update: unknown) => {
    const encodedSize = Json.byteLength(update)
    if (Result.isFailure(encodedSize)) {
      route.overflowed = true
      return
    }
    const size = encodedSize.success
    if (route.buffered.length >= provisionalLimits.updates || route.bytes + size > (provisionalLimits.bytes ?? 4 * 1024 * 1024)) {
      route.overflowed = true
      return
    }
    route.bytes += size
    route.buffered.push({ sessionId, update })
  }

  /** Routes one decoded `session/update` notification. */
  const onUpdate = (params: unknown) =>
    Semaphore.withPermit(routingLock, Effect.suspend(() => {
      if (!isRecord(params)) return Effect.void
      const sessionId = String(params["sessionId"])
      const update = params["update"]
      const route = routes.get(sessionId)
      if (route === undefined) {
        if (pendingNew.size === 1) {
          buffer([...pendingNew][0]!, sessionId, update)
          return Effect.void
        }
        return Effect.logWarning("Dropped session/update for unknown session", sessionId)
      }
      if (route._tag === "live") return Effect.asVoid(route.runtime.apply({ _tag: "update", update }))
      buffer(route, sessionId, update)
      // A replay notification has no marker distinguishing it from new live
      // traffic. Keep the prior runtime unchanged until this attempt ends.
      return route.previous === undefined || route.replayHistory
        ? Effect.void
        : Effect.asVoid(route.previous.runtime.apply({ _tag: "update", update }))
    }))

  const sessionIdOf = (params: unknown): string | undefined =>
    isRecord(params) && typeof params["sessionId"] === "string" ? params["sessionId"] : undefined

  /** Creates a pending interaction and parks until something settles it. */
  const onInteraction = (
    kind: "permission" | "elicitation",
    version: 1 | 2,
    params: unknown,
    sessionId: string | undefined
  ): Effect.Effect<InteractionOutcome, AcpRemoteError> =>
    Effect.gen(function*() {
      const request = yield* Schema.decodeUnknownEffect(InteractionRequest)(params).pipe(
        Effect.mapError(() => new AcpRemoteError({ code: ErrorCode.InvalidParams, message: "Invalid interaction request" }))
      )
      const encodedParams = yield* Json.encode(params).pipe(Effect.mapError(() => new AcpRemoteError({
        code: ErrorCode.InvalidParams, message: "Interaction is not JSON serializable"
      })))
      const { runtime, interactionId, respond } = yield* Semaphore.withPermit(routingLock, Effect.gen(function*() {
        const current = sessionId === undefined ? undefined : routes.get(sessionId)
        const route = current?._tag === "provisional" ? current.previous : current
        if (route === undefined || route._tag !== "live") {
          return yield* new AcpRemoteError({
            code: ErrorCode.InvalidParams,
            message: `No live session for ${kind} request`
          })
        }
        const runtime = route.runtime
        const interactionId = localId(kind)
        const respond = yield* Deferred.make<InteractionOutcome>()
        const snapshot = yield* runtime.current
        if (Object.values(snapshot.interactions).filter((i) => i.status === "pending").length >= limits.interactions ||
          new TextEncoder().encode(encodedParams).byteLength > (limits.transcriptBytes ?? 4 * 1024 * 1024) / (limits.interactions + 1)) {
          return yield* new AcpRemoteError({ code: ErrorCode.InvalidRequest, message: "Interaction capacity exceeded" })
        }
        const interaction: InteractionSnapshot = {
          interactionId,
          kind,
          version,
          status: "pending",
          request,
          outcome: null,
          createdAt: snapshot.seq + 1,
          resolvedAt: null
        }
        yield* runtime.registerInteraction(interaction, respond)
        return { runtime, interactionId, respond }
      }))
      // Waiting happens here, on this request's own fiber. The connection
      // reader keeps dispatching other traffic while a human decides.
      const waiting = options.interactionTimeout === undefined
        ? Deferred.await(respond)
        : Deferred.await(respond).pipe(
          Effect.timeoutOrElse({
            duration: options.interactionTimeout,
            orElse: () =>
              runtime.settle(interactionId, "expired", cancelledOutcome(kind, version)).pipe(
                Effect.andThen(Effect.succeed(cancelledOutcome(kind, version)))
              )
          })
        )
      return yield* waiting.pipe(
        // An incoming `$/cancel_request` interrupts this fiber; record that
        // the interaction was cancelled so it stops being offered as pending.
        Effect.onInterrupt(() => Effect.ignore(runtime.settle(interactionId, "cancelled", cancelledOutcome(kind, version))))
      )
    })

  /** The protocol's own "the user did not answer" outcome for each request kind. */
  const cancelledOutcome = (kind: "permission" | "elicitation", _version: 1 | 2): InteractionOutcome =>
    kind === "permission" ? { outcome: { outcome: "cancelled" } } : { action: "cancel" }

  const cancelPendingInteractions = (runtime: Runtime) => Effect.gen(function*() {
    const snapshot = yield* runtime.current
    for (const interaction of Object.values(snapshot.interactions)) {
      if (interaction.status === "pending") {
        yield* runtime.settle(interaction.interactionId, "cancelled", cancelledOutcome(interaction.kind, interaction.version))
      }
    }
  })

  const v1HandlerRoutes = (negotiated: AcpProtocol.Negotiated): ReadonlyArray<AcpConnection.Route> => {
    if (negotiated.version !== 1) return []
    const V1 = AcpProtocol.schemas[1]
    const routes: Array<AcpConnection.Route> = []
    const add = <M extends string, P, A>(
      method: import("./AcpSchema.ts").RequestMethod<M, P, A>,
      handler: ((params: P) => Effect.Effect<A>) | undefined
    ) => {
      if (handler !== undefined) routes.push(AcpConnection.onRequest(method, handler))
    }
    add(V1.clientMethods["fs/read_text_file"], v1Handlers.readTextFile)
    add(V1.clientMethods["fs/write_text_file"], v1Handlers.writeTextFile)
    add(V1.clientMethods["terminal/create"], v1Handlers.createTerminal)
    add(V1.clientMethods["terminal/output"], v1Handlers.terminalOutput)
    add(V1.clientMethods["terminal/wait_for_exit"], v1Handlers.waitForTerminalExit)
    add(V1.clientMethods["terminal/kill"], v1Handlers.killTerminal)
    add(V1.clientMethods["terminal/release"], v1Handlers.releaseTerminal)
    return routes
  }

  const onRequestElicitation = (version: 1 | 2, params: unknown): Effect.Effect<InteractionOutcome, AcpRemoteError> =>
    Effect.gen(function*() {
      const request = yield* Schema.decodeUnknownEffect(Schema.Union([V1.CreateElicitationRequest, V2.CreateElicitationRequest]))(params).pipe(
        Effect.mapError(() => new AcpRemoteError({ code: ErrorCode.InvalidParams, message: "Invalid elicitation request" })))
      if (!Schema.is(V1.ElicitationRequestScope)(request)) return yield* new AcpRemoteError({ code: ErrorCode.InvalidParams, message: "Missing elicitation request identity" })
      const size = Json.byteLength(request)
      if (Result.isFailure(size) || size.success > (limits.transcriptBytes ?? 4 * 1024 * 1024) / (limits.interactions + 1)) {
        return yield* new AcpRemoteError({ code: ErrorCode.InvalidRequest, message: "Interaction capacity exceeded" })
      }
      if (options.onElicitation === undefined) return { action: "cancel" }
      const register = Semaphore.withPermit(routingLock, Effect.gen(function*() {
        if (requestElicitations.size >= limits.interactions) return yield* new AcpRemoteError({ code: ErrorCode.InvalidRequest, message: "Interaction capacity exceeded" })
        const key = localId("request-elicitation")
        const cancelled = yield* Deferred.make<void>()
        requestElicitations.set(key, {
          ...("elicitationId" in request && typeof request.elicitationId === "string" ? { elicitationId: request.elicitationId } : {}), cancelled
        })
        return { key, cancelled }
      }))
      // Admission and cleanup form one resource boundary, including interruption during admission.
      return yield* Effect.acquireUseRelease(register, ({ cancelled }) => {
        const callback = Effect.suspend(() => options.onElicitation!(request, version)).pipe(
          Effect.map((resolution) => encodeResolution("elicitation", resolution)),
          Effect.catchCause((cause) => Cause.hasInterruptsOnly(cause) ? Effect.interrupt
            : Effect.fail(new AcpRemoteError({ code: ErrorCode.InternalError, message: "Elicitation handler failed" }))))
        const waiting = Effect.raceFirst(callback, Deferred.await(cancelled).pipe(Effect.as<InteractionOutcome>({ action: "cancel" })))
        return options.interactionTimeout === undefined ? waiting : waiting.pipe(Effect.timeoutOrElse({
          duration: options.interactionTimeout, orElse: () => Effect.succeed<InteractionOutcome>({ action: "cancel" })
        }))
      }, ({ key }) => Effect.sync(() => { requestElicitations.delete(key) }))
    })

  const handlers = (negotiated: AcpProtocol.Negotiated): AcpConnection.Handlers => {
    const version = negotiated.version
    const dispatch = AcpConnection.handlers(v1HandlerRoutes(negotiated), {
      request: (method, params) => {
        switch (method) {
          case "session/request_permission":
            return onInteraction("permission", version, params, sessionIdOf(params))
          case "elicitation/create": {
            if (!isRecord(params) || typeof params.mode !== "string" || !Capability.elicitationSupported(negotiated, params.mode)) {
              return Effect.fail(new AcpRemoteError({ code: ErrorCode.InvalidParams, message: "Elicitation mode was not advertised" }))
            }
            // A request identity remains connection-scoped, even when there is one live session.
            return "requestId" in params ? onRequestElicitation(version, params)
              : onInteraction("elicitation", version, params, sessionIdOf(params))
          }
          default:
            return undefined
        }
      },
      notification: (method, params) => {
        switch (method) {
          case "session/update":
            return onUpdate(params)
          case "elicitation/complete": {
            const elicitationId = isRecord(params) && typeof params.elicitationId === "string" ? params.elicitationId : undefined
            const cancelled = [...requestElicitations.values()].filter((entry) => entry.elicitationId === elicitationId)
            // The agent withdrew the request; settle it as cancelled.
            const sessionId = sessionIdOf(params) ?? soleLiveSession()
            const route = sessionId === undefined ? undefined : routes.get(sessionId)
            return Effect.andThen(Effect.forEach(cancelled, (entry) => Deferred.succeed(entry.cancelled, undefined), { discard: true }),
              route === undefined || route._tag !== "live" ? Effect.void : Effect.ignore(withdrawElicitations(route.runtime, elicitationId)))
          }
          default:
            return undefined
        }
      }
    })
    const methods: Readonly<Record<string, import("./AcpSchema.ts").Method>> = AcpProtocol.schemas[version].clientMethods
    return {
      request: (method, params, context) => {
        const descriptor = Object.hasOwn(methods, method) ? methods[method] : undefined
        if (descriptor?._tag !== "Request") return dispatch.request?.(method, params, context)
        return Schema.decodeUnknownEffect(descriptor.params)(params).pipe(
          Effect.mapError(() => new AcpRemoteError({ code: ErrorCode.InvalidParams, message: "Invalid client request" })),
          Effect.flatMap((decoded) => dispatch.request?.(method, decoded, context) ?? Effect.fail(new AcpRemoteError({ code: ErrorCode.MethodNotFound, message: "Method not found" }))),
          Effect.flatMap(Schema.encodeUnknownEffect(descriptor.result)),
          Effect.mapError((error) => Schema.is(AcpRemoteError)(error) ? error : new AcpRemoteError({ code: ErrorCode.InternalError, message: "Invalid client response" })))
      },
      notification: (method, params) => {
        const descriptor = Object.hasOwn(methods, method) ? methods[method] : undefined
        return descriptor?._tag === "Notification" ? Schema.decodeUnknownEffect(descriptor.params)(params).pipe(
          Effect.flatMap((decoded) => dispatch.notification?.(method, decoded) ?? Effect.void), Effect.ignore) : dispatch.notification?.(method, params)
      }
    }
  }

  /** The only live session, when there is exactly one; else undefined. */
  const soleLiveSession = (): string | undefined => {
    const live = [...routes.entries()].filter(([, route]) => route._tag === "live")
    return live.length === 1 ? live[0]![0] : undefined
  }

  const withdrawElicitations = (runtime: Runtime, elicitationId: string | undefined) =>
    Effect.gen(function*() {
      const snapshot = yield* runtime.current
      const pending = Object.values(snapshot.interactions).filter(
        (interaction) => interaction.kind === "elicitation" && interaction.status === "pending" && "elicitationId" in interaction.request && interaction.request.elicitationId === elicitationId
      )
      yield* Effect.forEach(
        pending,
        (interaction) => runtime.settle(interaction.interactionId, "cancelled", { action: "cancel" }),
        { discard: true }
      )
    })

  const firstVersion = (options.versions ?? [1])[0]
  // Decode the selected version once, then retain its capability types while
  // deriving advertisements from the handlers the application actually installed.
  const invalidInitialize = (cause: Schema.SchemaError) => new AcpProtocolError({ message: "Invalid initialize params", cause })
  const connectOptions: AcpProtocol.ConnectOptions = yield* Effect.gen(function*() {
    if (firstVersion === 1) {
      const params = yield* Schema.decodeEffect(V1.InitializeRequest)({ ...options.params, protocolVersion: 1 })
      const auth = { ...params.clientCapabilities?.auth }
      if (options.terminalAuth) auth.terminal = true
      else delete auth.terminal
      return {
        ...options, versions: [1] as const, handlers,
        params: { ...params, clientCapabilities: {
          ...params.clientCapabilities, auth,
          fs: { readTextFile: !!v1Handlers.readTextFile, writeTextFile: !!v1Handlers.writeTextFile },
          terminal: installed.terminal
        } }
      }
    }
    const params = yield* Schema.decodeUnknownEffect(V2.InitializeRequest)({ ...options.params, protocolVersion: 2 })
    const auth = { ...params.capabilities?.auth }
    if (options.terminalAuth) auth.terminal = {}
    else delete auth.terminal
    return {
      ...options, versions: options.versions?.some((version) => version === 1) ? [2, 1] as const : [2] as const,
      params: { ...params, capabilities: { ...params.capabilities, auth } }, handlers
    }
  }).pipe(Effect.mapError(invalidInitialize))
  const protocolScope = yield* Scope.fork(scope)
  const { connection, negotiated } = yield* Scope.provide(AcpProtocol.connect(connectOptions), protocolScope).pipe(
    Effect.onExit((exit) => Exit.isFailure(exit) ? Scope.close(protocolScope, exit) : Effect.void))
  const capabilities = Capability.normalize(negotiated, negotiated.advertised.version === 1 ? installed : { filesystem: false, terminal: false })
  const version = negotiated.version

  const request = connection.request

  const unsupported = (operation: string, detail?: string) =>
    Effect.fail(new AcpCapabilityUnsupported({ operation, version, detail }))

  // ---------------------------------------------------------------------------
  // Session handle
  // ---------------------------------------------------------------------------

  const makeSession = (runtime: Runtime, sessionScope: Scope.Closeable): AcpSession => {
    // One admitted foreground submission at a time. `takeIfAvailable` makes
    // the check-and-claim atomic, so two concurrent submits cannot both win.
    const foreground = Semaphore.makeUnsafe(1)
    const sessionId = runtime.sessionId
    const ensureLive = Effect.suspend(() => {
      const route = routes.get(sessionId)
      const current = route?._tag === "provisional" ? route.previous : route
      return current?.runtime === runtime && sessionScopes.get(sessionId) === sessionScope && sessionScope.state._tag !== "Closed"
        ? Effect.void
        : Effect.fail(new AcpConnectionClosed({ message: `Session ${sessionId} released` }))
    })

    const submit = (prompt: Parameters<AcpSession["submit"]>[0]) =>
      Effect.andThen(ensureLive, Effect.gen(function*() {
        if (!capabilities.session.prompt) return yield* unsupported("session/prompt")
        const unsupportedBlock = prompt.find((block) => !Capability.contentSupported(capabilities, block))
        if (unsupportedBlock !== undefined) {
          return yield* unsupported("session/prompt", "Unsupported prompt content type")
        }
        const state = yield* runtime.current
        if (state.activeSubmissionId !== null || !["unknown", "idle"].includes(state.foreground.state)) {
          return yield* new AcpSessionBusy({ message: `Session ${sessionId} has foreground work` })
        }
        const admitted = yield* Semaphore.takeIfAvailable(foreground, 1)
        if (!admitted) {
          return yield* new AcpSessionBusy({
            message: `Session ${sessionId} already has foreground work in flight`,
            submissionId: (yield* runtime.current).activeSubmissionId ?? undefined
          })
        }
        let permitHeld = true
        const releasePermit = Effect.uninterruptible(Effect.suspend(() => {
          if (!permitHeld) return Effect.void
          permitHeld = false
          return Semaphore.release(foreground, 1)
        }))
        return yield* dispatch(prompt, releasePermit).pipe(
          // The permit is held until the submission settles, not until the
          // call returns: the session stays busy for the whole turn.
          Effect.tapCause(() => releasePermit)
        )
      }).pipe(Effect.forkIn(sessionScope), Effect.flatMap(Fiber.join)))

    const dispatch = (prompt: Parameters<AcpSession["submit"]>[0], releasePermit: Effect.Effect<void>) =>
      Effect.gen(function*() {
        const id = localId("submission")
        const record: SubmissionSnapshot = {
          id,
          prompt,
          status: { _tag: "pending" },
          requestId: null,
          agentMessageId: null,
          // v1 has no insertion acknowledgement at all.
          acceptanceUnavailable: version === 1,
          foreground: "inferred"
        }
        let latest = record
        // Registered before anything is written, so an update that arrives
        // before the response already has a record to reconcile against.
        // Register only after schema validation, still before the wire write.

        const accepted = yield* Deferred.make<string, OperationError>()
        if (version === 1) yield* Deferred.fail(accepted, new AcpCapabilityUnsupported({ operation: "session/prompt acceptance", version }))
        const outcome = yield* Deferred.make<SubmissionSnapshot, OperationError>()

        const promptMethod = AcpProtocol.schemas[version].agentMethods["session/prompt"]
        const encoded = yield* Schema.encodeUnknownEffect(promptMethod.params)({ sessionId, prompt }).pipe(
          Effect.mapError(() => new AcpProtocolError({ message: "Invalid session/prompt params" })))
        yield* runtime.apply({ _tag: "submissionRegistered", submission: record })
        const sent = yield* connection.send("session/prompt", encoded).pipe(
          Effect.tapError((error) => runtime.apply({ _tag: "submissionFailed", id, failure: toFailure(error) })))
        yield* runtime.apply({ _tag: "submissionDispatched", id, requestId: sent.id })

        const settle = Effect.gen(function*() {
          const value = yield* sent.response.pipe(Effect.flatMap(Schema.decodeUnknownEffect(promptMethod.result)),
            Effect.mapError((error) => error instanceof Schema.SchemaError ? new AcpProtocolError({ message: "Invalid prompt response" }) : error))
          if (version === 2) {
            // v2: the response acknowledges insertion only. Foreground work
            // ends later, on the idle state update.
            if (!("messageId" in value)) return yield* new AcpProtocolError({ message: "Missing v2 prompt messageId" })
            const agentMessageId = value.messageId
            yield* runtime.apply({ _tag: "submissionAccepted", id, agentMessageId })
            yield* Deferred.succeed(accepted, agentMessageId)
            // Foreground work ends on the idle state update, not here.
            yield* Effect.raceFirst(runtime.awaitIdle, Effect.flatMap(connection.closed, Effect.fail))
          } else {
            // v1: updates sent before the response must be projected before completion.
            yield* connection.drainNotifications
            // v1: the response *is* turn completion; acceptance never comes.
            yield* Deferred.fail(
              accepted,
              new AcpCapabilityUnsupported({
                operation: "session/prompt acceptance",
                version,
                detail: "v1 has no prompt insertion acknowledgement"
              })
            )
          }
          const stopReason = "stopReason" in value ? value.stopReason : null
          const snapshot = yield* runtime.apply({ _tag: "submissionCompleted", id, stopReason })
          latest = snapshot.submissions[id] ?? { ...latest, prompt: [], status: { _tag: "completed" } }
          yield* Deferred.succeed(outcome, latest)
          return snapshot
        }).pipe(
          Effect.onExit((exit) => {
            if (Exit.isSuccess(exit)) return Effect.void
            const cause = exit.cause
            const error = Cause.hasInterruptsOnly(cause) ? new AcpConnectionClosed({ message: "Session owner closed" }) : Option.getOrElse(Cause.findErrorOption(cause), () => new AcpProtocolError({ message: "Prompt failed", cause: Cause.squash(cause) }))
            // Waiters can outlive this scope. Complete them before reducer
            // bookkeeping, which may contend with an update during shutdown.
            return Effect.uninterruptible(Effect.gen(function*() {
              yield* Deferred.fail(accepted, error)
              yield* Deferred.fail(outcome, error)
              const snapshot = yield* runtime.apply({ _tag: "submissionFailed", id, failure: toFailure(error) })
              latest = snapshot.submissions[id] ?? { ...latest, prompt: [], status: { _tag: "failed", failure: toFailure(error) } }
            }))
          }),
          Effect.ensuring(releasePermit))

        yield* Effect.forkIn(settle, sessionScope, { startImmediately: true })

        return {
          id,
          snapshot: Effect.map(runtime.current, (state) => { latest = state.submissions[id] ?? latest; return latest }),
          accepted: Deferred.await(accepted),
          outcome: Deferred.await(outcome)
        } satisfies Submission
      })

    return {
      sessionId,
      version,
      release: Semaphore.withPermit(routingLock, Effect.suspend(() => {
        const route = routes.get(sessionId)
        const ownedRoute = route?._tag === "provisional" ? route.previous : route
        if (ownedRoute?.runtime !== runtime) return Effect.void
        if (route?._tag === "provisional") delete route.previous
        else routes.delete(sessionId)
        const owned = sessionScopes.get(sessionId)
        sessionScopes.delete(sessionId)
        return owned === undefined ? Effect.void : Effect.andThen(
          cancelPendingInteractions(runtime),
          Scope.close(owned, Exit.void)
        )
      })),
      snapshot: runtime.current,
      observe: runtime.observe,
      changes: Stream.unwrap(Effect.map(runtime.observe, (observed) => observed.changes)),
      submit,
      cancel: Effect.gen(function*() {
        yield* ensureLive
        const state = yield* runtime.current
        yield* Effect.forEach(Object.values(state.interactions).filter((i) => i.status === "pending"),
          (i) => runtime.settle(i.interactionId, "cancelled", cancelledOutcome(i.kind, version)), { discard: true })
        yield* connection.notifyRaw("session/cancel", { sessionId })
        yield* runtime.apply({ _tag: "cancelRequested" })
        // Updates keep being applied while this waits; cancellation is only
        // confirmed by the negotiated completion signal.
        return yield* Effect.raceFirst(runtime.awaitIdle, Effect.flatMap(connection.closed, Effect.fail)).pipe(
          Effect.timeoutOrElse({
            duration: cancelTimeout,
            orElse: () =>
              new AcpCancellationUnconfirmed({
                message: `Session ${sessionId} did not confirm cancellation within the configured window`
              })
          })
        )
      }),
      resolveInteraction: (interactionId, resolution) =>
        Effect.gen(function*() {
          yield* ensureLive
          const snapshot = yield* runtime.current
          const interaction = Object.hasOwn(snapshot.interactions, interactionId) ? snapshot.interactions[interactionId] : undefined
          if (interaction === undefined || interaction.status !== "pending") {
            return yield* interaction?.status === "expired"
              ? new AcpInteractionExpired({ interactionId })
              : new AcpInteractionAlreadyResolved({ interactionId })
          }
          if (interaction.kind === "permission" && resolution._tag !== "cancelled" &&
            (resolution._tag !== "selected" || ! ("options" in interaction.request) || !interaction.request.options.some((option) => option.optionId === resolution.optionId))) {
            return yield* new AcpProtocolError({ message: "Invalid permission resolution" })
          }
          if (interaction.kind === "elicitation" && !["accept", "decline", "cancel"].includes(resolution._tag)) return yield* new AcpProtocolError({ message: "Invalid elicitation resolution" })
          const settled = yield* runtime.settle(
            interactionId,
            "resolved",
            encodeResolution(interaction.kind, resolution)
          )
          // Lost the race with another caller: exactly one resolution is sent.
          if (!settled) return yield* new AcpInteractionAlreadyResolved({ interactionId })
        }),
      setConfigOption: (configId, value) => Effect.gen(function*() {
        yield* ensureLive
        if (!capabilities.session.setConfigOption) return yield* unsupported("session/set_config_option")
        yield* request(version === 2 ? V2.agentMethods["session/set_config_option"] : V1.agentMethods["session/set_config_option"], typeof value === "boolean" ? { sessionId, configId, type: "boolean", value } : { sessionId, configId, ...(version === 2 ? { type: "id" } : {}), value }).pipe(
          Effect.flatMap((result) => runtime.apply({ _tag: "lifecycle", configOptions: result.configOptions })))
      }),
      setMode: (modeId) => Effect.gen(function*() {
        yield* ensureLive
        if (version !== 1 || !(yield* runtime.current).config["acp/modes"]) return yield* unsupported("session/set_mode")
        yield* request(V1.agentMethods["session/set_mode"], { sessionId, modeId })
        yield* runtime.apply({ _tag: "update", update: { sessionUpdate: "current_mode_update", currentModeId: modeId } })
      }),
      close: Effect.gen(function*() {
        yield* ensureLive
        if (!capabilities.session.close) return yield* unsupported("session/close")
        yield* request(version === 2 ? V2.agentMethods["session/close"] : V1.agentMethods["session/close"], { sessionId })
      }),
      delete: Effect.gen(function*() {
        yield* ensureLive
        if (!capabilities.session.delete) return yield* unsupported("session/delete")
        yield* request(version === 2 ? V2.agentMethods["session/delete"] : V1.agentMethods["session/delete"], { sessionId })
      })
    }
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  /**
   * Installs a route, runs the lifecycle request, then promotes the route to
   * a live runtime and replays whatever arrived in between.
   */
  const establish = Effect.fnUntraced(function*<E>(
    knownSessionId: string | undefined,
    cwd: string,
    replayHistory: boolean,
    request: (routeKey: string) => Effect.Effect<{ readonly sessionId: string; readonly result: V1.NewSessionResponse | V1.LoadSessionResponse | V1.ResumeSessionResponse | V2.NewSessionResponse | V2.ResumeSessionResponse }, E>
  ) {
    // For `session/new` the id is not known until the response, so routing is
    // keyed on a placeholder until the live route can be committed.
    const routeKey = knownSessionId ?? localId("pending-session")
    const provisional: Provisional = { _tag: "provisional", buffered: [], replayHistory, overflowed: false, bytes: 0 }
    const previousScope = yield* Semaphore.withPermit(routingLock, Effect.gen(function*() {
      const previous = knownSessionId === undefined ? undefined : routes.get(knownSessionId)
      const owned = knownSessionId === undefined ? undefined : sessionScopes.get(knownSessionId)
      const previousSnapshot = previous?._tag === "live" ? yield* previous.runtime.current : undefined
      if (previous?._tag === "live" && owned !== undefined && owned.state._tag !== "Closed" &&
        previousSnapshot?.activeSubmissionId !== null) {
        return yield* new AcpSessionBusy({ message: "Cannot resume a session with active work" })
      }
      if (previous?._tag === "live" && owned !== undefined && owned.state._tag !== "Closed") {
        provisional.previous = previous
      }
      routes.set(routeKey, provisional)
      // Unknown ids are buffered until session/new supplies the real id.
      if (knownSessionId === undefined) pendingNew.add(provisional)
      return owned
    }))

    const cleanup = Semaphore.withPermit(routingLock, Effect.sync(() => {
      pendingNew.delete(provisional)
      // A released or closed runtime cannot be restored. An intervening route
      // owner must also be left alone.
      if (routes.get(routeKey) !== provisional) return
      if (provisional.previous !== undefined && previousScope !== undefined &&
        previousScope.state._tag !== "Closed" && sessionScopes.get(routeKey) === previousScope) {
        routes.set(routeKey, provisional.previous)
      } else {
        routes.delete(routeKey)
      }
    }))

    let promoted = false
    let candidateScope: Scope.Closeable | undefined
    return yield* Effect.gen(function*() {
      // Drain notifications already queued before promoting the candidate or
      // restoring the prior route. Explicit replay remains isolated here.
      const outcome = yield* Effect.exit(request(routeKey).pipe(Effect.ensuring(Effect.ignore(connection.drainNotifications))))
      if (Exit.isFailure(outcome)) return yield* Effect.failCause(outcome.cause)
      const { result, sessionId } = outcome.value

      return yield* Semaphore.withPermit(routingLock, Effect.gen(function*() {
        pendingNew.delete(provisional)
        if (provisional.overflowed) {
          return yield* new AcpProvisionalOverflow({ sessionId, limit: provisionalLimits.updates })
        }
        const sessionScope = yield* Scope.fork(scope)
        candidateScope = sessionScope
        const runtime = yield* Scope.provide(
          makeRuntime(sessionId, version, cwd, limits, observerCapacity),
          sessionScope
        )
        // Replay into the new runtime while the old live runtime remains
        // available. Explicit history replay never mutates that old runtime.
        for (const entry of provisional.buffered) {
          if (entry.sessionId === sessionId) yield* runtime.apply({ _tag: "update", update: entry.update })
        }
        yield* runtime.apply({
          _tag: "lifecycle",
          cwd,
          ...(result.configOptions === undefined ? {} : { configOptions: result.configOptions }),
          ...("modes" in result && result.modes !== undefined ? { modes: result.modes } : {})
        })
        if (sessionScope.state._tag === "Closed") {
          return yield* new AcpConnectionClosed({ message: "Connection closed while establishing session" })
        }
        const owned = sessionScopes.get(sessionId)
        const displaced = routes.get(sessionId)
        const prior = displaced?._tag === "provisional" ? displaced.previous : displaced
        if (owned !== undefined && owned.state._tag !== "Closed" && prior !== undefined) {
          yield* cancelPendingInteractions(prior.runtime)
        }
        sessionScopes.set(sessionId, sessionScope)
        if (routes.get(routeKey) === provisional) routes.delete(routeKey)
        routes.set(sessionId, { _tag: "live", runtime })
        promoted = true
        if (owned !== undefined) yield* Scope.close(owned, Exit.void)
        return makeSession(runtime, sessionScope)
      }))
    }).pipe(Effect.onExit(() => promoted ? Effect.void : Effect.andThen(
      cleanup,
      candidateScope === undefined ? Effect.void : Scope.close(candidateScope, Exit.void)
    )))
  })

  const newSession = (sessionOptions: NewSessionOptions) =>
    Semaphore.withPermit(lifecycleLock, Effect.gen(function*() {
      if (!capabilities.session.prompt) return yield* unsupported("session/new")
      const rejected = sessionOptions.mcpServers?.find((server) => !Capability.mcpServerSupported(capabilities, server))
      if (rejected !== undefined) {
        return yield* unsupported("session/new", "Unsupported MCP server configuration")
      }
      if (sessionOptions.additionalDirectories !== undefined && !capabilities.session.additionalDirectories) {
        return yield* unsupported("session/new", "additionalDirectories is not supported")
      }
      const input = { cwd: sessionOptions.cwd,
        ...(sessionOptions.additionalDirectories === undefined ? {} : { additionalDirectories: sessionOptions.additionalDirectories }),
        mcpServers: sessionOptions.mcpServers ?? [] }
      return yield* establish(undefined, sessionOptions.cwd, false, () => Effect.gen(function*() {
        const invalid = (cause: Schema.SchemaError) => new AcpProtocolError({ message: "Invalid session/new params", cause })
        const result = version === 2
          ? yield* request(V2.agentMethods["session/new"], yield* Schema.decodeUnknownEffect(V2.NewSessionRequest)(input).pipe(Effect.mapError(invalid)))
          : yield* request(V1.agentMethods["session/new"], yield* Schema.decodeUnknownEffect(V1.NewSessionRequest)(input).pipe(Effect.mapError(invalid)))
        return { sessionId: result.sessionId, result }
      }))
    }))

  const resumeSession = (sessionOptions: ResumeSessionOptions) =>
    Semaphore.withPermit(lifecycleLock, Effect.gen(function*() {
      if (version === 2 && !capabilities.session.resume) return yield* unsupported("session/resume")
      if (sessionOptions.mcpServers?.some((server) => !Capability.mcpServerSupported(capabilities, server))) return yield* unsupported("session/resume", "Unsupported MCP server transport")
      if (sessionOptions.additionalDirectories && !capabilities.session.additionalDirectories) return yield* unsupported("session/resume", "additionalDirectories is not supported")
      // v1 resume deliberately omits history. An explicit replay request must
      // use load even when the agent also advertises resume.
      if (version === 1 && sessionOptions.replayFrom !== undefined && sessionOptions.replayFrom.type !== "start") {
        return yield* new AcpHistoryUnavailable({
          sessionId: sessionOptions.sessionId,
          operation: "load",
          detail: "v1 supports full history replay only"
        })
      }
      const operation = version === 2 || (capabilities.session.resume && sessionOptions.replayFrom === undefined)
        ? "session/resume" : "session/load"
      if (version === 1 && operation === "session/load" && !capabilities.session.loadSession) {
        return yield* new AcpHistoryUnavailable({
          sessionId: sessionOptions.sessionId,
          operation: "load",
          detail: "v1 agent did not advertise loadSession"
        })
      }
      const input = { sessionId: sessionOptions.sessionId, cwd: sessionOptions.cwd,
        ...(sessionOptions.additionalDirectories === undefined ? {} : { additionalDirectories: sessionOptions.additionalDirectories }),
        mcpServers: sessionOptions.mcpServers ?? [],
        ...(version === 2 && sessionOptions.replayFrom !== undefined ? { replayFrom: sessionOptions.replayFrom } : {}) }
      return yield* establish(sessionOptions.sessionId, sessionOptions.cwd,
        version === 1 ? operation === "session/load" : sessionOptions.replayFrom !== undefined && sessionOptions.replayFrom !== null,
        () => Effect.gen(function*() {
          const invalid = (cause: Schema.SchemaError) => new AcpProtocolError({ message: "Invalid session resume params", cause })
          const result = version === 2
            ? yield* request(V2.agentMethods["session/resume"], yield* Schema.decodeUnknownEffect(V2.ResumeSessionRequest)(input).pipe(Effect.mapError(invalid)))
            : yield* request(V1.agentMethods[operation], yield* Schema.decodeUnknownEffect(V1.agentMethods[operation].params)(input).pipe(Effect.mapError(invalid)))
          return { sessionId: sessionOptions.sessionId, result }
        }))
    }))

  const listSessions = (cwd?: string) =>
    capabilities.session.list
      ? request(version === 2 ? V2.agentMethods["session/list"] : V1.agentMethods["session/list"], cwd === undefined ? {} : { cwd }).pipe(
        Effect.map((result): ReadonlyArray<SessionListEntry> => result.sessions.map((session) => ({
          sessionId: session.sessionId, cwd: session.cwd ?? null,
          title: session.title ?? null, updatedAt: session.updatedAt ?? null
        })))
      )
      : unsupported("session/list")

  const authenticate = (methodId: string) =>
    Effect.gen(function*() {
      if (protocolScope.state._tag === "Closed") return yield* new AcpConnectionClosed({ message: "Connection released after terminal authentication" })
      if (!capabilities.auth.methods.includes(methodId)) {
        return yield* unsupported("authenticate", `Unknown auth method ${methodId}`)
      }
      const method = advertisedMethod(capabilities, methodId)
      // Advertising terminal authentication without being able to run the
      // agent's command would strand the user, so require the callback.
      if (method && "type" in method && method.type === "terminal" && Schema.is(Schema.Union([V1.AuthMethodTerminal, V2.AuthMethodTerminal]))(method)) {
        if (options.terminalAuth === undefined) return yield* unsupported("authenticate", "Terminal authentication requires a terminalAuth callback")
        return yield* Semaphore.withPermit(lifecycleLock, Effect.gen(function*() {
          if (protocolScope.state._tag === "Closed") return yield* new AcpConnectionClosed({ message: "Connection released after terminal authentication" })
          yield* options.terminalAuth!(method)
          // Terminal login changes credentials outside ACP. This transport cannot be reinitialized;
          // invalidate it and let the owner establish a fresh initialized connection.
          yield* Effect.uninterruptible(Effect.gen(function*() {
            yield* Scope.close(protocolScope, Exit.void)
            const owned = yield* Semaphore.withPermit(routingLock, Effect.sync(() => {
              const owned = [...sessionScopes.values()]
              routes.clear()
              sessionScopes.clear()
              pendingNew.clear()
              return owned
            }))
            yield* Effect.forEach(owned, (sessionScope) => Scope.close(sessionScope, Exit.void), { discard: true })
          }))
        }))
      }
      return yield* Effect.asVoid(
        request(version === 2 ? V2.agentMethods["auth/login"] : V1.agentMethods.authenticate, { methodId })
      )
    })

  const logout = capabilities.auth.logout
    ? Effect.asVoid(request(version === 2 ? V2.agentMethods["auth/logout"] : V1.agentMethods.logout, {}))
    : unsupported("logout")

  return {
    capabilities,
    negotiated,
    closed: connection.closed,
    request: connection.requestRaw,
    newSession,
    resumeSession,
    listSessions,
    authenticate,
    logout
  } satisfies AcpAgentConnection
})

const advertisedMethod = (capabilities: Capabilities, methodId: string) =>
  capabilities.negotiated.authMethods?.find((method) => Capability.authMethodId(method) === methodId)

/** Encodes a caller's resolution into the interaction's protocol response. */
const encodeResolution = (
  kind: "permission" | "elicitation",
  resolution: InteractionResolution
): InteractionOutcome => {
  if (kind === "permission") {
    return resolution._tag === "selected"
      ? { outcome: { outcome: "selected", optionId: resolution.optionId } }
      : { outcome: { outcome: "cancelled" } }
  }
  switch (resolution._tag) {
    case "accept":
      return { action: "accept", ...(resolution.content === undefined ? {} : { content: resolution.content }) }
    case "decline":
      return { action: "decline" }
    default:
      return { action: "cancel" }
  }
}

/**
 * `AcpClient` implemented over `AcpConnector`.
 *
 * @category layers
 */
export const layer: Layer.Layer<AcpClient, never, AcpConnector> = Layer.effect(
  AcpClient,
  Effect.map(AcpConnector, (connector) =>
    AcpClient.of({
      // The connector is captured here so `connect` does not leak it into the
      // caller's requirements: an application only needs `AcpClient`.
      connect: (options) => Effect.provideService(connect(options), AcpConnector, connector)
    }))
)
