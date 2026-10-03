import { randomUUID } from "./internal/crypto.ts"
/**
 * AcpClient implementation over the package-owned gateway (never raw ACP).
 */
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as RcMap from "effect/RcMap"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import { SessionSnapshot, type SubmissionSnapshot } from "./AcpApp.ts"
import { AcpClient, type AcpAgentConnection, type AcpSession, type NewSessionOptions, type Observation, type OperationError } from "./AcpClient.ts"
import { AcpConnectionClosed } from "./AcpError.ts"
import * as AcpGateway from "./AcpGateway.ts"
import type { Client } from "./AcpGatewayClient.ts"
import { AcpCapabilityUnsupported, AcpSessionBusy, AcpSubscriptionOverflow } from "./AcpSessionError.ts"
import { authMethodId } from "./internal/capabilities.ts"

/**
 * Host launch profile, profile arguments, observer capacity, and retained connection namespace.
 *
 * **Details**
 *
 * Observer capacity defaults to 256. `connectionKey` distinguishes independent connections to the
 * same profile.
 *
 * @category configuration
 */
export interface Options {
  /**
   * Authorized host launch profile to open through gateway admission.
   */
  readonly profile: string
  /**
   * Application-defined profile arguments sent to the host resolver.
   */
  readonly profileOptions?: unknown
  /**
   * Frames buffered for each observer before it must resynchronize. Defaults to 256.
   */
  readonly observerCapacity?: number
  /**
   * Distinguishes multiple connections to the same launch profile.
   */
  readonly connectionKey?: string
}
/**
 * Hosted session metadata used to attach to a retained agent session.
 *
 * @category models
 */
export type SessionDescriptor = AcpGateway.SessionDescriptor
const RetainedSession = Schema.UndefinedOr(Schema.Struct({ cursor: AcpGateway.Cursor, snapshot: SessionSnapshot }))
const RetainedConnection = Schema.UndefinedOr(Schema.Struct({ epoch: Schema.String, descriptor: Schema.optionalKey(AcpGateway.ConnectionDescriptor), operationId: Schema.String }))
const network = <A, E, R>(effect: Effect.Effect<A, E, R>) => effect.pipe(Effect.catchIf(
  (error): error is Extract<E, { _tag: "RpcClientError" }> => typeof error === "object" && error !== null && "_tag" in error && error._tag === "RpcClientError",
  () => Effect.fail(AcpGateway.failure("Closed"))))

/**
 * Creates a remote application client over an established gateway client.
 *
 * **Details**
 *
 * Returns connection operations, scoped attachment acquisition, and descriptor lookup for returned
 * session handles. Attachments restore retained snapshots and cursors without reinitializing ACP or
 * resubmitting prompts. Successful terminal authentication releases the host connection and its
 * matching retained descriptor; call `connect` again to open and initialize a fresh connection.
 *
 * **Gotchas**
 *
 * Observer capacity must be a positive safe integer. A changed host epoch rejects retained
 * connections with `HostRestarted` . Attachment takeover can revoke the previous controller.
 *
 * @category constructors
 */
export const make = (gateway: Client, options: Options) => Effect.gen(function*() {
  const capacity = options.observerCapacity ?? 256
  if (!Number.isSafeInteger(capacity) || capacity <= 0) return yield* AcpGateway.failure("Invalid")
  const descriptors = new WeakMap<AcpSession, SessionDescriptor>()
  const controllers = new Map<string, number>()
  const run = (command: AcpGateway.Command, generation?: number): Effect.Effect<unknown, AcpGateway.CommandError> =>
    network(gateway.command(command, generation)).pipe(Effect.flatMap((op) => op.error ? Effect.fail(op.error) : Effect.succeed(op.result)))

  /**
   * Reattaches retained state without ACP initialize/resume or resubmitting a prompt.
   */
  const acquire = (descriptor: SessionDescriptor, takeover: boolean, cacheKey: string, token: symbol): Effect.Effect<AcpSession, AcpGateway.GatewayError, Scope.Scope> => Effect.gen(function*() {
    const owned = yield* Scope.fork(yield* Scope.Scope)
    yield* Scope.addFinalizer(owned, Effect.sync(() => {
      if (entries.get(cacheKey) === token) {
        entries.delete(cacheKey)
        if (requests.get(cacheKey)?.borrowers === 0) requests.delete(cacheKey)
      }
    }))
    const key = `${gateway.prefix}:session:${descriptor.session}`
    const saved = yield* gateway.storage.load(key).pipe(Effect.flatMap(Schema.decodeUnknownEffect(RetainedSession)), Effect.mapError(() => AcpGateway.failure("Invalid")))
    const retained = saved?.cursor.epoch === gateway.window.epoch ? saved : undefined
    // A retained frame came from this host epoch. Reject mismatched metadata
    // before Attach, since takeover would revoke the current controller.
    if ((retained && retained.cursor.session !== descriptor.session) ||
      (retained && (retained.snapshot.sessionId !== descriptor.sessionId || retained.snapshot.version !== descriptor.version))) {
      return yield* AcpGateway.failure("Invalid")
    }
    let snapshot = retained?.snapshot
    let sequence = retained?.cursor.sequence ?? -1
    let boundary = -1
    let generation = 0
    let failed: AcpGateway.GatewayError | undefined
    const observers = new Set<Queue.Queue<Observation, AcpSubscriptionOverflow | AcpGateway.GatewayError | Cause.Done>>()
    const ready = yield* Deferred.make<void, AcpGateway.GatewayError>()
    const failure = (error: AcpGateway.GatewayError) => Effect.gen(function*() {
      failed = error
      if (controllers.get(descriptor.session) === generation) {
        controllers.delete(descriptor.session)
      }
      if (entries.get(cacheKey) === token) {
        yield* RcMap.invalidate(attachments, cacheKey)
      }
      yield* Deferred.fail(ready, error)
      for (const queue of observers) yield* Queue.fail(queue, error)
    })
    const publish = (next: SessionSnapshot) => Effect.sync(() => {
      snapshot = next
      for (const queue of observers) if (!Queue.offerUnsafe(queue, { _tag: "snapshot", snapshot: next })) {
        Queue.failCauseUnsafe(queue, Cause.fail(new AcpSubscriptionOverflow({ message: "Remote observer requires a fresh boundary" })))
        observers.delete(queue)
      }
    })
    const receive = (frame: AcpGateway.Frame) => Effect.gen(function*() {
      if (frame._tag === "Attached") {
        generation = frame.generation
        controllers.set(descriptor.session, generation)
        boundary = frame.cursor.sequence
        if (frame.snapshot) { sequence = frame.cursor.sequence; yield* publish(frame.snapshot) }
      } else if (frame.cursor.sequence > sequence) {
        if (frame.cursor.sequence !== sequence + 1) return yield* AcpGateway.failure("ResyncRequired")
        sequence = frame.cursor.sequence
        yield* publish(frame.snapshot)
      }
      if (snapshot) yield* gateway.storage.save(key, { cursor: { ...frame.cursor, sequence }, snapshot })
      if (snapshot && sequence >= boundary) yield* Deferred.succeed(ready, undefined)
    })
    const incoming = gateway.api.Attach({ epoch: gateway.window.epoch, workspace: gateway.window.workspace,
      clientId: gateway.clientId, session: descriptor.session, takeover,
      expected: { sessionId: descriptor.sessionId, version: descriptor.version },
      ...(retained ? { cursor: retained.cursor } : {}) })
    yield* network(Stream.runForEach(incoming, receive)).pipe(
      Effect.andThen(failure(AcpGateway.failure("Closed"))), Effect.catch((error) => failure(error)), Effect.forkIn(owned))
    yield* Deferred.await(ready)
    if (snapshot!.sessionId !== descriptor.sessionId || snapshot!.version !== descriptor.version) return yield* AcpGateway.failure("Invalid")
    const observe: AcpSession["observe"] = Effect.gen(function*() {
      const queue = yield* Queue.bounded<Observation, AcpSubscriptionOverflow | AcpGateway.GatewayError | Cause.Done>(capacity)
      observers.add(queue)
      yield* Effect.addFinalizer(() => Effect.sync(() => { observers.delete(queue) }).pipe(Effect.andThen(Queue.shutdown(queue))))
      if (failed) yield* Queue.fail(queue, failed)
      return { snapshot: snapshot!, changes: Stream.fromQueue(queue) }
    })
    const command = (command: AcpGateway.Command) => Effect.suspend(() => failed ? Effect.fail(failed) : run(command, generation))
    const waitFor = (id: string, terminal: boolean): Effect.Effect<SubmissionSnapshot, OperationError> => Effect.scoped(Effect.gen(function*() {
      const observed = yield* observe
      const matches = (state: SessionSnapshot) => {
        const sub = Object.hasOwn(state.submissions, id) ? state.submissions[id] : undefined
        return sub && (!terminal || ["completed", "failed"].includes(sub.status._tag)) ? sub : undefined
      }
      const current = matches(observed.snapshot)
      if (current) return current
      const result = yield* Stream.runHead(observed.changes.pipe(Stream.map((event) => matches(event.snapshot)), Stream.filter((s) => s !== undefined))).pipe(
        Effect.mapError(() => AcpGateway.failure("ResyncRequired")))
      if (result._tag === "None") return yield* AcpGateway.failure("Closed")
      return result.value!
    }))
    const session: AcpSession = {
      sessionId: descriptor.sessionId, version: descriptor.version,
      release: Scope.close(owned, Exit.void),
      snapshot: Effect.sync(() => snapshot!), observe,
      changes: Stream.unwrap(Effect.map(observe, (value) => value.changes)),
      submit: (prompt) => Effect.gen(function*() {
        if (snapshot!.activeSubmissionId !== null || !["unknown", "idle"].includes(snapshot!.foreground.state)) return yield* new AcpSessionBusy({ message: "Session has foreground work" })
        let operation = yield* network(gateway.submit({ _tag: "Submit", session: descriptor.session, prompt }, generation))
        while (operation.result === null && operation.status === "admitted") {
          yield* Effect.sleep("1 millis")
          operation = yield* network(gateway.api.Operation({ window: gateway.window, operationId: operation.operationId }))
        }
        if (operation.error) return yield* operation.error
        const result = yield* Schema.decodeUnknownEffect(AcpGateway.SubmissionResult)(operation.result).pipe(Effect.mapError(() => AcpGateway.failure("Invalid")))
        const resultOperationId = operation.operationId
        let latest = yield* waitFor(result.submissionId, false)
        const finished = yield* Deferred.make<SubmissionSnapshot, OperationError>()
        yield* Deferred.complete(finished, waitFor(result.submissionId, true).pipe(
          Effect.tap((sub) => Effect.sync(() => { latest = sub })),
          Effect.flatMap((sub) => sub.status._tag === "failed" ? Effect.fail(AcpGateway.failure("AgentFailure")) : Effect.succeed(sub)))).pipe(Effect.forkIn(owned))
        const accepted = yield* Deferred.make<string, OperationError>()
        yield* Deferred.complete(accepted, descriptor.version === 1 ? Effect.fail(new AcpCapabilityUnsupported({ operation: "session/prompt acceptance", version: 1 })) : Effect.gen(function*() {
          const operation = yield* network(gateway.wait(resultOperationId))
          if (operation.error) return yield* operation.error
          const accepted = yield* Schema.decodeUnknownEffect(AcpGateway.SubmissionResult)(operation.result).pipe(Effect.mapError(() => AcpGateway.failure("Invalid")))
          if (accepted.agentMessageId === null) return yield* AcpGateway.failure("Invalid")
          return accepted.agentMessageId
        })).pipe(Effect.forkIn(owned))
        return { id: result.submissionId,
          snapshot: Effect.sync(() => { latest = Object.hasOwn(snapshot!.submissions, result.submissionId) ? snapshot!.submissions[result.submissionId]! : latest; return latest }),
          accepted: Deferred.await(accepted), outcome: Deferred.await(finished) }

      }),
      cancel: Effect.asVoid(command({ _tag: "Cancel", session: descriptor.session })),
      resolveInteraction: (interactionId, resolution) => Effect.asVoid(command({ _tag: "Resolve", session: descriptor.session, interactionId, resolution })),
      setConfigOption: (configId, value) => Effect.asVoid(command({ _tag: "Configure", session: descriptor.session, configId, value })),
      setMode: (modeId) => Effect.asVoid(command({ _tag: "Mode", session: descriptor.session, modeId })),
      close: Effect.asVoid(command({ _tag: "Close", session: descriptor.session })),
      delete: Effect.asVoid(command({ _tag: "Delete", session: descriptor.session }))
    }
    yield* Scope.addFinalizer(owned, Effect.suspend(() => {
      if (controllers.get(descriptor.session) === generation) controllers.delete(descriptor.session)
      failed = AcpGateway.failure("Closed")
      return Effect.forEach(observers, Queue.end, { discard: true })
    }))
    return session
  })
  // The key includes caller-supplied metadata and takeover intent so a fresh
  // descriptor cannot silently inherit a handle created for different inputs.
  const entries = new Map<string, symbol>()
  const requests = new Map<string, { descriptor: SessionDescriptor; takeover: boolean; borrowers: number }>()
  const mapScope = yield* Scope.make()
  const attachments = yield* RcMap.make({ lookup: (cacheKey: string) => {
    const request = requests.get(cacheKey)!
    const token = Symbol()
    entries.set(cacheKey, token)
    return acquire(request.descriptor, request.takeover, cacheKey, token)
  } }).pipe(Scope.provide(mapScope))
  const attach = (descriptor: SessionDescriptor, takeover = false): Effect.Effect<AcpSession, AcpGateway.GatewayError, Scope.Scope> => Effect.gen(function*() {
    if (descriptor.epoch !== gateway.window.epoch) return yield* AcpGateway.failure("HostRestarted")
    const borrowed = yield* Scope.fork(yield* Scope.Scope)
    const cacheKey = `${descriptor.session.length}:${descriptor.session}${descriptor.sessionId.length}:${descriptor.sessionId}${descriptor.version}${takeover ? 1 : 0}`
    // Check and publish the claim in one uninterruptible step. Other keys can
    // inspect it even before RcMap's lookup receives its first frame.
    yield* Effect.uninterruptible(Effect.suspend(() => {
      for (const request of requests.values()) {
        if (request.descriptor.session === descriptor.session &&
          (request.descriptor.sessionId !== descriptor.sessionId || request.descriptor.version !== descriptor.version)) {
          return Effect.fail(AcpGateway.failure("Invalid"))
        }
      }
      let request = requests.get(cacheKey)
      if (!request) {
        request = { descriptor, takeover, borrowers: 0 }
        requests.set(cacheKey, request)
      }
      request.borrowers++
      return Scope.addFinalizer(borrowed, Effect.sync(() => {
        request.borrowers--
        if (request.borrowers === 0 && !entries.has(cacheKey) && requests.get(cacheKey) === request) requests.delete(cacheKey)
      }))
    })).pipe(Effect.onError(() => Scope.close(borrowed, Exit.void)))
    const shared = yield* RcMap.get(attachments, cacheKey).pipe(
      Scope.provide(borrowed), Effect.onError(() => Scope.close(borrowed, Exit.void)))
    const ensureBorrowed = Effect.suspend(() => borrowed.state._tag === "Closed"
      ? Effect.fail(AcpGateway.failure("Closed")) : Effect.void)
    const held = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.flatMap(ensureBorrowed, () => effect)
    const handle: AcpSession = {
      ...shared,
      release: Scope.close(borrowed, Exit.void),
      submit: (prompt) => held(shared.submit(prompt)),
      cancel: held(shared.cancel),
      resolveInteraction: (interactionId, resolution) => held(shared.resolveInteraction(interactionId, resolution)),
      setConfigOption: (configId, value) => held(shared.setConfigOption(configId, value)),
      setMode: (modeId) => held(shared.setMode(modeId)),
      close: held(shared.close),
      delete: held(shared.delete)
    }
    descriptors.set(handle, descriptor)
    yield* Scope.addFinalizer(borrowed, Effect.sync(() => { descriptors.delete(handle) }))
    return handle
  })
  const connect: AcpClient["Service"]["connect"] = () => Effect.gen(function*() {
    const connectionScope = yield* Scope.Scope
    const key = `${gateway.prefix}:connection:${options.connectionKey ?? options.profile}`
    const cached = yield* gateway.storage.load(key).pipe(Effect.flatMap(Schema.decodeUnknownEffect(RetainedConnection)), Effect.mapError(() => AcpGateway.failure("Invalid")))
    if (cached && cached.epoch !== gateway.window.epoch) return yield* AcpGateway.failure("HostRestarted")
    let descriptor = cached?.epoch === gateway.window.epoch ? cached.descriptor : undefined
    if (!descriptor) {
      const operationId = cached?.epoch === gateway.window.epoch ? cached.operationId : (yield* randomUUID.pipe(Effect.mapError(() => AcpGateway.failure("Invalid"))))
      yield* gateway.storage.save(key, { epoch: gateway.window.epoch, operationId })
      let operation = cached?.epoch === gateway.window.epoch
        ? yield* network(gateway.retry(operationId))
        : yield* network(gateway.submit({ _tag: "Open", profile: options.profile, options: options.profileOptions ?? null }, undefined, operationId))
      if (operation.status === "admitted") operation = yield* network(gateway.wait(operationId))
      if (operation.error) return yield* AcpGateway.failure(operation.error._tag === "AcpGatewayError" ? operation.error.code : "AgentFailure")
      descriptor = yield* Schema.decodeUnknownEffect(AcpGateway.ConnectionDescriptor)(operation.result).pipe(Effect.mapError(() => AcpGateway.failure("Invalid")))
      yield* gateway.storage.save(key, { epoch: gateway.window.epoch, operationId, descriptor })
    }
    const connection = descriptor.connection
    const authMethods = descriptor.negotiated.response.authMethods
    const establish = (command: AcpGateway.Command) => run(command).pipe(Effect.flatMap((value) => Schema.decodeUnknownEffect(AcpGateway.SessionDescriptor)(value).pipe(Effect.mapError(() => AcpGateway.failure("Invalid")))), Effect.flatMap((descriptor) => Scope.provide(attach(descriptor), connectionScope)))
    const sessionOptions = ({ cwd, additionalDirectories, mcpServers }: NewSessionOptions) => ({
      cwd,
      ...(additionalDirectories === undefined ? {} : { additionalDirectories }),
      ...(mcpServers === undefined ? {} : { mcpServers })
    })
    return {
      capabilities: descriptor.capabilities, negotiated: descriptor.negotiated,
      closed: Effect.raceFirst(Effect.asVoid(gateway.api.Closed({ epoch: gateway.window.epoch, workspace: gateway.window.workspace, connection })).pipe(Effect.ignore), gateway.disconnected).pipe(
        Effect.as(new AcpConnectionClosed({ message: "Hosted connection closed" }))),
      request: (method, params) => Effect.suspend(() => run({ _tag: "Extension", connection, method, params: params ?? null, controllers: Object.fromEntries(controllers) })),
      newSession: (options) => establish({ _tag: "NewSession", connection, options: sessionOptions(options) }),
      resumeSession: (options) => establish({ _tag: "ResumeSession", connection, options: {
        ...sessionOptions(options),
        sessionId: options.sessionId,
        ...(options.replayFrom === undefined ? {} : { replayFrom: options.replayFrom })
      } }),
      listSessions: (cwd) => network(gateway.api.List({ epoch: gateway.window.epoch, workspace: gateway.window.workspace, connection, ...(cwd ? { cwd } : {}) })),
      authenticate: (methodId) => Effect.gen(function*() {
        yield* run({ _tag: "Authenticate", connection, methodId })
        const method = authMethods?.find((method) => authMethodId(method) === methodId)
        if (method && "type" in method && method.type === "terminal") {
          yield* Effect.uninterruptible(Effect.gen(function*() {
            const retained = yield* gateway.storage.load(key).pipe(Effect.flatMap(Schema.decodeUnknownEffect(RetainedConnection)), Effect.mapError(() => AcpGateway.failure("Invalid")))
            if (retained?.epoch === gateway.window.epoch && retained.descriptor?.connection === connection) yield* gateway.storage.remove(key)
          }))
        }
      }),
      logout: Effect.asVoid(run({ _tag: "Logout", connection }))
    } satisfies AcpAgentConnection
  })
  return { connect, attach, descriptor: (session: AcpSession): SessionDescriptor | undefined => descriptors.get(session) }
})
/**
 * Provides the application client service through a gateway connection.
 *
 * **Details**
 *
 * The gateway client and its socket lifetime must be supplied by the application.
 *
 * @category layers
 */
export const layer = (gateway: Client, options: Options) => Layer.effect(AcpClient, make(gateway, options))
