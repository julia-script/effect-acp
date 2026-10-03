import * as Result from "effect/Result"
import { randomUUID } from "./internal/crypto.ts"
import * as Json from "./internal/json.ts"
/**
 * In-memory host ownership, recovery journal, and bounded command admission.
 */
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import type { SessionSnapshot } from "./AcpApp.ts"
import type { AcpAgentConnection, AcpSession, ConnectOptions } from "./AcpClient.ts"
import * as AcpGateway from "./AcpGateway.ts"
import * as Semaphore from "effect/Semaphore"
import * as Errors from "./AcpSessionError.ts"
import * as Schema from "effect/Schema"

/**
 * Finite retention, shutdown, buffering, and capacity limits for a hosted agent runtime.
 *
 * **Gotchas**
 *
 * Every field must be a positive safe integer. Time limits are in milliseconds; byte budgets and
 * record counts are separate limits.
 *
 * @category configuration
 */
export interface Policy {
  /**
   * Milliseconds to retain a detached session before owner cleanup.
   */
  readonly retentionMs: number
  /**
   * Milliseconds allowed for pending user interactions.
   */
  readonly interactionMs: number
  /**
   * Milliseconds allowed for cancellation confirmation and agent cleanup.
   */
  readonly shutdownMs: number
  /**
   * Milliseconds an admission window remains valid for command recovery.
   */
  readonly retryMs: number
  /**
   * Maximum snapshot events retained per session recovery journal.
   */
  readonly events: number
  /**
   * Maximum serialized bytes retained per session recovery journal.
   */
  readonly eventBytes: number
  /**
   * Maximum queued frames for a session controller before resynchronization is required.
   */
  readonly subscriberCapacity: number
  /**
   * Serialized session-content budget enforced when opening agent connections.
   */
  readonly transcriptBytes: number
  /**
   * Retained output-byte budget per display terminal.
   */
  readonly terminalBytes: number
  /**
   * Maximum retained command records and maximum active admission windows.
   */
  readonly commands: number
  /**
   * Maximum owned agent connections, including connections being opened.
   */
  readonly connections: number
  /**
   * Maximum hosted sessions, including sessions being created.
   */
  readonly sessions: number
}
/**
 * Workspace, operation, and optional hosted resource identifiers passed to authorization.
 *
 * @category models
 */
export interface Access {
  /**
   * Workspace whose retained resources or operations are being authorized.
   */
  readonly workspace: string
  /**
   * Ownership boundary being checked before the operation proceeds.
   */
  readonly action: "open" | "read" | "attach" | "takeover" | "command"
  /**
   * Hosted connection identifier, when the operation addresses a connection.
   */
  readonly connection?: string
  /**
   * Hosted session identifier, when the operation addresses a session.
   */
  readonly session?: string
}
/**
 * Host capacity policy and application callbacks for authorization, agent opening, and lifecycle
 * reporting.
 *
 * @category configuration
 */
export interface Options<R = never, E = never> {
  /**
   * Required positive capacity and retention limits; no implicit host defaults are supplied.
   */
  readonly policy: Policy
  /**
   * Application ownership checks; invoked before accessing retained data.
   */
  readonly authorize: (identity: AcpGateway.Identity, access: Access) => Effect.Effect<void, AcpGateway.GatewayError, R>
  /**
   * Observes ownership and admission transitions; callback failures are ignored.
   */
  readonly onLifecycle?: (event: { readonly type: "opened" | "attached" | "detached" | "expired" | "admitted" | "settled"; readonly connections: number; readonly sessions: number; readonly commands: number }) => Effect.Effect<void>
  /**
   * Resolves an authorized launch profile and opens it in the host-owned scope using enforced
   * runtime limits.
   * Client arguments describe profile options rather than selecting executable paths.
   */
  readonly open: (identity: AcpGateway.Identity, workspace: string, profile: string, options: unknown, enforced: Pick<ConnectOptions, "interactionTimeout" | "cancelTimeout" | "limits" | "observerCapacity">) => Effect.Effect<AcpAgentConnection, import("./AcpClient.ts").ConnectError | E, R | Scope.Scope>
}
interface Owner {
  readonly id: string
  readonly identity: string
  readonly workspace: string
  readonly clientId: string
  readonly connection: AcpAgentConnection
  readonly scope: Scope.Closeable
  readonly lock: Semaphore.Semaphore
  readonly sessions: Set<string>
  idle?: Fiber.Fiber<void>
  closed: boolean
}
interface Session {
  readonly id: string
  readonly owner: Owner
  readonly handle: AcpSession
  readonly scope: Scope.Closeable
  snapshot: SessionSnapshot
  sequence: number
  journal: Array<{ readonly event: AcpGateway.Frame & { readonly _tag: "Event" }; readonly bytes: number }>
  bytes: number
  floor: number
  generation: number
  controller?: { clientId: string; generation: number; queue: Queue.Queue<AcpGateway.Frame, AcpGateway.GatewayError | Cause.Done> }
  expiry?: Fiber.Fiber<void>
  closing: boolean
}
interface WindowRecord { readonly value: AcpGateway.Window; readonly principal: string }
interface RecordEntry { readonly key: string; readonly payload: string; readonly window: WindowRecord; value: AcpGateway.Operation }
const sameWindow = Schema.toEquivalence(AcpGateway.Window)
const textBytes = (value: string) => new TextEncoder().encode(value).byteLength
const canonicalValue = (value: Schema.Json): Schema.Json => {
  if (Array.isArray(value)) return value.map(canonicalValue)
  if (value !== null && typeof value === "object") return Object.fromEntries(
    Object.entries(value).sort(([a], [b]) => {
      if (a === b) return 0
      return a < b ? -1 : 1
    }).map(([key, item]) => [key, canonicalValue(item)]))
  return value
}
// Serialize before traversing: cycles and BigInt fail before ledger admission.
const canonical = (value: unknown) => Json.encode(value).pipe(
  Effect.flatMap(Json.decode),
  Effect.flatMap((decoded) => Json.encode(canonicalValue(decoded))),
  Effect.tapCause((cause) => Cause.hasInterruptsOnly(cause) ? Effect.void : Effect.logError("Cannot serialize hosted command")),
  Effect.mapError(() => AcpGateway.failure("Invalid"))
)
const hostedId = randomUUID.pipe(
  Effect.tapCause((cause) => Cause.hasInterruptsOnly(cause) ? Effect.void : Effect.logError("Cannot generate hosted ID")),
  Effect.mapError(() => AcpGateway.failure("Invalid"))
)
const safe = (cause: Cause.Cause<unknown>) => {
  const reason = cause.reasons.length === 1 ? cause.reasons[0] : undefined
  const error = reason && Cause.isFailReason(reason) ? reason.error : undefined
  if (Schema.is(AcpGateway.CommandError)(error)) {
    switch (error._tag) {
      case "AcpGatewayError": return AcpGateway.failure(error.code)
      case "AcpCapabilityUnsupported": return new Errors.AcpCapabilityUnsupported({ operation: error.operation, version: error.version })
      case "AcpSessionBusy": return new Errors.AcpSessionBusy({ message: "Session has foreground work" })
      case "AcpHistoryUnavailable": return new Errors.AcpHistoryUnavailable({ sessionId: error.sessionId, operation: error.operation })
      case "AcpInteractionAlreadyResolved":
      case "AcpInteractionExpired": return error
      default: return error satisfies never
    }
  }
  return typeof error === "object" && error !== null && "_tag" in error && (error._tag === "AcpConnectionClosed" || error._tag === "AcpTimeoutError") ? AcpGateway.failure("OutcomeUnknown") : AcpGateway.failure("AgentFailure")
}
/**
 * Creates a scoped host that owns agent connections, session attachments, and a bounded operation
 * journal.
 *
 * **When to use**
 *
 * Use when sessions must outlive individual browser connections and commands need admission and
 * recovery tracking.
 *
 * **Details**
 *
 * Captures callback dependencies and assigns a new host epoch. Authorization runs before retained
 * resources are accessed.
 *
 * **Gotchas**
 *
 * State is in memory and expires according to `policy` ; restarting the host invalidates retained
 * identifiers. Invalid policy values fail with gateway code `Invalid` .
 *
 * @category constructors
 */
export const make = <R, E>(options: Options<R, E>) => Effect.gen(function*() {
  const policy = options.policy
  for (const key of ["retentionMs", "interactionMs", "shutdownMs", "retryMs", "events", "eventBytes", "subscriberCapacity", "transcriptBytes", "terminalBytes", "commands", "connections", "sessions"] as const) {
    if (!Number.isSafeInteger(policy[key]) || policy[key] <= 0) return yield* AcpGateway.failure("Invalid")
  }
  const scope = yield* Scope.Scope
  const services = yield* Effect.context<R>()
  const authorize = (identity: AcpGateway.Identity, access: Access) => options.authorize(identity, access).pipe(Effect.provideContext(services))
  const epoch = yield* hostedId
  const owners = new Map<string, Owner>()
  const sessions = new Map<string, Session>()
  const windows = new Map<string, WindowRecord>()
  const ledger = new Map<string, RecordEntry>()
  let opening = 0
  let creating = 0
  let stopping = false
  const report = (type: "opened" | "attached" | "detached" | "expired" | "admitted" | "settled") =>
    options.onLifecycle?.({ type, connections: owners.size, sessions: sessions.size, commands: ledger.size }).pipe(Effect.ignoreCause) ?? Effect.void
  const checkEpoch = (value: string) => value === epoch ? Effect.void : Effect.fail(AcpGateway.failure("HostRestarted"))
  const windowFor = (identity: AcpGateway.Identity, value: AcpGateway.Window) => Effect.gen(function*() {
    yield* checkEpoch(value.epoch)
    const now = yield* Clock.currentTimeMillis
    const record = windows.get(value.token)
    if (value.expiresAt <= now) return yield* AcpGateway.failure("WindowExpired")
    if (!record || record.principal !== identity.principalId || !sameWindow(record.value, value)) return yield* AcpGateway.failure("Unauthorized")
    yield* authorize(identity, { workspace: value.workspace, action: "command" })
    return record
  })
  const ownerFor = (identity: AcpGateway.Identity, workspace: string, id: string) => Effect.gen(function*() {
    yield* authorize(identity, { workspace, connection: id, action: "read" })
    const owner = owners.get(id)
    if (!owner || owner.identity !== identity.principalId || owner.workspace !== workspace) return yield* AcpGateway.failure("NotFound")
    if (owner.closed) return yield* AcpGateway.failure("Closed")
    return owner
  })
  const sessionFor = (identity: AcpGateway.Identity, workspace: string, id: string, action: Access["action"]) => Effect.gen(function*() {
    yield* authorize(identity, { workspace, session: id, action })
    const session = sessions.get(id)
    if (!session || session.owner.identity !== identity.principalId || session.owner.workspace !== workspace) return yield* AcpGateway.failure("NotFound")
    if (session.closing) return yield* AcpGateway.failure("Closed")
    return session
  })
  const closeOwner = (owner: Owner) => Effect.suspend(() => {
    owner.closed = true
    owners.delete(owner.id)
    return Scope.close(owner.scope, Exit.void)
  })
  const expire = (session: Session, cleanupAgent = true) => Effect.gen(function*() {
    if (session.closing) return
    session.closing = true
    const cleanup = session.handle.cancel.pipe(Effect.ignore, Effect.andThen(Effect.ignore(session.handle.close)))
    if (cleanupAgent) yield* cleanup.pipe(Effect.timeoutOption(policy.shutdownMs), Effect.ignore)
    yield* session.handle.release
    sessions.delete(session.id)
    session.owner.sessions.delete(session.id)
    yield* report("expired")
    if (session.controller) yield* Queue.fail(session.controller.queue, AcpGateway.failure("Closed"))
    // The observer is owned by a separate scope, so expiring one session does not stop its siblings.
    yield* Scope.close(session.scope, Exit.void)
    if (session.owner.sessions.size === 0) yield* closeOwner(session.owner)
  })
  const scheduleExpiry = (session: Session) => Effect.gen(function*() {
    if (session.expiry) yield* Fiber.interrupt(session.expiry)
    session.expiry = yield* Effect.sleep(policy.retentionMs).pipe(Effect.andThen(expire(session)), Effect.interruptible, Effect.forkIn(scope))
  })
  const cursor = (session: Session, sequence = session.sequence): AcpGateway.Cursor => ({ epoch, session: session.id, sequence })
  const publish = (session: Session, snapshot: SessionSnapshot) => Effect.gen(function*() {
    if (session.closing) return
    session.snapshot = snapshot
    const event: AcpGateway.Frame & { _tag: "Event" } = { _tag: "Event", cursor: cursor(session, ++session.sequence), snapshot }
    const encoded = yield* Json.encode(event).pipe(Effect.result)
    if (Result.isFailure(encoded)) {
      yield* Effect.logError("Cannot serialize hosted session snapshot")
      return yield* expire(session)
    }
    const size = textBytes(encoded.success)
    session.journal.push({ event, bytes: size })
    session.bytes += size
    while (session.journal.length > policy.events || session.bytes > policy.eventBytes) {
      const removed = session.journal.shift()!
      session.bytes -= removed.bytes
      session.floor = removed.event.cursor.sequence
    }
    const controller = session.controller
    if (controller && !Queue.offerUnsafe(controller.queue, event)) {
      Queue.failCauseUnsafe(controller.queue, Cause.fail(AcpGateway.failure("ResyncRequired")))
      delete session.controller
      return yield* scheduleExpiry(session)
    }
  })
  const register = (owner: Owner, handle: AcpSession) => Effect.gen(function*() {
    const existing = [...sessions.values()].find((s) => s.owner === owner && s.handle.sessionId === handle.sessionId)
    if (existing) return { epoch, session: existing.id, sessionId: handle.sessionId, version: handle.version }
    if (sessions.size >= policy.sessions) return yield* AcpGateway.failure("Capacity")
    const owned = yield* Scope.fork(scope)
    const observerScope = yield* Scope.fork(owned)
    const observed = yield* Scope.provide(handle.observe, observerScope)
    const session: Session = { id: (yield* hostedId), owner, handle, scope: owned, snapshot: observed.snapshot, sequence: 0,
      journal: [], bytes: 0, floor: 0, generation: 0, closing: false }
    sessions.set(session.id, session)
    owner.sessions.add(session.id)
    if (owner.idle) yield* Fiber.interrupt(owner.idle)
    // On local observer overflow, refresh from an atomic boundary and publish a complete snapshot.
    const watch = (changes: typeof observed.changes, observerScope: Scope.Closeable): Effect.Effect<void, AcpGateway.GatewayError> => Stream.runForEach(changes, (event) => publish(session, event.snapshot)).pipe(
      Effect.catchTag("AcpSubscriptionOverflow", () => Effect.gen(function*() {
        yield* Scope.close(observerScope, Exit.void)
        const freshScope = yield* Scope.fork(owned)
        const fresh = yield* Scope.provide(handle.observe, freshScope)
        yield* publish(session, fresh.snapshot)
        yield* watch(fresh.changes, freshScope)
      })))
    yield* watch(observed.changes, observerScope).pipe(Effect.forkIn(owned))
    yield* scheduleExpiry(session)
    return { epoch, session: session.id, sessionId: handle.sessionId, version: handle.version }
  })
  const hello = (identity: AcpGateway.Identity, input: { version: number; workspace: string; clientId: string }) => Effect.gen(function*() {
    if (input.version !== AcpGateway.version) return yield* AcpGateway.failure("UnsupportedVersion")
    yield* authorize(identity, { workspace: input.workspace, action: "open" })
    if (stopping) return yield* AcpGateway.failure("Closed")
    const now = yield* Clock.currentTimeMillis
    for (const [token, record] of windows) if (record.value.expiresAt <= now) windows.delete(token)
    for (const [key, record] of ledger) if (record.window.value.expiresAt <= now && record.value.status !== "admitted") ledger.delete(key)
    const existing = [...windows.values()].find((w) => w.principal === identity.principalId && w.value.clientId === input.clientId && w.value.workspace === input.workspace)
    if (existing) return existing.value
    if (windows.size >= policy.commands) return yield* AcpGateway.failure("Capacity")
    const value: AcpGateway.Window = { token: (yield* hostedId), epoch, workspace: input.workspace, clientId: input.clientId, expiresAt: now + policy.retryMs }
    windows.set(value.token, { value, principal: identity.principalId })
    return value
  })
  const execute = (identity: AcpGateway.Identity, window: AcpGateway.Window, command: AcpGateway.Command, progress: (value: typeof AcpGateway.SubmissionResult.Type) => void): Effect.Effect<unknown, import("./AcpClient.ts").OperationError | import("./AcpClient.ts").ConnectError | E> => Effect.gen(function*() {
    if (command._tag === "Open") {
      if (owners.size + opening >= policy.connections) return yield* AcpGateway.failure("Capacity")
      opening++
      const owned = yield* Scope.fork(scope)
      const connected = yield* options.open(identity, window.workspace, command.profile, command.options, {
        interactionTimeout: policy.interactionMs, cancelTimeout: policy.shutdownMs,
        limits: { transcriptBytes: policy.transcriptBytes, terminalBytes: policy.terminalBytes }, observerCapacity: policy.subscriberCapacity
      }).pipe(Scope.provide(owned), Effect.provideContext(services), Effect.exit, Effect.ensuring(Effect.sync(() => { opening-- })))
      if (Exit.isFailure(connected)) { yield* Scope.close(owned, Exit.void); return yield* Effect.failCause(connected.cause) }
      const owner: Owner = { id: (yield* hostedId), identity: identity.principalId, workspace: window.workspace,
        clientId: window.clientId, connection: connected.value, scope: owned, lock: Semaphore.makeUnsafe(1), sessions: new Set(), closed: false }
      owners.set(owner.id, owner)
      yield* report("opened")
      yield* owner.connection.closed.pipe(Effect.andThen(Effect.sync(() => { owner.closed = true })), Effect.forkIn(owned))
      owner.idle = yield* Effect.sleep(policy.retentionMs).pipe(Effect.andThen(Effect.suspend(() => owner.sessions.size ? Effect.void : closeOwner(owner))), Effect.interruptible, Effect.forkIn(scope))
      return { connection: owner.id, capabilities: owner.connection.capabilities, negotiated: owner.connection.negotiated }
    }
    if ("connection" in command) {
      const owner = yield* ownerFor(identity, window.workspace, command.connection)
      switch (command._tag) {
        case "NewSession":
        case "ResumeSession": return yield* Semaphore.withPermit(owner.lock, Effect.gen(function*() {
          if (command._tag === "ResumeSession") {
            const retained = [...sessions.values()].find((s) => s.owner === owner && s.handle.sessionId === command.options.sessionId)
            if (retained) return { epoch, session: retained.id, sessionId: retained.handle.sessionId, version: retained.handle.version }
          }
          if (sessions.size + creating >= policy.sessions) return yield* AcpGateway.failure("Capacity")
          creating++
          return yield* (command._tag === "NewSession" ? owner.connection.newSession(command.options) : owner.connection.resumeSession(command.options)).pipe(
            Effect.flatMap((session) => register(owner, session)), Effect.ensuring(Effect.sync(() => { creating-- })))
        }))
        case "Authenticate": return yield* owner.connection.authenticate(command.methodId)
        case "Logout": return yield* owner.connection.logout
        case "Extension": return yield* owner.connection.request(command.method, command.params)
      }
    }
    const session = yield* sessionFor(identity, window.workspace, command.session, "command")
    switch (command._tag) {
      case "Submit": {
        const submission = yield* session.handle.submit(command.prompt)
        progress({ submissionId: submission.id, agentMessageId: null, acceptanceUnavailable: session.handle.version === 1 })
        const accepted = yield* Effect.exit(session.handle.version === 1 ? Effect.as(submission.outcome, null) : submission.accepted)
        if (Exit.isFailure(accepted)) return yield* Effect.failCause(accepted.cause)
        return { submissionId: submission.id, agentMessageId: Exit.isSuccess(accepted) ? accepted.value : null, acceptanceUnavailable: session.handle.version === 1 }
      }
      case "Cancel": return yield* session.handle.cancel
      case "Configure": return yield* session.handle.setConfigOption(command.configId, command.value)
      case "Mode": return yield* session.handle.setMode(command.modeId)
      case "Resolve": return yield* session.handle.resolveInteraction(command.interactionId, command.resolution)
      case "Close": yield* session.handle.close; return yield* expire(session, false)
      case "Delete": yield* session.handle.delete; return yield* expire(session, false)
    }
  })
  const admit = (identity: AcpGateway.Identity, input: AcpGateway.Admission) => Effect.gen(function*() {
    const window = yield* windowFor(identity, input.window)
    yield* authorize(identity, { workspace: window.value.workspace, action: "command",
      ...("session" in input.command ? { session: input.command.session } : {}),
      ...("connection" in input.command ? { connection: input.command.connection } : {}) })
    const prior = ledger.get(`${window.value.token}:${input.operationId}`)
    if (prior) {
      if (prior.payload !== (yield* canonical(input.command))) return yield* AcpGateway.failure("Conflict")
      return prior.value
    }
    const target = "session" in input.command ? yield* sessionFor(identity, window.value.workspace, input.command.session, "command") : undefined
    if ("connection" in input.command) {
      const owner = yield* ownerFor(identity, window.value.workspace, input.command.connection)
      if (owner.clientId !== window.value.clientId) return yield* AcpGateway.failure("Unauthorized")
      if (input.command._tag === "Extension") {
        for (const id of owner.sessions) {
          const controller = sessions.get(id)?.controller
          if (!controller || controller.clientId !== window.value.clientId || input.command.controllers?.[id] !== controller.generation) return yield* AcpGateway.failure("StaleController")
        }
      }
    }
    // Only admission and launch are masked; authorization remains interruptible.
    return yield* Effect.uninterruptible(Effect.gen(function*() {
    if (stopping) return yield* AcpGateway.failure("Closed")
    if (target && (!target.controller || target.controller.generation !== input.generation || target.controller.clientId !== window.value.clientId)) return yield* AcpGateway.failure("StaleController")
    const key = `${window.value.token}:${input.operationId}`
    const payload = yield* canonical(input.command)
    const previous = ledger.get(key)
    if (previous) {
      if (previous.payload !== payload) return yield* AcpGateway.failure("Conflict")
      return previous.value
    }
    if (textBytes(payload) > policy.eventBytes) return yield* AcpGateway.failure("Capacity")
    if (input.command._tag === "Extension" && !input.command.method.startsWith("_")) return yield* AcpGateway.failure("Invalid")
    if (ledger.size >= policy.commands) return yield* AcpGateway.failure("Capacity")
    const record: RecordEntry = { key, payload, window, value: { operationId: input.operationId, status: "admitted", result: null, error: null } }
    ledger.set(key, record)
    yield* execute(identity, window.value, input.command, (result) => { record.value = { ...record.value, result } }).pipe(
      Effect.matchCauseEffect({
        onSuccess: (result) => Effect.sync(() => { record.value = { ...record.value, status: "succeeded", result: result ?? null } }),
        onFailure: (cause) => Cause.hasInterruptsOnly(cause) ? Effect.failCause(cause) : Effect.andThen(
          Effect.logError("Hosted operation failed"),
          Effect.sync(() => {
            const failure = safe(cause)
            record.value = { ...record.value, status: failure._tag === "AcpGatewayError" && failure.code === "OutcomeUnknown" ? "outcomeUnknown" : "failed", error: failure }
          }))
      }), Effect.ensuring(report("settled")), Effect.interruptible, Effect.forkIn(scope))
    yield* report("admitted")
    return record.value
    }))
  })
  const operation = (identity: AcpGateway.Identity, input: { window: AcpGateway.Window; operationId: string }) => Effect.gen(function*() {
    const window = yield* windowFor(identity, input.window)
    const record = ledger.get(`${window.value.token}:${input.operationId}`)
    if (!record) return yield* AcpGateway.failure("NotFound")
    const command = yield* Schema.decodeEffect(Schema.fromJsonString(AcpGateway.Command))(record.payload).pipe(Effect.mapError(() => AcpGateway.failure("Invalid")))
    yield* authorize(identity, { workspace: window.value.workspace, action: "read", ...("session" in command ? { session: command.session } : {}), ...("connection" in command ? { connection: command.connection } : {}) })
    return record.value
  })
  const attach = (identity: AcpGateway.Identity, input: AcpGateway.AttachmentRequest) => Stream.unwrap(Effect.gen(function*() {
    yield* checkEpoch(input.epoch)
    const session = yield* sessionFor(identity, input.workspace, input.session, input.takeover ? "takeover" : "attach")
    const queue = yield* Effect.acquireRelease(
      Queue.bounded<AcpGateway.Frame, AcpGateway.GatewayError | Cause.Done>(policy.subscriberCapacity), Queue.shutdown)
    const { initial, replay } = yield* Effect.uninterruptible(Effect.gen(function*() {
      // Validate authoritative metadata, replace control, and capture the observation
      // boundary synchronously. Authorization remains outside this masked transition.
      const { previous, generation, initial, replay } = yield* Effect.suspend(() => {
        if (sessions.get(session.id) !== session || session.closing) return Effect.fail(AcpGateway.failure("Closed"))
        if (input.expected && (input.expected.sessionId !== session.handle.sessionId || input.expected.version !== session.handle.version)) {
          return Effect.fail(AcpGateway.failure("Invalid"))
        }
        const requested = input.cursor
        if (requested && (requested.epoch !== epoch || requested.session !== session.id || requested.sequence < 0 || requested.sequence > session.sequence)) return Effect.fail(AcpGateway.failure("Invalid"))
        if (session.controller && !input.takeover) return Effect.fail(AcpGateway.failure("Conflict"))
        const previous = session.controller
        const generation = ++session.generation
        session.controller = { clientId: input.clientId, generation, queue }
        const resync = requested !== undefined && requested.sequence < session.floor
        const initial: AcpGateway.Frame = { _tag: "Attached", cursor: cursor(session), generation, resync,
          snapshot: !requested || resync ? session.snapshot : null }
        const replay = requested && !resync ? session.journal.filter(({ event }) => event.cursor.sequence > requested.sequence).map(({ event }) => event) : []
        return Effect.succeed({ previous, generation, initial, replay })
      })
      yield* Effect.addFinalizer(() => Effect.gen(function*() {
        if (session.controller?.generation === generation) {
          delete session.controller
          yield* report("detached")
          if (!session.closing) yield* scheduleExpiry(session)
        }
      }))
      if (previous) yield* Queue.fail(previous.queue, AcpGateway.failure("StaleController"))
      return { initial, replay }
    }))
    if (session.expiry) { const timer = session.expiry; delete session.expiry; yield* Fiber.interrupt(timer) }
    yield* report("attached")
    return Stream.concat(Stream.fromIterable([initial, ...replay]), Stream.fromQueue(queue))
  }))
  const list = (identity: AcpGateway.Identity, input: { epoch: string; workspace: string; connection: string; cwd?: string }) => Effect.gen(function*() {
    yield* checkEpoch(input.epoch)
    const owner = yield* ownerFor(identity, input.workspace, input.connection)
    return yield* owner.connection.listSessions(input.cwd).pipe(
      Effect.tapCause((cause) => Cause.hasInterruptsOnly(cause) ? Effect.void : Effect.logError("Hosted session list failed")),
      Effect.mapError(() => AcpGateway.failure("AgentFailure")))
  })
  yield* Effect.addFinalizer(() => Effect.sync(() => { stopping = true }))
  return { epoch, hello, admit, operation, attach, list, closed: (identity: AcpGateway.Identity, input: { epoch: string; workspace: string; connection: string }) => Effect.gen(function*() {
    yield* checkEpoch(input.epoch)
    const owner = yield* ownerFor(identity, input.workspace, input.connection)
    yield* owner.connection.closed
  }) }
})
/**
 * Host-owned connection, session, command, and attachment operations.
 *
 * **Details**
 *
 * - `epoch` identifies the current host lifetime; recovery requests must match it.
 * - `hello` issues a principal-bound admission window after workspace authorization.
 * - `admit` deduplicates command identities and returns their recorded state.
 * - `operation` reads a recorded command result using its admission window.
 * - `attach` streams a session boundary and subsequent snapshots, replaying retained events when possible.
 * - `list` queries the agent's session listing for an authorized connection.
 * - `closed` waits for the underlying agent connection to terminate.
 *
 * **Gotchas**
 *
 * Attachment takeover replaces the controller generation; mutations from the old controller fail.
 * Recovery is limited by the configured journal and admission-window retention.
 *
 * @see {@link make} for host acquisition and policy validation.
 * @category services
 */
export type Service = Effect.Success<ReturnType<typeof make>>
/**
 * Context service for hosted agent ownership and command admission.
 *
 * @category services
 */
export class AcpHost extends Context.Service<AcpHost, Service>()("effect-acp/AcpHost") {}
/**
 * Builds a scoped layer providing the hosted runtime.
 *
 * @see {@link make} for ownership and retention behavior.
 *
 * @category layers
 */
export const layer = <R, E>(options: Options<R, E>) => Layer.effect(AcpHost, make(options))
