import { McpServer } from "./AcpApp.ts"
import * as V1 from "./protocol/v1/Schema.ts"
import * as V2 from "./protocol/v2/Schema.ts"
import type { RequestMethod } from "./AcpSchema.ts"
import * as Data from "effect/Data"
/**
 * Author an ACP agent from typed Effect handlers.
 *
 * **Details**
 *
 * You supply implementation metadata, a version policy, and handlers; the
 * library owns the wire: version negotiation, capability advertisement,
 * request decoding, update emission, cancellation, and error mapping. Nothing
 * here knows about models — everything an agent needs from the outside world
 * arrives through the Effect environment, including the {@link Store}.
 *
 * Prompting is split in two phases, because v1 and v2 disagree about when a
 * prompt is answered:
 *
 * - `prompt.insert` records the user message and returns its canonical
 *   `messageId`. Only its success permits a v2 `session/prompt` response.
 * - `prompt.execute` runs the foreground turn, emitting updates and returning
 *   a stop reason. v2 runs it *after* responding; v1's response waits for it.
 */
import * as Cause from "effect/Cause"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Deferred from "effect/Deferred"
import * as Exit from "effect/Exit"
import * as Schema from "effect/Schema"
import * as AcpProtocol from "./AcpProtocol.ts"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Logger from "effect/Logger"
import * as Scope from "effect/Scope"
import type * as Stdio from "effect/Stdio"
import * as AcpConnection from "./AcpConnection.ts"
import { AcpRemoteError, type AcpTransportError } from "./AcpError.ts"
import { type Method, ErrorCode } from "./AcpSchema.ts"
import { AcpTransport } from "./AcpTransport.ts"
import type { ContentBlock, Prompt } from "./agent/Content.ts"
import { type MessageRole, Store, StoreError } from "./agent/Store.ts"
import * as ProcessStdio from "./transport/ProcessStdio.ts"

/**
 * The protocol version a connection negotiated.
 *
 * @category models
 */
export type Version = 1 | 2

// -----------------------------------------------------------------------------
// Failures
// -----------------------------------------------------------------------------

/**
 * An agent's advertised capabilities do not match its installed handlers.
 *
 * **Details**
 *
 * Returned by {@link make} before anything is served, so a misconfigured agent
 * never accepts a connection it cannot honor.
 *
 * @category errors
 */
export class AcpAgentConfigError extends Data.TaggedError("AcpAgentConfigError")<{ readonly missing: string; readonly message: string }> {
  /**
   * The handler or capability that is missing or inconsistent.
   */
  constructor(missing: string, message: string) {
    super({ message, missing })
  }
}

/**
 * A handler rejected an operation. `code` is sent to the client verbatim; defects are *not*, and
 * surface as a bare Internal error.
 *
 * @category errors
 */
export class AcpAgentError extends Data.TaggedError("AcpAgentError")<{ readonly code: number; readonly message: string; readonly data: unknown }> {
  constructor(options: { readonly code?: number; readonly message: string; readonly data?: unknown }) {
    super({ message: options.message, code: options.code ?? ErrorCode.InternalError, data: options.data })
  }
}

/**
 * Rejects the current operation with `Resource not found` for an unknown session.
 *
 * @category error handling
 */
export const unknownSession = (sessionId: string): AcpAgentError =>
  new AcpAgentError({ code: ErrorCode.ResourceNotFound, message: `Unknown session ${sessionId}` })

/**
 * Rejects the current operation with `Authentication required`.
 *
 * @category error handling
 */
export const authRequired = (message = "Authentication required"): AcpAgentError =>
  new AcpAgentError({ code: ErrorCode.AuthRequired, message })

/**
 * Failures an author's handler may return.
 *
 * @category errors
 */
export type HandlerError = AcpAgentError | StoreError

// -----------------------------------------------------------------------------
// Session updates
// -----------------------------------------------------------------------------

/**
 * Terminal state of a foreground turn.
 *
 * **Details**
 *
 * V2 reports failed work as `error`; deliberate refusal remains `refusal`. V1
 * has no error stop reason, so the authoring helper maps `error` to `refusal`.
 *
 * @category models
 */
export type StopReason = "end_turn" | "max_tokens" | "max_turn_requests" | "refusal" | "cancelled" | "error"

/**
 * Version-aware update emission for one session.
 *
 * **Details**
 *
 * Every method maps to the negotiated version's `session/update` shape.
 * `messageId` is required on v2 and dropped on v1, which has no message
 * identities; emitting a v2-only update on a v1 session fails rather than
 * inventing a wire shape.
 *
 * @category models
 */
export interface Emit {
  /**
   * The negotiated version, for handlers that genuinely need to branch.
   */
  readonly version: Version
  /**
   * Appends a chunk to an agent message.
   */
  readonly agentChunk: (messageId: string, content: ContentBlock) => Effect.Effect<void, HandlerError>
  /**
   * Appends a chunk to an agent thought.
   */
  readonly thoughtChunk: (messageId: string, content: ContentBlock) => Effect.Effect<void, HandlerError>
  /**
   * Appends a chunk echoing the user's message.
   */
  readonly userChunk: (messageId: string, content: ContentBlock) => Effect.Effect<void, HandlerError>
  /**
   * Replaces a message's full content (v2 only). Clients reset any content they had accumulated for
   * `messageId` before applying it.
   */
  readonly message: (
    role: MessageRole,
    messageId: string,
    content: ReadonlyArray<ContentBlock>
  ) => Effect.Effect<void, HandlerError>
  /**
   * Sends a raw update for the negotiated version, for surfaces without a helper.
   */
  readonly raw: (update: unknown) => Effect.Effect<void, HandlerError>
}

/**
 * Session-scoped client interactions available to `prompt.execute`.
 *
 * @category models
 */
export interface Interactions {
  /**
   * Asks the client to choose a permission option, returning the selected `optionId` or `null` when
   * the client cancelled. Runs on its own request, so other traffic keeps flowing while it waits.
   */
  readonly requestPermission: (options: {
    readonly title: string
    readonly options: ReadonlyArray<{ readonly optionId: string; readonly name: string; readonly kind: V1.PermissionOptionKind }>
    readonly toolCallId?: string | undefined
  }) => Effect.Effect<string | null, HandlerError>
  /**
   * Asks the client to elicit input. Fails before sending when the client did not advertise the
   * requested `mode` .
   */
  readonly elicit: (request: Omit<V2.CreateElicitationRequest, "sessionId">) => Effect.Effect<V2.CreateElicitationResponse, HandlerError>
}

// -----------------------------------------------------------------------------
// Handlers
// -----------------------------------------------------------------------------

/**
 * What the client told us about itself during `initialize`.
 *
 * @category models
 */
export interface Peer {
  readonly version: Version
  /**
   * Client implementation metadata, when it sent any.
   */
  readonly info: { readonly name: string; readonly version: string } | null
  /**
   * Elicitation modes the client advertised.
   */
  readonly elicitation: ReadonlyArray<"form" | "url">
  /**
   * The decoded initialize params verbatim.
   */
  readonly raw: V1.InitializeRequest | V2.InitializeRequest
}

/**
 * Session identity, negotiated version, and client advertisement passed to agent handlers.
 *
 * @category models
 */
export interface SessionContext {
  /**
   * Agent-issued session identity for the operation.
   */
  readonly sessionId: string
  /**
   * Wire version selected for the current client connection.
   */
  readonly version: Version
  /**
   * Client metadata and capabilities captured during initialize.
   */
  readonly peer: Peer
}

/**
 * Normalized workspace, MCP configuration, and client advertisement for session creation.
 *
 * @category models
 */
export interface CreateSessionRequest {
  /**
   * Requested absolute working directory for the new session.
   */
  readonly cwd: string
  /**
   * Additional workspace directories; normalized to an empty array when absent.
   */
  readonly additionalDirectories: ReadonlyArray<string>
  /**
   * Requested MCP server configurations; normalized to an empty array when absent.
   */
  readonly mcpServers: ReadonlyArray<McpServer>
  /**
   * Client metadata and capabilities captured during initialize.
   */
  readonly peer: Peer
}

/**
 * Session context and prompt blocks passed to the insertion phase.
 *
 * @category models
 */
export interface InsertRequest extends SessionContext {
  /**
   * User content to record before foreground execution begins.
   */
  readonly prompt: Prompt
}

/**
 * Foreground execution context with the inserted message identity, update emitter, and client
 * interactions.
 *
 * @category models
 */
export interface ExecuteRequest extends SessionContext {
  /**
   * User content associated with this execution.
   */
  readonly prompt: Prompt
  /**
   * The message id `insert` returned for this turn.
   */
  readonly messageId: string
  /**
   * Version-aware session updates and transcript retention.
   */
  readonly emit: Emit
  /**
   * Permission and elicitation requests available during execution.
   */
  readonly client: Interactions
}

/**
 * Session lifecycle handlers. `create` is the v2 baseline requirement.
 *
 * @category models
 */
export interface SessionHandlers<R = never> {
  /**
   * Creates a session and returns its id. Required whenever sessions are served.
   */
  readonly create: (request: CreateSessionRequest) => Effect.Effect<{ readonly sessionId: string }, HandlerError, R>
  /**
   * Resumes an existing session. Advertised as `session/resume` when present.
   */
  readonly resume?: (
    request: SessionContext & { readonly cwd: string; readonly replayFromStart: boolean }
  ) => Effect.Effect<void, HandlerError, R>
  /**
   * Deletes a session. Advertised as `session.delete` when present.
   */
  readonly delete?: (request: SessionContext) => Effect.Effect<void, HandlerError, R>
  /**
   * Closes a session without deleting it.
   */
  readonly close?: (request: SessionContext) => Effect.Effect<void, HandlerError, R>
  /**
   * Cancels foreground work. Called after owned execution is interrupted.
   */
  readonly cancel?: (request: SessionContext) => Effect.Effect<void, HandlerError, R>
}

/**
 * Prompt handlers: insertion is separate from foreground execution.
 *
 * @category models
 */
export interface PromptHandlers<R = never> {
  /**
   * Records the user message and returns its canonical id. On v2 its successful insertion
   * produces the `session/prompt` acknowledgement even if retention or foreground work later
   * fails. Request cancellation after insertion does not replace this acknowledgement.
   */
  readonly insert: (request: InsertRequest) => Effect.Effect<{ readonly messageId: string }, HandlerError, R>
  /**
   * Runs the foreground turn. On v2 this runs after the response, in a scope owned by the session;
   * on v1 the response waits for its stop reason.
   */
  readonly execute: (request: ExecuteRequest) => Effect.Effect<StopReason, HandlerError, R>
}

/**
 * Authentication handlers. Supplying `methods` obliges you to supply both `login` and `logout` :
 * advertising methods you cannot service is rejected.
 *
 * @category models
 */
export interface AuthHandlers<R = never> {
  /**
   * Authentication methods advertised during initialize. Terminal methods are included only
   * when the client advertises terminal authentication support.
   */
  readonly methods: ReadonlyArray<{
    readonly methodId: string
    readonly name: string
    readonly type?: string | undefined
    readonly description?: string | undefined
  }>
  /**
   * Authenticates using the advertised method identity supplied by the client.
   */
  readonly login?: (request: { readonly methodId: string }) => Effect.Effect<void, HandlerError, R>
  /**
   * Ends authentication for the current client connection.
   */
  readonly logout?: () => Effect.Effect<void, HandlerError, R>
}

/**
 * Agent identity, version policy, and handlers used to derive the capability advertisement.
 *
 * @category configuration
 */
export interface Options<R = never> {
  /**
   * Advertised implementation identity.
   */
  readonly info: { readonly name: string; readonly version: string; readonly title?: string | undefined }
  /**
   * Enabled versions, highest first. Defaults to `[1]`; v2 is draft.
   */
  readonly versions?: readonly [1] | readonly [2] | readonly [2, 1] | undefined
  /**
   * Session lifecycle implementation used to derive advertised support.
   */
  readonly session: SessionHandlers<R>
  /**
   * Separate insertion and foreground execution phases.
   */
  readonly prompt: PromptHandlers<R>
  /**
   * Optional authentication methods and their required handlers.
   */
  readonly auth?: AuthHandlers<R> | undefined
  /**
   * `session/list` support. Omitted means the method is not advertised; the store still backs
   * session existence checks.
   */
  readonly list?: boolean | undefined
}

// -----------------------------------------------------------------------------
// Capability validation
// -----------------------------------------------------------------------------

const validate = <R>(options: Options<R>): AcpAgentConfigError | undefined => {
  // v2's session capability has a baseline: advertising it obliges the agent to
  // serve session/new, session/prompt, session/cancel and session/update.
  if (typeof options.session?.create !== "function") {
    return new AcpAgentConfigError("session.create", "Advertising sessions requires a session.create handler")
  }
  if (typeof options.prompt?.insert !== "function") {
    return new AcpAgentConfigError("prompt.insert", "Advertising sessions requires a prompt.insert handler")
  }
  if (typeof options.prompt?.execute !== "function") {
    return new AcpAgentConfigError("prompt.execute", "Advertising sessions requires a prompt.execute handler")
  }
  if (options.auth !== undefined && options.auth.methods.length > 0) {
    if (typeof options.auth.login !== "function") {
      return new AcpAgentConfigError("auth.login", "Advertising authentication methods requires an auth.login handler")
    }
    if (typeof options.auth.logout !== "function") {
      return new AcpAgentConfigError("auth.logout", "Advertising authentication methods requires an auth.logout handler")
    }
  }
  return undefined
}

// -----------------------------------------------------------------------------
// Agent
// -----------------------------------------------------------------------------

/**
 * Validated agent configuration and an effect that serves one transport connection.
 *
 * @category models
 */
export interface AcpAgent<R = never> {
  /**
   * The validated options this agent serves.
   */
  readonly options: Options<R>
  /**
   * Serves one client using the injected `AcpTransport` until it disconnects. Owned session work
   * and pending interactions are settled before it returns.
   */
  readonly serve: Effect.Effect<void, AcpTransportError, R | Store | AcpTransport | Scope.Scope>
}

/**
 * The capability set this agent advertises for `version`.
 *
 * Only surfaces with an installed handler are advertised, so the wire never
 * promises more than construction validated.
 */
const advertisement = <R>(options: Options<R>, version: Version, terminalAuth: boolean): V1.InitializeResponse | V2.InitializeResponse => {
  const methods = (options.auth?.methods ?? []).filter((method) => method.type !== "terminal" || terminalAuth)
  if (version === 1) {
    return {
      protocolVersion: 1,
      agentInfo: { name: options.info.name, version: options.info.version, title: options.info.title ?? null },
      agentCapabilities: {
        loadSession: false,
        sessionCapabilities: {
          ...(options.list ? { list: {} } : {}),
          ...(options.session.resume ? { resume: {} } : {}),
          ...(options.session.delete ? { delete: {} } : {}),
          ...(options.session.close ? { close: {} } : {})
        }
      },
      // v1 keys an auth method by `id`; v2 by `methodId`.
      authMethods: methods.map((method) => ({
        type: method.type ?? "agent",
        id: method.methodId,
        name: method.name,
        description: method.description ?? null
      }))
    }
  }
  return {
    protocolVersion: 2,
    info: { name: options.info.name, version: options.info.version, title: options.info.title ?? null },
    capabilities: {
      session: (options.session.delete ? { delete: {} } : {})
    },
    authMethods: methods.map((method) => ({
      type: method.type ?? "agent",
      methodId: method.methodId,
      name: method.name,
      description: method.description ?? null
    }))
  }
}

/** Maps a handler failure onto the wire without leaking internal causes. */
const toRemote = (error: unknown): AcpRemoteError => {
  if (Schema.is(AcpRemoteError)(error)) return error
  if (error instanceof AcpAgentError) {
    return new AcpRemoteError({ code: error.code, message: error.message, data: error.data })
  }
  if (error instanceof StoreError) {
    return error.kind === "SessionError"
      ? new AcpRemoteError({ code: ErrorCode.ResourceNotFound, message: error.message })
      : new AcpRemoteError({ code: ErrorCode.InternalError, message: "Internal error" })
  }
  return new AcpRemoteError({ code: ErrorCode.InternalError, message: "Internal error" })
}

interface SessionState {
  /** Scope owning this session's foreground execution. */
  readonly scope: Scope.Closeable
  inserting?: boolean
  closing?: boolean
  cancellation?: Deferred.Deferred<void, HandlerError> | undefined
  running: Fiber.Fiber<StopReason, HandlerError> | undefined
  cancelled: boolean
  cancelling: boolean
}

/**
 * Creates an agent after validating that its advertised capabilities have matching handlers.
 *
 * **Details**
 *
 * Construction validates configuration but does not open a transport or serve a client. Handler
 * dependencies are supplied when serving; configuration failures use the typed error channel.
 *
 * **Example** (Separating insertion from execution)
 *
 * The insertion phase assigns the user message identity. Foreground execution emits a separate
 * agent message. These counters are suitable for a process-local demonstration; persistent
 * implementations should allocate durable identities.
 *
 * ```ts
 * import * as AcpAgent from "effect-acp/AcpAgent"
 * import * as Effect from "effect/Effect"
 *
 * let sessionNumber = 0
 * let messageNumber = 0
 * const agent = AcpAgent.make({
 *   info: { name: "minimal", version: "1.0.0" },
 *   versions: [2, 1],
 *   session: {
 *     create: () => Effect.sync(() => ({ sessionId: `session-${++sessionNumber}` }))
 *   },
 *   prompt: {
 *     insert: () => Effect.sync(() => ({ messageId: `user-${++messageNumber}` })),
 *     execute: ({ emit, messageId }) => Effect.as(
 *       emit.agentChunk(`${messageId}:reply`, { type: "text", text: "hi" }),
 *       "end_turn"
 *     )
 *   }
 * })
 * void agent
 * ```
 *
 * @see {@link makeUnsafe} for synchronous construction when configuration is already trusted.
 * @category constructors
 */
export const make = <R = never>(options: Options<R>): Effect.Effect<AcpAgent<R>, AcpAgentConfigError> =>
  Effect.suspend(() => {
    const invalid = validate(options)
    return invalid ? Effect.fail(invalid) : Effect.sync(() => build(options))
  })

/**
 * Creates an agent synchronously from validated handler configuration.
 *
 * **When to use**
 *
 * Use when configuration is already trusted and construction must return an agent directly.
 *
 * **Gotchas**
 *
 * Invalid configuration throws `AcpAgentConfigError` synchronously. Construction defects also
 * remain exceptions.
 *
 * @see {@link make} for construction with typed configuration failures.
 * @category unsafe
 */
export const makeUnsafe = <R = never>(options: Options<R>): AcpAgent<R> => {
  const invalid = validate(options)
  if (invalid) throw invalid
  return build(options)
}

const build = <R>(options: Options<R>): AcpAgent<R> => {
  const enabled: ReadonlyArray<Version> = options.versions ?? [1]

  const serve = Effect.gen(function*() {
      const serveScope = yield* Scope.Scope
      const store = yield* Effect.service(Store)
      // Routes must be `Effect<_, _, never>`, so capture the author's services
      // once here and provide them to every handler effect.
      const services = yield* Effect.context<R>()
      const provided = <A, E>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E> =>
        Effect.provideContext(effect, services)
      const connection = yield* AcpConnection.make()
      const sessions = new Map<string, SessionState>()
      let peer: Peer | undefined

      const ended = (sessionId: string) =>
        Effect.suspend(() => {
          const state = sessions.get(sessionId)
          if (!state) return Effect.void
          state.cancelled = true
          sessions.delete(sessionId)
          return Scope.close(state.scope, Exit.void).pipe(Effect.ignore)
        })

      // --- updates -----------------------------------------------------------

      const notify = (sessionId: string, update: unknown) =>
        Schema.encodeUnknownEffect(AcpProtocol.schemas[peer!.version].clientMethods["session/update"].params)({ sessionId, update }).pipe(
          Effect.flatMap((encoded) => connection.notifyRaw("session/update", encoded)),
          Effect.mapError(() => new AcpAgentError({ message: "Invalid update or closed connection" })))

      const emitFor = (sessionId: string, version: Version): Emit => {
        const chunk = (kind: "agent" | "thought" | "user") => (messageId: string, content: ContentBlock) => {
          const sessionUpdate = { agent: "agent_message_chunk", thought: "agent_thought_chunk", user: "user_message_chunk" }[kind]
          const payload = version === 2
            ? { sessionUpdate, messageId, content }
            : { sessionUpdate, content }
          return Effect.gen(function*() {
            // Retention is the store's guarantee: it keeps the first instant
            // when a later chunk or replacement updates this message.
            yield* store.retain({
              sessionId,
              messageId,
              role: kind === "thought" ? "thought" : kind,
              replacement: null,
              chunks: [content],
              recordedAt: yield* DateTime.now
            }).pipe(Effect.ignore)
            yield* notify(sessionId, payload)
          })
        }
        return {
          version,
          agentChunk: chunk("agent"),
          thoughtChunk: chunk("thought"),
          userChunk: chunk("user"),
          message: (role, messageId, content) =>
            version === 1
              ? Effect.fail(
                new AcpAgentError({ message: "Full message replacement requires protocol v2" })
              )
              : Effect.gen(function*() {
                yield* store.retain({
                  sessionId,
                  messageId,
                  role,
                  replacement: content,
                  chunks: [],
                  recordedAt: yield* DateTime.now
                }).pipe(Effect.ignore)
                yield* notify(sessionId, {
                  sessionUpdate: messageRole(role),
                  messageId,
                  content
                })
              }),
          raw: (update) => notify(sessionId, update)
        }
      }

      /** v2 reports turn completion as an idle state update; v1 in its response. */
      const emitIdle = (sessionId: string, version: Version, stopReason: StopReason, error?: AcpRemoteError) =>
        version === 2 ? notify(sessionId, { sessionUpdate: "state_update", state: "idle", stopReason,
          ...(error ? { error: { code: error.code, message: error.message, ...(error.data === undefined ? {} : { data: error.data }) } } : {})
        }) : Effect.void

      const emitRunning = (sessionId: string, version: Version) =>
        version === 2 ? notify(sessionId, { sessionUpdate: "state_update", state: "running" }) : Effect.void

      // --- client interactions ----------------------------------------------

      const requestClient = <P, A>(descriptor: RequestMethod<string, P, A>, params: P) => Effect.gen(function*() {
        const encoded = yield* Schema.encodeEffect(descriptor.params)(params)
        const sent = yield* connection.send(descriptor.method, encoded)
        return yield* sent.response.pipe(Effect.flatMap(Schema.decodeUnknownEffect(descriptor.result)),
          Effect.onInterrupt(() => Effect.ignore(connection.cancelRequest(sent.id))))
      })
      const interactionsFor = (sessionId: string, version: Version, current: Peer): Interactions => ({
        requestPermission: ({ options: permissionOptions, title, toolCallId }) =>
          (version === 2
            ? requestClient(V2.clientMethods["session/request_permission"], { sessionId, title, options: permissionOptions })
            : requestClient(V1.clientMethods["session/request_permission"], {
              sessionId, toolCall: { toolCallId: toolCallId ?? "tool-1", title }, options: permissionOptions
            })).pipe(
            Effect.map((raw) => {
              const outcome = raw.outcome
              return outcome.outcome === "selected" && "optionId" in outcome ? outcome.optionId : null
            }),
            Effect.mapError((error) => new AcpAgentError({ message: `Permission request failed: ${error.message}` }))
          ),
        elicit: (request) =>
          current.elicitation.some((mode) => mode === request.mode)
            // Reject before sending: the client told us it cannot answer this mode.
            ? requestClient(AcpProtocol.schemas[version].clientMethods["elicitation/create"], { ...request, sessionId }).pipe(
              Effect.mapError((error) => new AcpAgentError({ message: `Elicitation failed: ${error.message}` }))
            )
            : Effect.fail(
              new AcpAgentError({
                code: ErrorCode.InvalidRequest,
                message: `Client does not support ${request.mode} elicitation`
              })
            )
      })

      // --- request handling --------------------------------------------------

      const logPrivateError = (cause: Cause.Cause<unknown>) => {
        const reason = cause.reasons.length === 1 ? cause.reasons[0] : undefined
        return reason && Cause.isFailReason(reason) && reason.error instanceof StoreError && reason.error.kind !== "SessionError"
          ? Effect.logError("Agent store operation failed", cause)
          : Effect.void
      }

      const requirePeer = Effect.suspend(() =>
        peer === undefined
          ? Effect.fail(new AcpRemoteError({ code: ErrorCode.InvalidRequest, message: "Not initialized" }))
          : Effect.succeed(peer)
      )

      const requireSession = (sessionId: string) =>
        store.get(sessionId).pipe(
          Effect.tapCause(logPrivateError),
          Effect.mapError(toRemote),
          Effect.filterOrFail(
            (session) => session !== undefined,
            () => toRemote(unknownSession(sessionId))
          )
        )

      /**
       * Runs an author handler. Typed failures keep their code and message;
       * defects are logged locally and reported as a bare Internal error, so a
       * handler that throws never leaks its cause to the client.
       */
      const handler = <A>(effect: Effect.Effect<A, HandlerError, R>) =>
        effect.pipe(
          Effect.tapCause(logPrivateError),
          Effect.mapError(toRemote),
          Effect.catchCauseIf(
            (cause) => cause.reasons.some(Cause.isDieReason) && !Cause.hasInterrupts(cause),
            (cause) => Effect.andThen(Effect.logError("Agent handler failed", cause),
              Effect.fail(new AcpRemoteError({ code: ErrorCode.InternalError, message: "Internal error" })))
          ),
          provided
        )

      const initialize = (params: unknown) => Effect.gen(function*() {
        if (peer !== undefined) return yield* new AcpRemoteError({ code: ErrorCode.InvalidRequest, message: "Already initialized" })
        const raw = yield* decodeInput(PeerInput, params)
        const wire = raw.protocolVersion === 1
          ? yield* decodeInput(V1.InitializeRequest, params)
          : yield* decodeInput(V2.InitializeRequest, params)
        const highest: Version = enabled.includes(2) ? 2 : 1
        const version = enabled.find((candidate) => candidate === raw.protocolVersion) ?? highest
        const capabilities = raw.capabilities ?? raw.clientCapabilities
        const elicitation = capabilities?.elicitation
        peer = {
          version, info: raw.info ?? raw.clientInfo ?? null,
          elicitation: elicitation ? (["form", "url"] as const).filter((mode) => elicitation[mode] != null) : [],
          raw: wire
        }
        const terminalAuth = raw.protocolVersion === 1
          ? (wire as V1.InitializeRequest).clientCapabilities?.auth?.terminal === true
          : (wire as V2.InitializeRequest).capabilities?.auth?.terminal != null
        return advertisement(options, version, terminalAuth)
      })

      const newSession = (params: unknown) =>
        Effect.gen(function*() {
          const current = yield* requirePeer
          const request = yield* decodeInput(CreateInput, params)
          const created = yield* handler(options.session.create({
            cwd: request.cwd,
            additionalDirectories: request.additionalDirectories ?? [],
            mcpServers: request.mcpServers ?? [],
            peer: current
          }))
          yield* store.create({
            sessionId: created.sessionId,
            cwd: request.cwd,
            additionalDirectories: request.additionalDirectories ?? null
          }).pipe(Effect.tapCause(logPrivateError), Effect.mapError(toRemote))
          return { sessionId: created.sessionId }
        })

      /** Owns one turn's foreground work; used by both versions. */
      const execute = (context: SessionContext, prompt: Prompt, messageId: string, before: Effect.Effect<void, HandlerError> = Effect.void) =>
        Effect.gen(function*() {
          const emit = emitFor(context.sessionId, context.version)
          const client = interactionsFor(context.sessionId, context.version, context.peer)
          yield* emitRunning(context.sessionId, context.version)
          // V2 distinguishes execution failure from deliberate refusal. V1
          // keeps its existing refusal completion because it has no error stop reason.
          let failure: AcpRemoteError | undefined
          const stopReason = yield* before.pipe(
            Effect.andThen(Effect.suspend(() => options.prompt.execute({ ...context, prompt, messageId, emit, client }))),
            Effect.catchCause((cause) => {
              if (Cause.hasInterruptsOnly(cause)) return Effect.failCause(cause)
              if (Cause.hasInterrupts(cause)) {
                // V1's request boundary logs this failure; V2 has already replied.
                return context.version === 1 ? Effect.failCause(cause) : Effect.andThen(Effect.logError("Agent execution failed", cause), Effect.failCause(cause))
              }
              const reason = cause.reasons.length === 1 ? cause.reasons[0] : undefined
              failure = toRemote(reason && Cause.isFailReason(reason) ? reason.error : undefined)
              return Effect.andThen(Effect.logError("Agent execution failed", cause), Effect.succeed<StopReason>(context.version === 2 ? "error" : "refusal"))
            })
          )
          // Final updates precede the idle signal.
          const compatibleReason = context.version === 1 && stopReason === "error" ? "refusal" : stopReason
          yield* emitIdle(context.sessionId, context.version, compatibleReason, failure)
          return compatibleReason
        })

      const prompt = (params: unknown, requestContext: AcpConnection.RequestContext) =>
        Effect.gen(function*() {
          const current = yield* requirePeer
          const request = yield* decodeInput(PromptInput, params)
          yield* requireSession(request.sessionId)
          const context: SessionContext = {
            sessionId: request.sessionId,
            version: current.version,
            peer: current
          }
          const state: SessionState = sessions.get(request.sessionId) ?? { scope: yield* Scope.fork(serveScope), running: undefined, cancelled: false, cancelling: false }
          sessions.set(request.sessionId, state)
          if (state.running || state.inserting || state.cancelling || state.closing) return yield* new AcpRemoteError({ code: ErrorCode.InvalidRequest, message: "Session is busy" })
          state.inserting = true
          state.cancelled = false
          const insert = handler(Effect.suspend(() => options.prompt.insert({ ...context, prompt: request.prompt })))
          const retain = (messageId: string) => Effect.gen(function*() {
            yield* store.retain({
              sessionId: request.sessionId, messageId, role: "user",
              replacement: [...request.prompt], chunks: [], recordedAt: yield* DateTime.now
            }).pipe(Effect.tapCause(logPrivateError))
          })
          const start = (messageId: string, before: Effect.Effect<void, HandlerError>) => Effect.gen(function*() {
            const started = yield* Deferred.make<void>()
            const completed = yield* Deferred.make<StopReason, HandlerError>()
            const work = Deferred.await(started).pipe(Effect.andThen(execute(context, request.prompt, messageId, before)),
              Effect.catchCauseIf((cause) => state.cancelled && Cause.hasInterruptsOnly(cause),
                () => Effect.succeed<StopReason>("cancelled")),
              Effect.ensuring(Effect.sync(() => { state.running = undefined })),
              Effect.onExit((exit) => state.cancelled && Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)
                ? Deferred.succeed(completed, "cancelled")
                : Deferred.done(completed, exit)))
            const fiber = yield* Effect.forkIn(Effect.interruptible(work), state.scope)
            state.running = fiber
            state.inserting = false
            yield* Deferred.succeed(started, undefined)
            return { fiber, completed }
          })
          return yield* Effect.gen(function*() {
            if (current.version === 2) {
              // Successful author insertion is the acknowledgement boundary.
              // Retention and foreground work belong to the session, so their
              // failures or cancellation cannot replace its committed result.
              return yield* Effect.uninterruptibleMask((restore) => Effect.gen(function*() {
                const acceptance = insert.pipe(
                  Effect.map(({ messageId }) => ({ messageId })),
                  Effect.tap((result) => Schema.encodeEffect(V2.PromptResponse)(result).pipe(
                    Effect.mapError(() => new AcpRemoteError({ code: ErrorCode.InternalError, message: "Invalid handler result" }))))
                )
                const inserted = yield* requestContext.commitResult(restore(acceptance))
                if (!state.cancelled && sessions.get(request.sessionId) === state) {
                  yield* start(inserted.messageId, retain(inserted.messageId))
                }
                return inserted
              }))
            }
            const inserted = yield* insert
            const { fiber, completed } = yield* Effect.uninterruptible(Effect.gen(function*() {
              yield* retain(inserted.messageId).pipe(Effect.mapError(toRemote))
              if (state.cancelled) return yield* new AcpRemoteError({ code: ErrorCode.RequestCancelled, message: "Request cancelled" })
              return yield* start(inserted.messageId, Effect.void)
            }))
            return { stopReason: yield* Deferred.await(completed).pipe(Effect.onInterrupt(() => Fiber.interrupt(fiber))) }
          }).pipe(Effect.ensuring(Effect.sync(() => { state.inserting = false })))
        })

      const cancelSession = (params: unknown) =>
        Effect.gen(function*() {
          const { sessionId } = yield* decodeInput(SessionInput, params)
          const state = sessions.get(sessionId)
          const current = peer
          if (state?.cancellation) return yield* Deferred.await(state.cancellation)
          const cancellation = yield* Deferred.make<void, HandlerError>()
          if (state) {
            state.cancellation = cancellation
            state.cancelled = true
            state.cancelling = true
          }
          yield* Effect.gen(function*() {
            const running = state?.running
            let failure: AcpRemoteError | undefined
            if (running) {
              // Interrupting drains the execution's finalizers (its final
              // updates) before we report the cancelled stop reason.
              yield* Fiber.interrupt(running)
              const exit = yield* Fiber.await(running)
              if (Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause)) failure = toRemote(undefined)
              if (state.running === running) state.running = undefined
            }
            if (options.session.cancel && current) {
              yield* options.session.cancel({ sessionId, version: current.version, peer: current }).pipe(Effect.ignore)
            }
            if (current && (!state || sessions.get(sessionId) === state)) yield* emitIdle(sessionId, current.version, failure ? "error" : "cancelled", failure)
          }).pipe(
            Effect.onExit((exit) => Deferred.done(cancellation, exit)),
            Effect.ensuring(Effect.sync(() => { if (state) { state.cancelling = false; state.cancellation = undefined } }))
          )
        })

      const routes: Array<AcpConnection.Route> = [
        { _tag: "Request", method: "initialize", run: (params) => initialize(params) },
        { _tag: "Request", method: "session/new", run: (params) => newSession(params) },
        { _tag: "Request", method: "session/prompt", run: (params, context) => provided(prompt(params, context)).pipe(Effect.tapCause(logPrivateError), Effect.mapError(toRemote)) },
        {
          _tag: "Notification",
          method: "session/cancel",
          run: (params) => provided(cancelSession(params)).pipe(Effect.ignore)
        }
      ]

      if (options.list || enabled.includes(2)) {
        routes.push({
          _tag: "Request",
          method: "session/list",
          run: (params) =>
            decodeInput(ListInput, params ?? {}).pipe(
              Effect.flatMap(({ cwd }) => store.list(cwd)),
              Effect.map((sessions) => ({
                sessions: sessions.map((session) => ({
                  sessionId: session.sessionId,
                  cwd: session.cwd,
                  title: session.title ?? null,
                  updatedAt: session.updatedAt ?? null
                }))
              })),
              Effect.tapCause(logPrivateError), Effect.mapError(toRemote)
            )
        })
      }

      if (options.session.resume || enabled.includes(2)) {
        const resume = options.session.resume ?? (() => Effect.void)
        routes.push({
          _tag: "Request",
          method: "session/resume",
          run: (params) =>
            Effect.gen(function*() {
              const current = yield* requirePeer
              if (current.version === 1 && !options.session.resume) return yield* new AcpRemoteError({ code: ErrorCode.MethodNotFound, message: "Method not found" })
              const request = yield* decodeInput(ResumeInput, params)
              yield* requireSession(request.sessionId)
              const context: SessionContext = {
                sessionId: request.sessionId,
                version: current.version,
                peer: current
              }
              const replayFromStart = request.replayFrom?.type === "start"
              yield* handler(resume({ ...context, cwd: request.cwd, replayFromStart }))
              if (replayFromStart) yield* replay(context)
              return {}
            })
        })
      }

      if (options.session.delete) {
        const remove = options.session.delete
        routes.push({
          _tag: "Request",
          method: "session/delete",
          run: (params) =>
            Effect.gen(function*() {
              const current = yield* requirePeer
              const { sessionId } = yield* decodeInput(SessionInput, params)
              yield* requireSession(sessionId)
              yield* handler(remove({ sessionId, version: current.version, peer: current }))
              yield* ended(sessionId)
              yield* store.remove(sessionId).pipe(Effect.tapCause(logPrivateError), Effect.mapError(toRemote))
              return {}
            })
        })
      }

      if (options.session.close || enabled.includes(2)) {
        const close = options.session.close ?? (() => Effect.void)
        routes.push({
          _tag: "Request",
          method: "session/close",
          run: (params) =>
            Effect.gen(function*() {
              const current = yield* requirePeer
              const { sessionId } = yield* decodeInput(SessionInput, params)
              yield* requireSession(sessionId)
              const state = sessions.get(sessionId)
              if (state) state.closing = true
              yield* Effect.gen(function*() {
                if (current.version === 2 && (state?.running || state?.inserting || state?.cancelling)) {
                  yield* provided(cancelSession({ sessionId })).pipe(Effect.uninterruptible, Effect.mapError(toRemote))
                }
                yield* handler(close({ sessionId, version: current.version, peer: current }))
                yield* ended(sessionId)
              }).pipe(Effect.ensuring(Effect.sync(() => { if (state) state.closing = false })))
              return {}
            })
        })
      }

      const auth = options.auth
      if (auth && auth.methods.length > 0) {
        const login = auth.login!
        const logout = auth.logout!
        // v1 and v2 disagree on the method names for the same operations.
        routes.push(
          {
            _tag: "Request",
            method: "authenticate",
            run: (params) =>
              decodeInput(LoginInput, params).pipe(Effect.flatMap((request) => handler(login(request))), Effect.as({}))
          },
          {
            _tag: "Request",
            method: "auth/login",
            run: (params) =>
              decodeInput(LoginInput, params).pipe(Effect.flatMap((request) => handler(login(request))), Effect.as({}))
          },
          { _tag: "Request", method: "logout", run: () => Effect.as(handler(logout()), {}) },
          { _tag: "Request", method: "auth/logout", run: () => Effect.as(handler(logout()), {}) }
        )
      }

      /**
       * Replays retained history. A replacement resets whatever the client
       * accumulated for that message before the chunks are appended, so the
       * replayed message keeps its original identity.
       */
      const replay = (context: SessionContext) =>
        Effect.gen(function*() {
          const messages = yield* store.retained(context.sessionId)
          for (const message of messages) {
            const role = messageRole(message.role)
            if (context.version === 2) yield* notify(context.sessionId, {
              sessionUpdate: role, messageId: message.messageId, content: message.replacement ?? []
            })
            const chunks = context.version === 1 ? [...(message.replacement ?? []), ...message.chunks] : message.chunks
            for (const content of chunks) yield* notify(context.sessionId, {
              sessionUpdate: `${role}_chunk`, ...(context.version === 2 ? { messageId: message.messageId } : {}), content
            })
          }
        }).pipe(Effect.tapCause(logPrivateError), Effect.mapError(toRemote))

      const dispatch = AcpConnection.handlers(routes)
      yield* connection.setHandlers({
        request: (method, params, context) => Effect.gen(function*() {
          if (method !== "initialize") yield* requirePeer
          const requested = isRecord(params) && params["protocolVersion"] === 2 ? 2 : 1
          const selected = method === "initialize" ? requested : peer!.version
          if (selected === 1 && method === "session/list" && !options.list) return yield* new AcpRemoteError({ code: ErrorCode.MethodNotFound, message: "Method not found" })
          const methods: Readonly<Record<string, Method>> = AcpProtocol.schemas[selected].agentMethods
          const descriptor = methods[method]
          if (descriptor?._tag !== "Request") return yield* new AcpRemoteError({ code: ErrorCode.MethodNotFound, message: "Method not found" })
          const decoded = yield* Schema.decodeUnknownEffect(descriptor.params)(params).pipe(
            Effect.mapError(() => new AcpRemoteError({ code: ErrorCode.InvalidParams, message: "Invalid params" })))
          const effect = dispatch.request?.(method, decoded, context)
          if (effect === undefined) return yield* new AcpRemoteError({ code: ErrorCode.MethodNotFound, message: "Method not found" })
          const result = yield* effect
          const resultSchema = method === "initialize" ? AcpProtocol.schemas[peer!.version].InitializeResponse : descriptor.result
          return yield* Schema.encodeUnknownEffect(resultSchema)(result).pipe(
            Effect.mapError(() => new AcpRemoteError({ code: ErrorCode.InternalError, message: "Invalid handler result" })))
        }).pipe(Effect.catchDefect(() => Effect.fail(new AcpRemoteError({ code: ErrorCode.InternalError, message: "Internal error" })))),
        notification: (method, params) => Effect.gen(function*() {
          if (!peer) return
          const methods: Readonly<Record<string, Method>> = AcpProtocol.schemas[peer.version].agentMethods
          const descriptor = methods[method]
          if (descriptor?._tag !== "Notification") return
          const decoded = yield* Schema.decodeUnknownEffect(descriptor.params)(params)
          yield* dispatch.notification?.(method, decoded) ?? Effect.void
        }).pipe(Effect.ignoreCause)
      })
      // Serve until the client disconnects, then settle owned work.
      yield* Effect.ensuring(
        connection.closed,
        Effect.suspend(() => Effect.forEach([...sessions.keys()], ended, { discard: true }))
      )
    }).pipe(Effect.scoped)

  return { options, serve }
}

/**
 * Serves this agent over the current process's stdin/stdout until the client disconnects. Stdout
 * carries only ACP frames.
 *
 * @category running
 */
export const serveStdio = <R>(
  agent: AcpAgent<R>,
  options?: ProcessStdio.Options
): Effect.Effect<void, AcpTransportError, R | Store | Stdio.Stdio | Scope.Scope> =>
  agent.serve.pipe(Effect.provide(Layer.merge(ProcessStdio.layer(options), Logger.layer([Logger.withConsoleError(Logger.formatJson)]))))

/**
 * A layer that serves this agent over process stdio for the layer's lifetime. Provide `Store` (e.g.
 * `agent/Store.layer` ) and a platform `Stdio` .
 *
 * @category layers
 */
export const layerStdio = <R>(
  agent: AcpAgent<R>,
  options?: ProcessStdio.Options
): Layer.Layer<never, AcpTransportError, R | Store | Stdio.Stdio> =>
  Layer.effectDiscard(Effect.forkScoped(serveStdio(agent, options)))

/**
 * Version-neutral prompt and content types for agent handlers.
 *
 * @category re-exports
 */
export type { ContentBlock, Prompt } from "./agent/Content.ts"
/**
 * Session store service and persistence failures for agent authors.
 *
 * @category re-exports
 */
export { Store, StoreError } from "./agent/Store.ts"

const messageRole = (role: "agent" | "thought" | "user") => ({ agent: "agent_message", thought: "agent_thought", user: "user_message" })[role]

const isRecord = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value)
const ElicitationCapabilities = Schema.Struct({ elicitation: Schema.optionalKey(Schema.NullOr(V2.ElicitationCapabilities)) })
const PeerInput = Schema.Struct({
  protocolVersion: Schema.Finite,
  info: Schema.optionalKey(Schema.NullOr(AcpProtocol.schemas[2].Implementation)),
  clientInfo: Schema.optionalKey(Schema.NullOr(AcpProtocol.schemas[1].Implementation)),
  capabilities: Schema.optionalKey(Schema.NullOr(ElicitationCapabilities)),
  clientCapabilities: Schema.optionalKey(Schema.NullOr(ElicitationCapabilities))
})
const CreateInput = Schema.Struct({ cwd: Schema.String,
  additionalDirectories: Schema.optionalKey(Schema.NullOr(Schema.Array(Schema.String))),
  mcpServers: Schema.optionalKey(Schema.NullOr(Schema.Array(McpServer))) })
const SessionInput = Schema.Struct({ sessionId: Schema.String })
const PromptInput = Schema.Struct({ sessionId: Schema.String, prompt: Schema.Array(
  Schema.Union([V1.ContentBlock, V2.ContentBlock])) })
const ResumeInput = Schema.Struct({ sessionId: Schema.String, cwd: Schema.String,
  replayFrom: Schema.optionalKey(Schema.NullOr(Schema.Struct({ type: Schema.optionalKey(Schema.String) }))) })
const ListInput = Schema.Struct({ cwd: Schema.optionalKey(Schema.NullOr(Schema.String)) })
const LoginInput = Schema.Struct({ methodId: Schema.String })
const decodeInput = <A>(schema: Schema.Codec<A>, value: unknown) => Schema.decodeUnknownEffect(schema)(value).pipe(
  Effect.mapError(() => new AcpRemoteError({ code: ErrorCode.InvalidParams, message: "Invalid params" })))
