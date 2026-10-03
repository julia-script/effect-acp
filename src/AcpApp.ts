/**
 * Application-level identities, provenance, capabilities, snapshots, submissions, interactions, and
 * their round-trippable schemas.
 *
 * **Details**
 *
 * Everything here is pure data: handlers and commands live on session
 * handles, so snapshots stay serializable for future hosted/gateway use.
 */
import * as Schema from "effect/Schema"
import * as Effect from "effect/Effect"
import { RequestId } from "./AcpSchema.ts"
import * as V1 from "./protocol/v1/Schema.ts"
import * as V2 from "./protocol/v2/Schema.ts"

/**
 * Versioned MCP configurations accepted by session creation.
 *
 * @category schemas
 */
export const McpServer = Schema.Union([V1.McpServer, V2.McpServer])
/**
 * Decoded value of the McpServer schema.
 *
 * @category models
 */
export type McpServer = typeof McpServer.Type
/**
 * Permission or elicitation request payload from either supported ACP version.
 *
 * @category schemas
 */
export const InteractionRequest = Schema.Union([
  V1.RequestPermissionRequest, V2.RequestPermissionRequest,
  V1.CreateElicitationRequest, V2.CreateElicitationRequest
])
/**
 * Permission or elicitation request payload from either supported ACP version.
 *
 * @category models
 */
export type InteractionRequest = typeof InteractionRequest.Type
/**
 * Permission or elicitation response payload from either supported ACP version.
 *
 * @category schemas
 */
export const InteractionOutcome = Schema.Union([
  V1.RequestPermissionResponse, V2.RequestPermissionResponse,
  V1.CreateElicitationResponse, V2.CreateElicitationResponse
])
/**
 * Permission or elicitation response payload from either supported ACP version.
 *
 * @category models
 */
export type InteractionOutcome = typeof InteractionOutcome.Type
/**
 * Form response fields keyed by name and restricted to ACP elicitation value types.
 *
 * @category schemas
 */
export const ElicitationContent = Schema.Record(Schema.String, V2.ElicitationContentValue)
/**
 * Form response fields keyed by name and restricted to ACP elicitation value types.
 *
 * @category models
 */
export type ElicitationContent = typeof ElicitationContent.Type
/**
 * Session listing metadata with nullable working directory, title, and last-activity timestamp.
 *
 * @category schemas
 */
export const SessionListEntry = Schema.Struct({
  sessionId: Schema.String, cwd: Schema.NullOr(Schema.String),
  title: Schema.NullOr(Schema.String), updatedAt: Schema.NullOr(Schema.String)
})
/**
 * Session listing metadata with nullable working directory, title, and last-activity timestamp.
 *
 * @category models
 */
export type SessionListEntry = typeof SessionListEntry.Type

// -----------------------------------------------------------------------------
// Identities
// -----------------------------------------------------------------------------

/**
 * An agent session identifier.
 *
 * @category models
 */
export type SessionId = string
/**
 * Schema for SessionId application values.
 *
 * @category schemas
 */
export const SessionId: Schema.Codec<SessionId> = Schema.String

/**
 * A locally unique submission identifier assigned by this client.
 *
 * @category models
 */
export type SubmissionId = string
/**
 * Schema for SubmissionId application values.
 *
 * @category schemas
 */
export const SubmissionId: Schema.Codec<SubmissionId> = Schema.String

/**
 * A locally unique pending-interaction identifier assigned by this client.
 *
 * @category models
 */
export type InteractionId = string
/**
 * Schema for InteractionId application values.
 *
 * @category schemas
 */
export const InteractionId: Schema.Codec<InteractionId> = Schema.String

/**
 * The protocol version a session speaks.
 *
 * @category models
 */
export type SessionVersion = 1 | 2
/**
 * Schema for SessionVersion application values.
 *
 * @category schemas
 */
export const SessionVersion: Schema.Codec<SessionVersion> = Schema.Union([
  Schema.Literal(1),
  Schema.Literal(2)
])

// -----------------------------------------------------------------------------
// Provenance
// -----------------------------------------------------------------------------

/**
 * Where a message, tool call, or plan identity originated.
 *
 * **Details**
 *
 * `agent` means the identity belongs to the agent (e.g. its `messageId`).
 * `local` means this client synthesized the identity and nothing about it is
 * durable on the agent side; it SHALL NOT be matched against agent content.
 *
 * @category models
 */
export type Provenance = { readonly _tag: "agent"; readonly agentId: string | null } | {
  readonly _tag: "local"
}
/**
 * Schema for Provenance application values.
 *
 * @category schemas
 */
export const Provenance: Schema.Codec<Provenance> = Schema.Union([
  Schema.TaggedStruct("agent", {
    agentId: Schema.Union([Schema.String, Schema.Null])
  }),
  Schema.TaggedStruct("local", { })
])

// -----------------------------------------------------------------------------
// Capabilities
// -----------------------------------------------------------------------------

/** Normalized capability surface, plus the raw negotiated data. */
type CapabilityFields = {
  /** The protocol version string actually negotiated. */
  readonly protocolVersion: string
  /** Agent identity as reported by the agent, when provided. */
  readonly agentInfo: V2.Implementation | null
  /** Authentication-related capabilities. */
  readonly auth: AuthCapabilities
  /** Session lifecycle and prompt capabilities. */
  readonly session: SessionCapabilities
  /** MCP capabilities. */
  readonly mcp: McpCapabilities
  /** Whether v1 filesystem request handlers are installed. */
  readonly filesystem: boolean
  /** Whether v1 terminal request handlers are installed. */
  readonly terminal: boolean
  /** Whether elicitation requests are supported. */
  readonly elicitation: boolean
}
/**
 * Normalized operation support paired with the version-specific initialize response.
 *
 * **When to use**
 *
 * Use to decide which session and authentication controls to present while retaining the raw
 * capability advertisement.
 *
 * @category models
 */
export type Capabilities = CapabilityFields & (
  | { readonly version: 1; readonly negotiated: V1.InitializeResponse }
  | { readonly version: 2; readonly negotiated: V2.InitializeResponse }
)

/**
 * Normalized authentication support and the identifiers of usable advertised methods.
 *
 * @category models
 */
export type AuthCapabilities = {
  readonly authenticate: boolean
  readonly logout: boolean
  /**
   * Advertised authentication method ids.
   */
  readonly methods: ReadonlyArray<string>
}

/**
 * Normalized session capabilities. `loadSession`/`setMode` are v1-only surfaces.
 *
 * @category models
 */
export type SessionCapabilities = {
  readonly list: boolean
  readonly delete: boolean
  readonly resume: boolean
  readonly close: boolean
  readonly additionalDirectories: boolean
  readonly prompt: boolean
  readonly setConfigOption: boolean
  readonly loadSession: boolean
  readonly setMode: boolean
}

/**
 * Normalized MCP support and server connection types accepted by the peer.
 *
 * @category models
 */
export type McpCapabilities = {
  readonly supported: boolean
  /**
   * Server connection types the agent accepts for the negotiated version.
   */
  readonly serverTypes: ReadonlyArray<string>
}

const CapabilityFields = Schema.Struct({
  protocolVersion: Schema.String, agentInfo: Schema.NullOr(V2.Implementation),
  auth: Schema.Struct({ authenticate: Schema.Boolean, logout: Schema.Boolean, methods: Schema.Array(Schema.String) }),
  session: Schema.Struct({ list: Schema.Boolean, delete: Schema.Boolean, resume: Schema.Boolean, close: Schema.Boolean,
    additionalDirectories: Schema.Boolean, prompt: Schema.Boolean, setConfigOption: Schema.Boolean, loadSession: Schema.Boolean, setMode: Schema.Boolean }),
  mcp: Schema.Struct({ supported: Schema.Boolean, serverTypes: Schema.Array(Schema.String) }),
  filesystem: Schema.Boolean, terminal: Schema.Boolean, elicitation: Schema.Boolean
})
/**
 * Normalized operation support paired with the version-specific initialize response.
 *
 * **When to use**
 *
 * Use to decide which session and authentication controls to present while retaining the raw
 * capability advertisement.
 *
 * @category schemas
 */
export const Capabilities: Schema.Codec<Capabilities> = Schema.Union([
  Schema.Struct({ ...CapabilityFields.fields, version: Schema.Literal(1), negotiated: V1.InitializeResponse }),
  Schema.Struct({ ...CapabilityFields.fields, version: Schema.Literal(2), negotiated: V2.InitializeResponse })
])

// -----------------------------------------------------------------------------
// Foreground
// -----------------------------------------------------------------------------

/**
 * Provenance of a foreground state inference.
 *
 * @category models
 */
export type ForegroundProvenance = "agent-reported" | "inferred"
/**
 * Schema for ForegroundProvenance application values.
 *
 * @category schemas
 */
export const ForegroundProvenance: Schema.Codec<ForegroundProvenance> = Schema.Union([
  Schema.Literal("agent-reported"),
  Schema.Literal("inferred")
])

/**
 * Session-scoped foreground work state.
 *
 * **Details**
 *
 * `unknown` means no evidence exists yet. `inferred` states are locally
 * derived (e.g. while a v1 prompt is outstanding) and are distinct from
 * agent-reported states.
 *
 * @category models
 */
export type Foreground =
  | { readonly state: "unknown" }
  | { readonly state: "idle"; readonly stopReason: string | null }
  | { readonly state: "running"; readonly provenance: ForegroundProvenance }
  | { readonly state: "requires_action"; readonly provenance: ForegroundProvenance }
  | { readonly state: string; readonly provenance: ForegroundProvenance }
/**
 * Schema for Foreground application values.
 *
 * @category schemas
 */
export const Foreground: Schema.Codec<Foreground> = Schema.Union([
  Schema.Struct({ state: Schema.Literal("unknown") }),
  Schema.Struct({
    state: Schema.Literal("idle"),
    stopReason: Schema.Union([Schema.String, Schema.Null])
  }),
  Schema.Struct({
    state: Schema.Literal("running"),
    provenance: ForegroundProvenance
  }),
  Schema.Struct({
    state: Schema.Literal("requires_action"),
    provenance: ForegroundProvenance
  }),
  Schema.Struct({
    state: Schema.String,
    provenance: ForegroundProvenance
  })
])

// -----------------------------------------------------------------------------
// Submissions
// -----------------------------------------------------------------------------

/**
 * How a foreground submission failure is recorded in app state.
 *
 * @category models
 */
export type SubmissionFailure =
  | { readonly _tag: "remote"; readonly code: number; readonly message: string; readonly data: unknown }
  | { readonly _tag: "protocol"; readonly message: string }
  | { readonly _tag: "closed"; readonly message: string }
  | { readonly _tag: "timeout" }
  | { readonly _tag: "capacity"; readonly resource: string; readonly limit: number }
/**
 * Schema for SubmissionFailure application values.
 *
 * @category schemas
 */
export const SubmissionFailure: Schema.Codec<SubmissionFailure> = Schema.Union([
  Schema.TaggedStruct("remote", {
    code: Schema.Finite,
    message: Schema.String,
    data: Schema.Unknown
  }),
  Schema.TaggedStruct("protocol", { message: Schema.String }),
  Schema.TaggedStruct("closed", { message: Schema.String }),
  Schema.TaggedStruct("timeout", { }),
  Schema.TaggedStruct("capacity", { resource: Schema.String, limit: Schema.Finite })
])

/**
 * Lifecycle of a single submission record.
 *
 * @category models
 */
export type SubmissionStatus =
  | { readonly _tag: "pending" } // registered, not yet dispatched
  | { readonly _tag: "dispatched" } // sent; awaiting protocol evidence
  | { readonly _tag: "accepted" } // v2 prompt response arrived (insertion ack)
  | { readonly _tag: "completed" } // agreement that foreground work ended
  | { readonly _tag: "failed"; readonly failure: SubmissionFailure }
/**
 * Schema for SubmissionStatus application values.
 *
 * @category schemas
 */
export const SubmissionStatus: Schema.Codec<SubmissionStatus> = Schema.Union([
  Schema.TaggedStruct("pending", { }),
  Schema.TaggedStruct("dispatched", { }),
  Schema.TaggedStruct("accepted", { }),
  Schema.TaggedStruct("completed", { }),
  Schema.TaggedStruct("failed", { failure: SubmissionFailure })
])

/**
 * A local prompt submission. Distinct from the session foreground: a submission is accepted when
 * the agent acknowledges insertion; the session foreground ends independently.
 *
 * @category models
 */
export type SubmissionSnapshot = {
  readonly id: SubmissionId
  /**
   * The prompt blocks submitted.
   */
  readonly prompt: ReadonlyArray<V2.ContentBlock>
  readonly status: SubmissionStatus
  /**
   * The wire request id this submission dispatched under, if any.
   */
  readonly requestId: RequestId
  /**
   * The agent message id reported on acceptance, or null while unavailable. Always null for v1,
   * where acceptance is unavailable.
   */
  readonly agentMessageId: string | null
  /**
   * True on v1, where the protocol exposes no insertion acknowledgement.
   */
  readonly acceptanceUnavailable: boolean
  /**
   * Foreground provenance this submission is (or was) driving.
   */
  readonly foreground: ForegroundProvenance
}
/**
 * Schema for SubmissionSnapshot application values.
 *
 * @category schemas
 */
export const SubmissionSnapshot: Schema.Codec<SubmissionSnapshot> = Schema.Struct({
  id: SubmissionId,
  prompt: Schema.Array(V2.ContentBlock),
  status: SubmissionStatus,
  requestId: RequestId,
  agentMessageId: Schema.Union([Schema.String, Schema.Null]),
  acceptanceUnavailable: Schema.Boolean,
  foreground: ForegroundProvenance
})

// -----------------------------------------------------------------------------
// Messages, tool calls, plans
// -----------------------------------------------------------------------------

/**
 * A message in the transcript. `id` is the local message identity.
 *
 * @category models
 */
export type MessageSnapshot = {
  readonly id: string
  readonly kind: "user" | "agent" | "thought"
  readonly provenance: Provenance
  /**
   * Current content after applying chunk and replacement semantics.
   */
  readonly content: ReadonlyArray<V2.ContentBlock>
  /**
   * The reducer sequence at which this message was last touched.
   */
  readonly seq: number
}
/**
 * Schema for MessageSnapshot application values.
 *
 * @category schemas
 */
export const MessageSnapshot: Schema.Codec<MessageSnapshot> = Schema.Struct({
  id: Schema.String,
  kind: Schema.Literals(["user", "agent", "thought"]),
  provenance: Provenance,
  content: Schema.Array(V2.ContentBlock),
  seq: Schema.Finite
})

/**
 * A tool call as projected from versioned updates.
 *
 * @category models
 */
export type ToolCallSnapshot = {
  readonly toolCallId: string
  readonly provenance: Provenance
  readonly title: string | null
  readonly name: string | null
  readonly kind: V2.ToolKind | null
  readonly status: V2.ToolCallStatus | null
  readonly content: ReadonlyArray<V1.ToolCallContent | V2.ToolCallContent> | null
  readonly locations: ReadonlyArray<V2.ToolCallLocation> | null
  readonly rawInput: unknown
  readonly rawOutput: unknown
  readonly seq: number
}
/**
 * Schema for ToolCallSnapshot application values.
 *
 * @category schemas
 */
export const ToolCallSnapshot: Schema.Codec<ToolCallSnapshot> = Schema.Struct({
  toolCallId: Schema.String,
  provenance: Provenance,
  title: Schema.Union([Schema.String, Schema.Null]),
  name: Schema.Union([Schema.String, Schema.Null]),
  kind: Schema.Union([V2.ToolKind, Schema.Null]),
  status: Schema.Union([V2.ToolCallStatus, Schema.Null]),
  content: Schema.Union([Schema.Array(Schema.Union([V1.ToolCallContent, V2.ToolCallContent])), Schema.Null]),
  locations: Schema.Union([Schema.Array(V2.ToolCallLocation), Schema.Null]),
  rawInput: Schema.Unknown,
  rawOutput: Schema.Unknown,
  seq: Schema.Finite
})

/**
 * A plan as projected. v1 has a single open plan; v2 plans are keyed by id.
 *
 * @category models
 */
export type PlanSnapshot = {
  readonly planId: string
  readonly provenance: Provenance
  /**
   * Current entries, or null when the last update was an unknown variant.
   */
  readonly entries: ReadonlyArray<V2.PlanEntry> | null
  /**
   * The last decoded plan payload verbatim (observable variants included).
   */
  readonly variant: V2.PlanUpdateContent
  readonly seq: number
}
/**
 * Schema for PlanSnapshot application values.
 *
 * @category schemas
 */
export const PlanSnapshot: Schema.Codec<PlanSnapshot> = Schema.Struct({
  planId: Schema.String,
  provenance: Provenance,
  entries: Schema.Union([Schema.Array(V2.PlanEntry), Schema.Null]),
  variant: V2.PlanUpdateContent,
  seq: Schema.Finite
})

// -----------------------------------------------------------------------------
// Terminals, configuration, usage
// -----------------------------------------------------------------------------

/**
 * A display terminal's retained output bytes (bounded, tail-truncated).
 *
 * @category models
 */
export type TerminalSnapshot = {
  readonly terminalId: string
  readonly command: string | null
  readonly cwd: string | null
  readonly outputBytes: ReadonlyArray<number>
  /** True after a concrete exit status, even when its code and signal are unknown. */
  readonly exited?: boolean
  readonly exitCode: number | null
  readonly exitSignal: string | null
  /**
   * True when retained output bytes were truncated to the configured budget.
   */
  readonly outputTruncated: boolean
  readonly seq: number
}
/**
 * Schema for TerminalSnapshot application values.
 *
 * @category schemas
 */
export const TerminalSnapshot: Schema.Codec<TerminalSnapshot> = Schema.Struct({
  terminalId: Schema.String,
  command: Schema.Union([Schema.String, Schema.Null]),
  cwd: Schema.Union([Schema.String, Schema.Null]),
  outputBytes: Schema.Array(Schema.Int),
  exited: Schema.Boolean.pipe(Schema.withDecodingDefaultKey(Effect.succeed(false))),
  exitCode: Schema.Union([Schema.Finite, Schema.Null]),
  exitSignal: Schema.Union([Schema.String, Schema.Null]),
  outputTruncated: Schema.Boolean,
  seq: Schema.Finite
})

/**
 * A configuration option value as last reported by the agent.
 *
 * @category models
 */
export type ConfigOptionSnapshot = {
  /**
   * Normalized key: `id` on v1, `configId` on v2.
   */
  readonly key: string
  /**
   * The decoded wire option object verbatim.
   */
  readonly option: V1.SessionConfigOption | V2.SessionConfigOption | V1.SessionModeState | V1.CurrentModeUpdate
  readonly seq: number
}
/**
 * Schema for ConfigOptionSnapshot application values.
 *
 * @category schemas
 */
export const ConfigOptionSnapshot: Schema.Codec<ConfigOptionSnapshot> = Schema.Struct({
  key: Schema.String,
  option: Schema.Union([V1.SessionConfigOption, V2.SessionConfigOption, V1.SessionModeState, V1.CurrentModeUpdate]),
  seq: Schema.Finite
})

/**
 * The session's context-usage projection.
 *
 * @category models
 */
export type UsageSnapshot = {
  readonly used: number
  readonly size: number
  readonly cost: V2.Cost | null
}
/**
 * Schema for UsageSnapshot application values.
 *
 * @category schemas
 */
export const UsageSnapshot: Schema.Codec<UsageSnapshot> = Schema.Struct({
  used: Schema.Int,
  size: Schema.Int,
  cost: Schema.Union([V2.Cost, Schema.Null])
})

// -----------------------------------------------------------------------------
// Interactions
// -----------------------------------------------------------------------------

/**
 * Lifecycle of a pending user interaction.
 *
 * @category models
 */
export type InteractionStatus = "pending" | "resolved" | "cancelled" | "expired"
/**
 * Schema for InteractionStatus application values.
 *
 * @category schemas
 */
export const InteractionStatus: Schema.Codec<InteractionStatus> = Schema.Union([
  Schema.Literal("pending"),
  Schema.Literal("resolved"),
  Schema.Literal("cancelled"),
  Schema.Literal("expired")
])

/**
 * A schema-backed user interaction (permission or elicitation) pending a single resolution. The
 * handler fiber waits on a deferred keyed by `interactionId` , so waiting never blocks the
 * connection reader.
 *
 * @category models
 */
export type InteractionSnapshot = {
  readonly interactionId: InteractionId
  readonly kind: "permission" | "elicitation"
  readonly version: SessionVersion
  readonly status: InteractionStatus
  /**
   * The decoded wire request object verbatim.
   */
  readonly request: InteractionRequest
  /**
   * The decoded outcome payload once resolved/cancelled (else null).
   */
  readonly outcome: InteractionOutcome | null
  /**
   * Reducer sequence at creation.
   */
  readonly createdAt: number
  /**
   * Reducer sequence at resolution (else null).
   */
  readonly resolvedAt: number | null
}
/**
 * Schema for InteractionSnapshot application values.
 *
 * @category schemas
 */
export const InteractionSnapshot: Schema.Codec<InteractionSnapshot> = Schema.Struct({
  interactionId: InteractionId,
  kind: Schema.Literals(["permission", "elicitation"]),
  version: SessionVersion,
  status: InteractionStatus,
  request: InteractionRequest,
  outcome: Schema.NullOr(InteractionOutcome),
  createdAt: Schema.Finite,
  resolvedAt: Schema.Union([Schema.Finite, Schema.Null])
})

// -----------------------------------------------------------------------------
// Truncation and raw updates
// -----------------------------------------------------------------------------

/**
 * Retention/truncation report for a snapshot.
 *
 * @category models
 */
export type TruncationSnapshot = {
  /**
   * Projection payloads exceeded the byte budget.
   */
  readonly content?: boolean
  /**
   * Oldest transcript messages were evicted.
   */
  readonly history: boolean
  /**
   * Oldest tool calls were evicted.
   */
  readonly toolCalls: boolean
  /**
   * Oldest plans were evicted.
   */
  readonly plans: boolean
  /**
   * Terminal ids whose retained bytes were truncated to the byte budget.
   */
  readonly terminals: ReadonlyArray<string>
  /**
   * Oldest raw update records were evicted.
   */
  readonly raw: boolean
  /**
   * Oldest resolved interactions were evicted.
   */
  readonly interactions: boolean
}
/**
 * Schema for TruncationSnapshot application values.
 *
 * @category schemas
 */
export const TruncationSnapshot: Schema.Codec<TruncationSnapshot> = Schema.Struct({
  content: Schema.optionalKey(Schema.Boolean),
  history: Schema.Boolean,
  toolCalls: Schema.Boolean,
  plans: Schema.Boolean,
  terminals: Schema.Array(Schema.String),
  raw: Schema.Boolean,
  interactions: Schema.Boolean
})

/**
 * A versioned raw update as observed at the wire boundary.
 *
 * @category models
 */
export type RawUpdateRecord = {
  readonly seq: number
  readonly version: SessionVersion
  /**
   * The `sessionUpdate` discriminator, or "undecodable".
   */
  readonly kind: string
  readonly update: unknown
}
/**
 * Schema for RawUpdateRecord application values.
 *
 * @category schemas
 */
export const RawUpdateRecord: Schema.Codec<RawUpdateRecord> = Schema.Struct({
  seq: Schema.Finite,
  version: SessionVersion,
  kind: Schema.String,
  update: Schema.Unknown
})

/**
 * Immutable, schema-backed projection of one session.
 *
 * @category models
 */
export type SessionSnapshot = {
  readonly sessionId: SessionId
  readonly version: SessionVersion
  /**
   * Monotonic reducer sequence; every applied event increments it.
   */
  readonly seq: number
  readonly metadata: {
    readonly title: string | null
    readonly updatedAt: string | null
    readonly cwd: string | null
  }
  readonly foreground: Foreground
  /**
   * The submission driving the current foreground, if any.
   */
  readonly activeSubmissionId: string | null
  readonly submissions: Readonly<Record<string, SubmissionSnapshot>>
  readonly messages: ReadonlyArray<MessageSnapshot>
  readonly toolCalls: Readonly<Record<string, ToolCallSnapshot>>
  readonly plans: Readonly<Record<string, PlanSnapshot>>
  readonly terminals: Readonly<Record<string, TerminalSnapshot>>
  readonly commands: ReadonlyArray<V2.AvailableCommand>
  readonly config: Readonly<Record<string, ConfigOptionSnapshot>>
  readonly usage: UsageSnapshot | null
  readonly interactions: Readonly<Record<string, InteractionSnapshot>>
  readonly raw: ReadonlyArray<RawUpdateRecord>
  readonly truncated: TruncationSnapshot
}

/**
 * Schema for SessionSnapshot application values.
 *
 * @category schemas
 */
export const SessionSnapshot: Schema.Codec<SessionSnapshot> = Schema.Struct({
  sessionId: SessionId,
  version: SessionVersion,
  seq: Schema.Finite,
  metadata: Schema.Struct({
    title: Schema.Union([Schema.String, Schema.Null]),
    updatedAt: Schema.Union([Schema.String, Schema.Null]),
    cwd: Schema.Union([Schema.String, Schema.Null])
  }),
  foreground: Foreground,
  activeSubmissionId: Schema.Union([Schema.String, Schema.Null]),
  submissions: Schema.Record(Schema.String, SubmissionSnapshot),
  messages: Schema.Array(MessageSnapshot),
  toolCalls: Schema.Record(Schema.String, ToolCallSnapshot),
  plans: Schema.Record(Schema.String, PlanSnapshot),
  terminals: Schema.Record(Schema.String, TerminalSnapshot),
  commands: Schema.Array(V2.AvailableCommand),
  config: Schema.Record(Schema.String, ConfigOptionSnapshot),
  usage: Schema.Union([UsageSnapshot, Schema.Null]),
  interactions: Schema.Record(Schema.String, InteractionSnapshot),
  raw: Schema.Array(RawUpdateRecord),
  truncated: TruncationSnapshot
})

/**
 * Bounded content-retention limits with documented finite defaults.
 *
 * @category configuration
 */
export type ContentLimits = {
  /**
   * Serialized content budget, plus required identity/lifecycle metadata. Default 4 MiB; excess
   * content is visibly discarded.
   */
  readonly transcriptBytes?: number
  /**
   * Max retained submission records. Default 128.
   */
  readonly submissions?: number
  /**
   * Max display terminals. Default 32.
   */
  readonly terminals?: number
  /**
   * Max transcript messages retained. Default 200.
   */
  readonly messages: number
  /**
   * Max tool calls retained. Default 64.
   */
  readonly toolCalls: number
  /**
   * Max plans retained. Default 16.
   */
  readonly plans: number
  /**
   * Max bytes retained per terminal output. Default 32768.
   */
  readonly terminalBytes: number
  /**
   * Max raw update records retained. Default 512.
   */
  readonly rawUpdates: number
  /**
   * Max interactions retained (pending + resolved). Default 64.
   */
  readonly interactions: number
}

/**
 * Finite default budgets for retained session content, records, and terminal output.
 *
 * **Details**
 *
 * Overrides can be supplied through client connection options. Snapshot truncation flags report
 * discarded content.
 *
 * @category configuration
 */
export const defaultContentLimits: ContentLimits = {
  transcriptBytes: 4 * 1024 * 1024,
  submissions: 128,
  terminals: 32,
  messages: 200,
  toolCalls: 64,
  plans: 16,
  terminalBytes: 32768,
  rawUpdates: 512,
  interactions: 64
}

/**
 * Bounded provisional-update routing defaults.
 *
 * @category configuration
 */
export type ProvisionalLimits = {
  /**
   * Total buffered update bytes before the lifecycle response. Default 4 MiB.
   */
  readonly bytes?: number
  /**
   * Max updates buffered before a lifecycle response. Default 128.
   */
  readonly updates: number
}

/**
 * Default bounds of 128 provisional updates and 4 MiB before a session lifecycle response.
 *
 * @category configuration
 */
export const defaultProvisionalLimits: ProvisionalLimits = { updates: 128, bytes: 4 * 1024 * 1024 }

/**
 * An observed session stream event.
 *
 * @category models
 */
export type SessionEvent =
  | { readonly _tag: "snapshot"; readonly snapshot: SessionSnapshot }
  | { readonly _tag: "resync" }
