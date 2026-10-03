import { McpServer, ElicitationContent, SessionListEntry } from "./AcpApp.ts"
/**
 * Package-owned application protocol. This is not an ACP transport.
 */
import * as Schema from "effect/Schema"
import * as Rpc from "effect/rpc/Rpc"
import * as RpcGroup from "effect/rpc/RpcGroup"
import * as AcpSessionError from "./AcpSessionError.ts"
import * as V1 from "./protocol/v1/Schema.ts"
import * as V2 from "./protocol/v2/Schema.ts"
import { Capabilities, SessionSnapshot } from "./AcpApp.ts"

/**
 * Version of the library-owned gateway application protocol.
 *
 * @category constants
 */
export const version = 1 as const
/**
 * Gateway failure code and message for authorization, admission, recovery, and transport
 * boundaries.
 *
 * @category errors
 */
export class GatewayError extends Schema.TaggedError<GatewayError>()("AcpGatewayError", {
  code: Schema.Literals(["UnsupportedVersion", "Unauthorized", "HostRestarted", "NotFound", "Conflict", "StaleController", "WindowExpired", "Capacity", "ResyncRequired", "Closed", "Invalid", "AgentFailure", "OutcomeUnknown"]),
  message: Schema.String
}, { identifier: "effect-acp/AcpGateway/GatewayError" }) {}
/**
 * Creates a gateway error whose message is the supplied error code.
 *
 * @category error handling
 */
export const failure = (code: GatewayError["code"]): GatewayError => new GatewayError({ code, message: code })
/**
 * Authenticated principal identity supplied by the server application.
 *
 * @category schemas
 */
export const Identity = Schema.Struct({ principalId: Schema.String })
/**
 * Authenticated principal identity supplied by the server application.
 *
 * @category models
 */
export type Identity = typeof Identity.Type
/**
 * Host-issued admission window bound to an epoch, client, workspace, and expiry instant.
 *
 * @category schemas
 */
export const Window = Schema.Struct({ token: Schema.String, epoch: Schema.String, clientId: Schema.String, workspace: Schema.String, expiresAt: Schema.Finite })
/**
 * Host-issued admission window bound to an epoch, client, workspace, and expiry instant.
 *
 * @category models
 */
export type Window = typeof Window.Type
/**
 * Host epoch, hosted session identity, and event sequence used for attachment recovery.
 *
 * @category schemas
 */
export const Cursor = Schema.Struct({ epoch: Schema.String, session: Schema.String, sequence: Schema.Int })
/**
 * Host epoch, hosted session identity, and event sequence used for attachment recovery.
 *
 * @category models
 */
export type Cursor = typeof Cursor.Type
const Advertised = Schema.Union([
  Schema.Struct({ version: Schema.Literal(1), params: V1.InitializeRequest }),
  Schema.Struct({ version: Schema.Literal(2), params: V2.InitializeRequest })
])
/**
 * Schema for the negotiated ACP version, original advertisement, and initialize response.
 *
 * @category schemas
 */
export const Negotiated = Schema.Union([
  Schema.Struct({ version: Schema.Literal(1), advertised: Advertised, response: V1.InitializeResponse }),
  Schema.Struct({ version: Schema.Literal(2), advertised: Advertised, response: V2.InitializeResponse })
])
/**
 * Schema for a hosted connection identifier, its capabilities, and negotiated protocol data.
 *
 * @category schemas
 */
export const ConnectionDescriptor = Schema.Struct({ connection: Schema.String, capabilities: Capabilities, negotiated: Negotiated })
/**
 * Hosted session identity and ACP session metadata within one host epoch.
 *
 * @category schemas
 */
export const SessionDescriptor = Schema.Struct({ epoch: Schema.String, session: Schema.String, sessionId: Schema.String, version: Schema.Literals([1, 2]) })
/**
 * Hosted session identity and ACP session metadata within one host epoch.
 *
 * @category models
 */
export type SessionDescriptor = typeof SessionDescriptor.Type
/**
 * Schema for a submission acknowledgement with its local and optional agent message identities.
 *
 * @category schemas
 */
export const SubmissionResult = Schema.Struct({ submissionId: Schema.String, agentMessageId: Schema.NullOr(Schema.String), acceptanceUnavailable: Schema.Boolean })
/**
 * Schema for permission and elicitation interaction answers sent through the gateway.
 *
 * @category schemas
 */
export const Resolution = Schema.Union([
  Schema.TaggedStruct("selected", { optionId: Schema.String }), Schema.TaggedStruct("cancelled", {}),
  Schema.TaggedStruct("accept", { content: Schema.optionalKey(ElicitationContent) }), Schema.TaggedStruct("decline", {}), Schema.TaggedStruct("cancel", {})
])
/**
 * Schema for the workspace and MCP configuration of a hosted session.
 *
 * @category configuration
 */
export const SessionOptions = Schema.Struct({ cwd: Schema.String, additionalDirectories: Schema.optionalKey(Schema.Array(Schema.String)), mcpServers: Schema.optionalKey(Schema.Array(McpServer)) })
/**
 * Serializable command for connection, session, interaction, or extension operations.
 *
 * @category schemas
 */
export const Command = Schema.Union([
  Schema.TaggedStruct("Open", { profile: Schema.String, options: Schema.Unknown }),
  Schema.TaggedStruct("NewSession", { connection: Schema.String, options: SessionOptions }),
  Schema.TaggedStruct("ResumeSession", { connection: Schema.String, options: Schema.Struct({ ...SessionOptions.fields, sessionId: Schema.String, replayFrom: Schema.optionalKey(V2.ReplayFrom) }) }),
  Schema.TaggedStruct("Authenticate", { connection: Schema.String, methodId: Schema.String }),
  Schema.TaggedStruct("Logout", { connection: Schema.String }),
  Schema.TaggedStruct("Submit", { session: Schema.String, prompt: Schema.Array(V2.ContentBlock) }),
  Schema.TaggedStruct("Cancel", { session: Schema.String }),
  Schema.TaggedStruct("Close", { session: Schema.String }),
  Schema.TaggedStruct("Delete", { session: Schema.String }),
  Schema.TaggedStruct("Configure", { session: Schema.String, configId: Schema.String, value: Schema.Union([Schema.String, Schema.Boolean]) }),
  Schema.TaggedStruct("Mode", { session: Schema.String, modeId: Schema.String }),
  Schema.TaggedStruct("Resolve", { session: Schema.String, interactionId: Schema.String, resolution: Resolution }),
  Schema.TaggedStruct("Extension", { connection: Schema.String, method: Schema.String, params: Schema.Unknown, controllers: Schema.optionalKey(Schema.Record(Schema.String, Schema.Int)) })
])
/**
 * Serializable command for connection, session, interaction, or extension operations.
 *
 * @category models
 */
export type Command = typeof Command.Type
/**
 * Command admission payload with an operation identity, admission window, and optional controller
 * generation.
 *
 * **Details**
 *
 * Retain the same operation identity and payload when recovering an uncertain admission. A
 * controller generation binds session mutations to the active attachment.
 *
 * @category schemas
 */
export const Admission = Schema.Struct({ window: Window, operationId: Schema.String, generation: Schema.optionalKey(Schema.Int), command: Command })
/**
 * Command admission payload with an operation identity, admission window, and optional controller
 * generation.
 *
 * **Details**
 *
 * Retain the same operation identity and payload when recovering an uncertain admission. A
 * controller generation binds session mutations to the active attachment.
 *
 * @category models
 */
export type Admission = typeof Admission.Type
/**
 * Admission is independent of the later agent result. Results contain data, never handles.
 *
 * @category errors
 */
export const CommandError = Schema.Union([GatewayError, AcpSessionError.AcpCapabilityUnsupported, AcpSessionError.AcpSessionBusy, AcpSessionError.AcpInteractionAlreadyResolved, AcpSessionError.AcpInteractionExpired, AcpSessionError.AcpHistoryUnavailable])
/**
 * Serializable gateway and session failures recorded as command outcomes.
 *
 * @category errors
 */
export type CommandError = typeof CommandError.Type
/**
 * Recorded command state separating admission from success, failure, or an unknown outcome.
 *
 * **Gotchas**
 *
 * An `admitted` operation is still running. `outcomeUnknown` is not confirmation that the agent
 * performed no work.
 *
 * @category schemas
 */
export const Operation = Schema.Struct({ operationId: Schema.String, status: Schema.Literals(["admitted", "succeeded", "failed", "outcomeUnknown"]), result: Schema.Unknown, error: Schema.NullOr(CommandError) })
/**
 * Recorded command state separating admission from success, failure, or an unknown outcome.
 *
 * **Gotchas**
 *
 * An `admitted` operation is still running. `outcomeUnknown` is not confirmation that the agent
 * performed no work.
 *
 * @category models
 */
export type Operation = typeof Operation.Type
/**
 * Session attachment request with optional takeover and a retained event cursor.
 *
 * @category schemas
 */
export const AttachmentRequest = Schema.Struct({ epoch: Schema.String, workspace: Schema.String, session: Schema.String, clientId: Schema.String, takeover: Schema.optionalKey(Schema.Boolean), cursor: Schema.optionalKey(Cursor), expected: Schema.optionalKey(Schema.Struct({ sessionId: Schema.String, version: Schema.Literals([1, 2]) })) })
/**
 * Session attachment request with optional takeover and a retained event cursor.
 *
 * @category models
 */
export type AttachmentRequest = typeof AttachmentRequest.Type
/**
 * Attachment boundary or subsequent session snapshot paired with its recovery cursor.
 *
 * @category schemas
 */
export const Frame = Schema.Union([
  Schema.TaggedStruct("Attached", { cursor: Cursor, generation: Schema.Int, resync: Schema.Boolean, snapshot: Schema.NullOr(SessionSnapshot) }),
  Schema.TaggedStruct("Event", { cursor: Cursor, snapshot: SessionSnapshot })
])
/**
 * Attachment boundary or subsequent session snapshot paired with its recovery cursor.
 *
 * @category models
 */
export type Frame = typeof Frame.Type
/**
 * Effect RPC group for gateway handshake, admission, operation lookup, listing, closure, and
 * attachment streaming.
 *
 * @category protocols
 */
export const Gateway = RpcGroup.make(
  Rpc.make("Hello", { payload: { version: Schema.Int, clientId: Schema.String, workspace: Schema.String }, success: Window, error: GatewayError }),
  Rpc.make("Admit", { payload: Admission, success: Operation, error: GatewayError }),
  Rpc.make("Operation", { payload: { window: Window, operationId: Schema.String }, success: Operation, error: GatewayError }),
  Rpc.make("List", { payload: { epoch: Schema.String, workspace: Schema.String, connection: Schema.String, cwd: Schema.optionalKey(Schema.String) }, success: Schema.Array(SessionListEntry), error: GatewayError }),
  Rpc.make("Closed", { payload: { epoch: Schema.String, workspace: Schema.String, connection: Schema.String }, success: Schema.Void, error: GatewayError }),
  Rpc.make("Attach", { payload: AttachmentRequest, success: Frame, error: GatewayError, stream: true })
)
