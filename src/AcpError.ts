/**
 * Typed failures for ACP transports and connections.
 */
import * as Schema from "effect/Schema"
import { RequestId } from "./AcpSchema.ts"

/**
 * A transport could not open, read, write, or frame a message.
 *
 * @category errors
 */
export class AcpTransportError extends Schema.TaggedError<AcpTransportError>()("AcpTransportError", {
  reason: Schema.Literals(["Open", "Read", "Write", "Closed", "FrameTooLarge", "InvalidFrame"]),
  message: Schema.String,
  cause: Schema.optional(Schema.Defect())
}, { identifier: "effect-acp/AcpError/AcpTransportError" }) {}

/**
 * The connection terminated; pending and later calls observe this failure.
 *
 * @category errors
 */
export class AcpConnectionClosed extends Schema.TaggedError<AcpConnectionClosed>()("AcpConnectionClosed", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect())
}, { identifier: "effect-acp/AcpError/AcpConnectionClosed" }) {}

/**
 * The remote peer answered a request with a JSON-RPC error.
 *
 * @category errors
 */
export class AcpRemoteError extends Schema.TaggedError<AcpRemoteError>()("AcpRemoteError", {
  code: Schema.Int,
  message: Schema.String,
  data: Schema.optional(Schema.Unknown)
}, { identifier: "effect-acp/AcpError/AcpRemoteError" }) {}

/**
 * Protocol validation failure for a local payload or data received from the peer.
 *
 * **Details**
 *
 * Covers invalid parameters or responses and values that cannot be encoded for the wire. The
 * optional cause retains diagnostic information.
 *
 * @category errors
 */
export class AcpProtocolError extends Schema.TaggedError<AcpProtocolError>()("AcpProtocolError", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect())
}, { identifier: "effect-acp/AcpError/AcpProtocolError" }) {}

/**
 * A configured communication limit was reached; nothing was sent.
 *
 * @category errors
 */
export class AcpCapacityError extends Schema.TaggedError<AcpCapacityError>()("AcpCapacityError", {
  resource: Schema.Literals(["pendingRequests"]),
  limit: Schema.Finite
}, { identifier: "effect-acp/AcpError/AcpCapacityError" }) {}

/**
 * A local deadline elapsed. The remote request may still be running; this is not a confirmed remote
 * cancellation.
 *
 * @category errors
 */
export class AcpTimeoutError extends Schema.TaggedError<AcpTimeoutError>()("AcpTimeoutError", {
  method: Schema.String,
  requestId: RequestId
}, { identifier: "effect-acp/AcpError/AcpTimeoutError" }) {}

/**
 * Initialization selected a protocol version outside the enabled set.
 *
 * @category services
 */
export class AcpUnsupportedVersion extends Schema.TaggedError<AcpUnsupportedVersion>()("AcpUnsupportedVersion", {
  requested: Schema.Finite,
  received: Schema.Unknown,
  supported: Schema.Array(Schema.Finite)
}, { identifier: "effect-acp/AcpError/AcpUnsupportedVersion" }) {}

/**
 * Failures of an outgoing request.
 *
 * @category errors
 */
export type AcpRequestError = AcpRemoteError | AcpProtocolError | AcpConnectionClosed | AcpCapacityError
