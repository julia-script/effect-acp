# Protocol and schema reference

Scope: `AcpProtocol`, `AcpConnection`, `AcpSchema`, and the generated v1/v2 schema modules. Application session projection is covered by the [client reference](client.md).

## Version negotiation

The default policy is **v1**. v2 is an explicitly enabled draft baseline. Allowed policies are `[1]`, `[2]`, and `[2, 1]`.

Initialization sends the highest enabled version once and validates the agent's response using the selected version's schema. `[2, 1]` permits a v1 response to that initialization; it does not retry initialization with a different payload if a v1-only peer rejects the v2 request. A connection may be initialized at most once, including failed attempts.

v1 initialize options use `clientInfo` and `clientCapabilities`; v2 uses `info` and `capabilities`. `AcpProtocol.Negotiated` retains both the advertisement actually sent and the selected response. An unaccepted selected version fails with `AcpUnsupportedVersion`.

| Surface | v1 | v2 draft baseline |
| --- | --- | --- |
| Prompt response | Turn completion with stop reason | Acknowledgement with inserted message ID |
| Turn completion | Prompt response | Idle state update |
| Transcript identity | No agent message IDs on chunks; local projection synthesizes IDs | Agent message IDs and replacement/chunk semantics |
| History | Advertised resume/load behavior | Resume with supported replay selector |
| Client filesystem/terminal callbacks | Optional installed v1 handlers | Different protocol surfaces; v1 callbacks are not advertised |
| Session mode | `setMode` when supported | Not exposed as the v1 mode operation |

These are baselines supported by the package, not a claim that all deployed agents implement every field or draft revision. The current [ACP documentation](https://agentclientprotocol.com) may evolve separately from the package's generated schemas.

## Schema modules

`effect-acp/protocol/v1` and `effect-acp/protocol/v2` export named schemas, matching static types, and `agentMethods`, `clientMethods`, and `protocolMethods` tables. A declared request binds a method name to parameter and result codecs. A notification binds its name to a parameter codec. `AcpProtocol.schemas` selects the tables by version.

Generated codecs preserve extension fields and validate known fields. Missing, null, and present values remain distinct where the source schema makes them distinct. JSON Schema `format` is treated as an annotation. Rust lenient-deserialization hints are not implemented as silent defaulting: invalid optional data fails validation.

Application snapshots, gateway admissions, cursors, and descriptors have their own schemas. They are not aliases for wire payloads. An arbitrary extension result or persistence value remains `unknown` until decoded at its boundary.

## Typed extension calls

`AcpSchema.request(name, params, result)` and `AcpSchema.notification(name, params)` declare application methods. `AcpSchema.Params` and `AcpSchema.Result` extract their types. This example declares a private application extension; it requires a matching handler on your own agent and is not a built-in Claude or Codex method.

<!-- example: ../examples/typed-extension.ts -->
```ts
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import type { AcpAgentConnection } from "effect-acp/AcpClient"
import type { Service } from "effect-acp/AcpConnection"
import * as AcpSchema from "effect-acp/AcpSchema"

// An application extension: this method must also exist on your agent.
export const ProjectInfo = AcpSchema.request(
  "_my_app/project_info",
  Schema.Struct({ sessionId: Schema.String }),
  Schema.Struct({ name: Schema.String, languages: Schema.Array(Schema.String) })
)

export const readFromPeer = (peer: Service, sessionId: string) =>
  peer.request(ProjectInfo, { sessionId })

// The high-level connection has a raw extension escape hatch. Decode its
// untrusted result at this boundary, keeping validation in the error channel.
export const readFromSessionClient = (connection: AcpAgentConnection, sessionId: string) =>
  connection.request(ProjectInfo.method, { sessionId }).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(ProjectInfo.result))
  )
```

The low-level peer validates request parameters and results through the declaration. The high-level connection's raw extension escape hatch requires explicit result decoding. Schema failures remain in the Effect error channel.

## JSON-RPC connection

`AcpConnection.make(options?)` requires `AcpTransport` and `Scope` and returns `AcpConnection.Service`. `AcpConnection.layer(options?)` provides that value through the `AcpConnection` service tag.

| Option | Default |
| --- | ---: |
| `maxPendingRequests` | 1,024 outgoing requests |
| `maxIncomingRequests` | 256 incoming requests |
| `notificationBuffer` | 256 queued notifications |
| `handlers` | Absent until installed |

Incoming requests run concurrently within the configured bound. Notifications dispatch serially through a bounded queue; a full queue backpressures the reader. Response delivery is independent of notification-handler completion. `drainNotifications` waits for already-queued notifications to finish dispatching and fails if the connection terminates. Calling it from a notification handler would wait on that same handler.

A notification handler that waits for a new outgoing response can also stall under sustained notification backpressure; long-running work belongs in an owned fiber rather than the serial handler.

| Member | Contract |
| --- | --- |
| `request(declaration, params, options?)` | Schema-validated request/result. |
| `requestRaw(method, params?, options?)` | Explicit untyped boundary. |
| `send(method, params?)` | Returns a pending request handle after transport dispatch. |
| `notify(declaration, params)` / `notifyRaw(method, params?)` | One-way messages. |
| `cancelRequest(id)` | Sends `$/cancel_request`; does not itself settle the pending result. |
| `setHandlers(handlers)` | Installs handlers and releases messages waiting for installation. |
| `pendingRequests` | Current outgoing request count. |
| `drainNotifications` | Notification dispatch boundary. |
| `closed` | Terminal reason as a successful Effect value. |

A request `timeout` includes send backpressure and response waiting. Timeout before dispatch has a null `requestId`. Interrupting a response wait does not imply remote session cancellation.

`onRequest` and `onNotification` construct schema-backed routes for `handlers(routes, fallback?)`. Raw handlers receive unknown payloads. Returning `undefined` declines a method; unknown requests get Method not found, while unknown notifications are ignored. Expected request failures use `AcpRemoteError`. Defects are converted to generic internal errors without their details.

Request handlers receive `RequestContext.commitResult(effect)` for an irreversible acceptance boundary. Its successful JSON result remains authoritative after later handler failures or request cancellation. Request cancellation waits for an active acceptance boundary to finish; connection shutdown can still interrupt it. Keep cancellable preparation and foreground work outside the boundary. The agent helper uses it to preserve a v2 prompt's inserted message ID.
