# Hosting and recovery reference

Scope: `AcpHost`, `AcpGateway`, `AcpGatewayClient`, `AcpRemoteClient`, and `server/GatewayHttp`. These implement an application protocol layered above ACP.

## Host construction

`AcpHost.layer(options)` supplies one host runtime for its layer scope. `options` requires `policy`, `authorize`, and `open`; `onLifecycle` is optional.

`authorize(identity, access)` receives an authenticated `principalId`, workspace, action (`open`, `read`, `attach`, `takeover`, or `command`), and relevant connection/session IDs. It returns `Effect<void, AcpGatewayError, R>`.

`open(identity, workspace, profile, options, enforced)` acquires a scoped `AcpAgentConnection`. Profile options are untrusted until the application's schema validates them. `enforced` contains client `interactionTimeout`, `cancelTimeout`, `limits`, and `observerCapacity`. The host depends on the callback applying those settings.

`onLifecycle` receives event type (`opened`, `attached`, `detached`, `expired`, `admitted`, `settled`) and connection/session/command counts. It contains no prompt or filesystem bodies.

Default host and gateway diagnostics report failure categories without attaching internal error causes or messages. Applications that need private diagnostics can record them inside their own callbacks with their own redaction policy.

## Policy

Every field is required and must be a positive safe integer. There are no default policy values.

| Field | Meaning |
| --- | --- |
| `retentionMs` | Retention after a session detaches, and idle connection retention. |
| `interactionMs` | Pending interaction deadline enforced in the local client. |
| `shutdownMs` | Bounded cancellation/cleanup window. Process termination also needs a platform policy. |
| `retryMs` | Lifetime of a server-issued command retry window. |
| `events` | Retained journal event count per session. |
| `eventBytes` | Retained journal byte budget per session. |
| `subscriberCapacity` | Attachment delivery queue capacity; also enforced on local observation. |
| `transcriptBytes` | Local session retained-content byte budget. |
| `terminalBytes` | Retained terminal output budget. |
| `commands` | Host command-ledger and retry-window capacity. |
| `connections` | Host connection capacity, including opens in progress. |
| `sessions` | Host session capacity, including creation in progress. |

History behind the journal floor triggers snapshot resynchronization. An expired session is removed; an otherwise healthy sibling session does not lose its shared connection solely because that sibling expired.

## Gateway route and client

`GatewayHttp.route` defaults to `/acp/gateway`. It requires `authenticate(request)` returning `AcpGateway.Identity` and `allowOrigin(origin)`. It serves versioned Effect RPC over WebSocket using NDJSON serialization. It is distinct from the raw ACP WebSocket bridge profile.

`AcpGatewayClient.connect(url, options)` requires `Scope` and `Socket.WebSocketConstructor`. Options are:

| Field | Default/contract |
| --- | --- |
| `workspace` | Required logical workspace ID |
| `storage` | Required `Storage` adapter |
| `storageKey` | `effect-acp:${workspace}` prefix |
| `disconnected` | Optional signal for API-backed clients; `connect` supplies its socket-disconnect signal |

Storage keys must distinguish unrelated identities and workspaces in the application's storage design. The client carries a logical client ID distinct from the authenticated principal and from any particular socket. Reconnection is explicit; the socket client does not automatically resend ACP commands.

## Storage

`Storage.load(key)` returns `Effect<unknown>`. `save(key, value)` returns `Effect<void, SaveError>`, and `remove(key)` returns `Effect<void>`. The optional `Storage<SaveError>` type parameter defaults to `AcpGateway.GatewayError`; `fromApi`, `make`, and `connect` retain a custom adapter's save error type. The unknown loaded value is an external persistence boundary; schemas validate retained protocol values when loaded.

The interface has no storage environment channel. Adapters must be composed with their dependencies and implement the application's failure policy. Synchronous browser storage and serialization may throw; convert expected failures into the adapter's typed save error instead of leaving them as defects in `Effect.sync`. The library does not provide a durable browser-storage adapter. `memoryStorage()` reports an uncloneable value as `AcpGateway.GatewayError` with code `Invalid`, leaving the previous value for that key intact.

Values must round-trip as structured data. Saves of operation admission data complete before transmission. Stored state includes logical identity, window tokens, operation IDs/admissions, cursors, and snapshots; command and transcript contents can be present. The application persists its session descriptor separately. `memoryStorage()` clones saved values into a Map and survives only as long as that object.

## Remote client

`AcpRemoteClient.make(gateway, options)` returns a client with `connect`, `attach`, and `descriptor`. `AcpRemoteClient.layer` exposes the common `AcpClient` service over the same implementation.

| Option/member | Meaning |
| --- | --- |
| `profile` | Required server-authorized launch profile |
| `profileOptions` | Application payload validated by the host profile |
| `connectionKey` | Separates connections to the same profile in retained client state |
| `observerCapacity` | Default 256, positive safe integer |
| `connect(options)` | Opens or recovers a connection. The host determines ACP negotiation and handlers; browser initialize options do not install host capabilities. |
| `attach(descriptor, takeover = false)` | Attaches to retained state without another ACP initialize/resume/prompt. Requires Scope. |
| `descriptor(session)` | Descriptor for a known remote session, or `undefined`. |

A session descriptor contains `epoch`, host `session`, agent `sessionId`, and ACP `version`. `AcpGateway.SessionDescriptor` is its schema. Host session IDs and agent session IDs are distinct namespaces.

Terminal authentication runs the host application's `terminalAuth` callback and closes the original ACP connection after success. The remote client's `authenticate(methodId)` then removes that connection's retained descriptor. Call `connect` again on the same remote client, or construct a new remote client with the same storage and connection key, to open a fresh host connection and initialize ACP with the updated credentials. Old connection and session handles remain closed. Agent authentication keeps the connection and its retained descriptor. Gateway disconnection alone does not discard retained recovery state.

Only one controller attaches at a time. Explicit authorized takeover advances a generation and revokes stale writes. Remote attachments send expected agent session ID and protocol version, which the host checks before replacing a controller. A mismatched descriptor fails with `Invalid` and leaves the existing controller active. Reattaching cancels the detached-session expiry timer. Pending decisions remain pending until answered or expired; reconnection never auto-approves them.

Direct gateway callers should supply `AttachmentRequest.expected` with `{ sessionId, version }` from the saved descriptor. The check and controller replacement share one host transition. The field is optional for compatibility with older direct gateway clients.

## Operation recovery

`pendingOperations` lists saved operation IDs. `retry(operationId, generation?)` looks up the original operation before attempting admission; it uses the original admission/window rather than renewing that retry. `wait` waits for a recorded operation to settle. `forget` removes client-side recovery data; it does not cancel the host operation.

Operations have `operationId`, `status`, `result`, and nullable `error`. Status is one of `admitted`, `succeeded`, `failed`, or `outcomeUnknown`. Admission acknowledges host acceptance; it is separate from an agent's message-insertion acknowledgement and foreground completion.

The host deduplicates an operation ID and equal payload within its retained retry window. A conflicting payload fails. Live retry records are not evicted merely to admit another command. Commands already admitted continue independently of the initiating RPC wait. Uncertain agent responses are represented as `outcomeUnknown` and are not automatically resent.

Recovery is bounded by host lifetime, session retention, and operation retry windows. There is no host-crash durability or cross-host exactly-once guarantee.

## Gateway errors

All use `_tag: "AcpGatewayError"` and a `code`:

| Code | Condition |
| --- | --- |
| `UnsupportedVersion` | Gateway protocol version is unsupported. |
| `Unauthorized` | Identity/workspace/profile/action authorization failed. |
| `HostRestarted` | Saved host epoch does not match this host. |
| `NotFound` | Requested session, connection, or operation is absent or not visible; includes removed retained state. |
| `Conflict` | Conflicting controller or operation payload. |
| `StaleController` | Mutation belongs to a revoked controller generation. |
| `WindowExpired` | Original retry window expired. |
| `Capacity` | A bounded host capacity is full. |
| `ResyncRequired` | Client cannot apply a discontinuous stream without a new boundary. |
| `Closed` | Gateway/attachment connection ended. |
| `Invalid` | Invalid settings, stored state, or command data. |
| `AgentFailure` | Agent-side failure that is not exposed as a more specific supported error. |
| `OutcomeUnknown` | ACP work may have happened; result is uncertain. |
