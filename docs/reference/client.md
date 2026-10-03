# Client and session reference

Scope: the application API exported by `effect-acp/AcpClient`, its local implementation, and the serializable state in `effect-acp/AcpApp`. The hosted implementation adds the [gateway's recovery and authorization behavior](hosting.md).

## Service composition

`AcpLocalClient.layer` provides `AcpClient` and requires `AcpConnector`. `Stdio.layer`, `WebSocket.layer`, or `AcpConnector.layer(acquire)` provide it and open a fresh scoped transport for each connection. Platform requirements come from that layer. `AcpClient.connect(options)` requires `Scope` and returns an initialized `AcpAgentConnection`.

Connection scope owns direct session runtimes. A session handle does not extend that lifetime. A hosted attachment has a client scope, while its retained runtime belongs to the server host.

## ConnectOptions

| Field | Default | Meaning |
| --- | --- | --- |
| `versions` | `[1]` | `[1]`, `[2]`, or `[2, 1]`; highest enabled version is offered. |
| `params` | Required | Initialize fields excluding `protocolVersion`; v1 uses `clientInfo`/`clientCapabilities`, v2 uses `info`/`capabilities`. |
| `timeout` | No deadline | Deadline for initialization, including its send; not a global prompt timeout. |
| `v1Handlers` | None | Implemented filesystem/terminal callbacks; ignored on v2. |
| `terminalAuth` | None | Callback that executes an advertised terminal authentication invocation. Enables advertising that capability. |
| `onElicitation` | None | Answers connection request-scoped elicitation; receives the original request and selected version. |
| `limits` | See below | Partial overrides for retained content budgets. |
| `provisional` | 128 updates, 4 MiB | Bounds updates arriving during session creation/resume before routing is established. |
| `observerCapacity` | 256 | Events buffered for each observer. |
| `interactionTimeout` | No deadline | Pending decision lifetime before expiration. |
| `cancelTimeout` | 10 seconds | Time to wait for confirmed foreground completion after cancellation. |

`v1Handlers` has `readTextFile`, `writeTextFile`, `createTerminal`, `terminalOutput`, `waitForTerminalExit`, `killTerminal`, and `releaseTerminal`. Their requests/results use the corresponding v1 schemas. These callbacks expose neither an environment requirement nor a typed error channel; application dependencies must be supplied when constructing them. Only installed capabilities are advertised. An application must implement its own filesystem and terminal access policy.

The `terminalAuth` callback receives the agent's version-specific terminal-auth method. After it succeeds, `authenticate` closes the original local connection and releases its session runtimes. Call `AcpClient.connect` again to acquire and initialize a fresh connection with the new credentials. Terminal methods are never sent through `authenticate` or `auth/login` on the wire. Without the callback, terminal methods are not considered usable.

Elicitation modes must be explicitly advertised through `params.clientCapabilities.elicitation` on v1 or `params.capabilities.elicitation` on v2, for example `{ form: {}, url: {} }`. Omitted, null, and unadvertised modes are rejected with `Invalid params` before an interaction or callback starts. Session-scoped requests appear in that session's interactions. Request-scoped requests call `onElicitation(request, version)` with their original `requestId`, URL elicitation ID, and metadata, without attribution to any session. Return `{ _tag: "accept", content? }`, `{ _tag: "decline" }`, or `{ _tag: "cancel" }`. Without a callback these requests receive cancellation. Callback failures receive a generic protocol error. Request cancellation, URL completion withdrawal, and `interactionTimeout` interrupt a pending callback and release its bounded admission slot. Supply callback dependencies before passing the callback; it has no environment requirement.

## AcpAgentConnection

| Member | Result and behavior |
| --- | --- |
| `capabilities` | Normalized, version-tagged capabilities and original peer advertisement. |
| `negotiated` | Selected version, actual initialize advertisement sent, and decoded response. |
| `closed` | Effect that succeeds with the terminal `AcpConnectionClosed` reason. It is not a command to close. |
| `newSession(options)` | Creates a session and returns its handle. |
| `resumeSession(options)` | Returns a handle for an existing agent session. History requirements may fail with `AcpHistoryUnavailable`. |
| `listSessions(cwd?)` | Session summaries where supported. The local implementation fetches one page; it does not iterate the agent's pagination cursor. |
| `authenticate(methodId)` | Selects an advertised method. Terminal success closes the local connection; connect again to initialize with the new credentials. |
| `logout` | Agent logout, where advertised. |
| `request(method, params?)` | Raw extension escape hatch; result is `unknown` until validated by an application schema. |

`NewSessionOptions` requires an absolute agent-side `cwd`; optional fields are `additionalDirectories` and `mcpServers`. `ResumeSessionOptions` adds `sessionId` and v2 `replayFrom`. v1 prefers advertised `session/resume`, otherwise uses `session/load` when supported. A v2 replay selector is not sent on v1.

MCP server descriptors configure tools available to the agent; they do not change the ACP transport. The client checks negotiated support for those descriptors and other optional surfaces before dispatch.

## AcpSession

| Member | Result and behavior |
| --- | --- |
| `sessionId`, `version` | Agent session identity and selected ACP version. |
| `snapshot` | Current immutable `SessionSnapshot`. |
| `observe` | Scoped acquisition of a snapshot plus changes after that exact boundary. |
| `changes` | Scoped stream of subsequent observations; no initial snapshot. |
| `submit(prompt)` | Dispatches content blocks and returns a `Submission`. One foreground submission at a time. |
| `cancel` | Sends session cancellation and waits for confirmation within `cancelTimeout`. |
| `resolveInteraction(id, resolution)` | Resolves one pending permission or elicitation request. |
| `setConfigOption(configId, value)` | Sets a string or boolean configuration value where supported. |
| `setMode(modeId)` | v1 session-mode operation; unsupported on v2. |
| `close`, `delete` | Explicit agent lifecycle mutations, capability-gated. |
| `release` | Releases local routing; hosted implementation detaches. Sends no close/delete/cancel ACP mutation. |

Permissions accept `{ _tag: "selected", optionId }` or `{ _tag: "cancelled" }`. Elicitations accept `{ _tag: "accept", content? }`, `{ _tag: "decline" }`, or `{ _tag: "cancel" }`. An option ID must come from the offered request. Interaction resolution is one-shot.

## Submission

| Member | Meaning |
| --- | --- |
| `id` | Library submission ID. |
| `snapshot` | Current `SubmissionSnapshot`, including retained terminal status. |
| `accepted` | v2 inserted agent message ID; immediately fails with `AcpCapabilityUnsupported` on v1. |
| `outcome` | Foreground completion snapshot: v1 prompt response, or v2 idle completion. |

The v1 completion boundary includes dispatch of notifications queued before that response. Completion does not mean all tools succeeded: foreground stop reasons can include refusal or cancellation. Interrupting either wait does not cancel the agent's work.

## Snapshot and retention

A `SessionSnapshot` contains `sessionId`, `version`, local `seq`, metadata, foreground state, active submission ID, submissions, messages, tool calls, plans, terminals, available commands, configuration, usage, interactions, raw updates, and truncation markers. Schemas for these values are exported from `AcpApp`.

Each observation is `{ _tag: "snapshot", snapshot }`. The stream is ordered and bounded. Slow subscribers fail with `AcpSubscriptionOverflow`; they need a fresh `observe` boundary. Snapshot sequence numbers are local projection order, not ACP message IDs or durable gateway cursors.

| Content limit | Default |
| --- | ---: |
| `transcriptBytes` | 4,194,304 |
| `submissions` | 128 |
| `messages` | 200 |
| `toolCalls` | 64 |
| `plans` | 16 |
| `terminals` | 32 |
| `terminalBytes` | 32,768 |
| `rawUpdates` | 512 |
| `interactions` | 64 |

Older retained content is discarded when budgets are exceeded, with `truncated` markers. Pending interactions are subject to admission bounds and are not silently evicted. A snapshot is a bounded projection, not an archival conversation log. v1 message identities are synthesized locally and marked with local provenance.

Tool-call content retains the negotiated version's diff shape: v1 uses `oldText`/`newText`, and v2 uses structured changes. A v1 null or omitted tool name preserves its previous value; v2 null clears it. Terminal snapshots expose `exited` separately from nullable `exitCode` and `exitSignal`, so a concrete exit status with unknown details remains observable. Newly projected and decoded snapshots include that boolean; older serialized snapshots missing it decode with `false`.

## Failures

Failures stay in the Effect error channel. Shared operations can expose connection/protocol failures as well as session and gateway failures.

| Error tag | Condition |
| --- | --- |
| `AcpTransportError` | Transport open/read/write/framing failure. |
| `AcpConnectionClosed` | The underlying peer terminated. |
| `AcpRemoteError` | Agent JSON-RPC error, with code/message/data. |
| `AcpProtocolError` | Invalid protocol data, interaction resolution, or local protocol misuse. |
| `AcpTimeoutError` | A configured request deadline elapsed. |
| `AcpUnsupportedVersion` | Selected protocol version is outside the allowed policy. |
| `AcpCapacityError` | Low-level outgoing request bound reached. |
| `AcpCapabilityUnsupported` | Operation or content is not supported by the negotiated peer. |
| `AcpSessionBusy` | Another foreground submission is active. |
| `AcpInteractionAlreadyResolved` | A decision is no longer pending. |
| `AcpInteractionExpired` | Decision lifetime ended. |
| `AcpCancellationUnconfirmed` | Cancellation was sent but completion was not confirmed. |
| `AcpHistoryUnavailable` | Requested history cannot be recovered. |
| `AcpProvisionalOverflow` | Updates exceeded provisional routing bounds. |
| `AcpSubscriptionOverflow` | Observer fell behind its delivery capacity. |
| `AcpGatewayError` | Hosted authorization, capacity, retention, or recovery failure; see [codes](hosting.md#gateway-errors). |
