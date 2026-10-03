# Agent authoring reference

Scope: `AcpAgent`, `agent/Content`, and `agent/Store`. These APIs serve an agent to ACP clients. They do not supply a language model or tool execution engine.

## Construction and serving

`AcpAgent.make(options)` returns an Effect that constructs the agent, preserving handler Effect environment requirements. Invalid handler/capability configuration fails with the typed `AcpAgentConfigError` before serving. `AcpAgent.makeUnsafe(options)` constructs synchronously and throws that error for invalid configuration; use it only when the configuration is known to be valid.

Migration: move `const agent = AcpAgent.make(options)` into an Effect as `const agent = yield* AcpAgent.make(options)`, then serve the result. For a deliberately synchronous startup path, replace the old call with `AcpAgent.makeUnsafe(options)`.

| Option | Contract/default |
| --- | --- |
| `info` | Required `name`, `version`, optional `title` |
| `versions` | `[1]`; also accepts `[2]` and `[2, 1]` |
| `session` | Required session handlers |
| `prompt` | Required insertion and execution handlers |
| `auth` | Optional advertised authentication methods and handlers |
| `list` | Enables session-list support backed by `Store`; omitted means not advertised |

The constructed agent's `serve` Effect requires `AcpTransport`, `Store`, handler dependencies, and Scope. `AcpAgent.serveStdio(agent)` supplies the current-process stdio transport and stderr logging; the caller supplies `Stdio`, `Store`, and handler dependencies. `layerStdio` is the layer form for serving at the application boundary.

## Handler contracts

| Handler | Input/result |
| --- | --- |
| `session.create` | `cwd`, additional directories, MCP servers, peer; returns `{ sessionId }`. |
| `session.resume` | Session context, `cwd`, `replayFromStart`; optional. |
| `session.close` | Session context; optional. |
| `session.delete` | Session context; optional. |
| `session.cancel` | Session context after owned execution has been interrupted; optional. |
| `prompt.insert` | Session context and prompt; returns `{ messageId }`. |
| `prompt.execute` | Session context, prompt, inserted message ID, `emit`, and `client`; returns stop reason. |
| `auth.login` | `{ methodId }`; required when authentication methods are advertised. |
| `auth.logout` | No argument; required when authentication methods are advertised. |

Session context includes `sessionId`, selected `version`, and `peer`. The peer contains client info, advertised elicitation modes, and the decoded initialize request. Execution handlers obtain workspace or conversation details from application state using the session ID; `execute` does not receive a `cwd` field directly.

Stop reasons are `end_turn`, `max_tokens`, `max_turn_requests`, `refusal`, `cancelled`, and `error`. v1 returns its compatible stop reason in the prompt response; `error` maps to `refusal` on v1. v2 acknowledges successful insertion separately and ends foreground execution with an idle update after final output. Failed v2 execution or input retention reports `error`, with a sanitized JSON-RPC error when available. A deliberate refusal remains `refusal`.

Once `prompt.insert` succeeds on v2, its message ID is the successful acknowledgement even if retention fails or request cancellation arrives. Retention and execution run in the session-owned scope. Request cancellation before the insertion phase can interrupt the request. During `prompt.insert`, request cancellation waits for its accepted message ID or rejection, because the helper cannot inspect an application-owned conversation transaction. The insertion remains interruptible when the owning connection closes. After insertion, cancelling the prompt RPC does not cancel foreground work. `session/cancel` interrupts the session-owned turn and drains its finalizers before reporting idle. Closing an active v2 session performs the same cancellation before calling the optional close handler and releasing session resources.

Terminal authentication methods are advertised only when the initializing client enables terminal authentication (`clientCapabilities.auth.terminal: true` on v1, or a non-null `capabilities.auth.terminal` object on v2). Other authentication methods remain available.

## Output and client interactions

| Helper | Behavior |
| --- | --- |
| `emit.agentChunk(id, content)` | Appends agent text/content. |
| `emit.thoughtChunk(id, content)` | Appends agent thought content. |
| `emit.userChunk(id, content)` | Echoes user content. |
| `emit.message(role, id, content)` | Full replacement, v2 only. |
| `emit.raw(update)` | Version-specific raw update, encoded against the selected protocol schema. |
| `client.requestPermission({ title, options, toolCallId? })` | Returns selected option ID or `null` for cancellation. Each option has `optionId`, `name`, and v1-compatible permission `kind`. |
| `client.elicit(request)` | v2 elicitation response; unsupported advertised mode fails before dispatch. |

Chunk helpers omit message identity on v1. Full replacements fail on v1. Message IDs must be meaningful and stable in the author's conversation model.

`HandlerError` includes agent/store and protocol communication failures. `AcpAgentError` carries code, message, and optional data. `unknownSession` and `authRequired` construct common failures. Request-handler defects receive a generic Internal error response. Non-interruption failures during turn execution report `error` on v2 and retain the existing `refusal` completion on v1. Internal defects receive a generic error message.

## Store service

`Store.layer` provides an in-memory implementation. The `Store` service operations are:

| Operation | Semantics |
| --- | --- |
| `list(cwd?)` | Session metadata, optionally filtered by working directory. |
| `get(sessionId)` | Session metadata or `undefined`. |
| `create(session)` | Creates metadata; duplicate ID fails with conflict. |
| `update(sessionId, partial)` | Updates metadata excluding identity. |
| `remove(sessionId)` | Removes metadata and transcript; missing ID is a no-op. |
| `retain(message)` | Records/merges a message and returns the stored form. |
| `retained(sessionId)` | Messages in recording order. |

Retention appends chunks when no replacement is supplied. A non-null replacement resets prior chunks before appending the new ones. Replay emits a retained replacement before its chunks.

The authoring helper performs output retention on a best-effort basis and ignores errors from those output-retention writes. Therefore neither a successful emit nor supplying a durable Store guarantees that every emitted chunk is durably recorded. Session existence checks and other store operations have their own failure handling. Host gateway retention is separate from agent-side persistence.
