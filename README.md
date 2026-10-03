# effect-acp

**Agent sessions for Effect applications.**

Connect to coding agents, build streaming chat interfaces, or expose your own agent through the [Agent Client Protocol](https://agentclientprotocol.com). `effect-acp` brings ACP into Effect's services, schemas, streams, and scopes—from a local process to a browser application with retained sessions.

[![Effect 4](https://img.shields.io/badge/Effect-4.0-111827)](https://effect.website/)
[![TypeScript](https://img.shields.io/badge/TypeScript-ESM-3178c6?logo=typescript&logoColor=white)](package.json)
[![MIT license](https://img.shields.io/badge/license-MIT-22c55e)](LICENSE)

[Get started](docs/tutorials/first-session.md) · [Connect to Claude or Codex](docs/how-to/real-agents.md) · [API reference](docs/reference/client.md) · [Documentation](docs/README.md)

## Why effect-acp?

- **A session API for your application.** Connect, create a session, submit a prompt, and read or observe its state.
- **Streaming state, ready for a UI.** Immutable snapshots collect messages, tool calls, plans, usage, and pending interactions. Subscribe to changes with Effect streams.
- **Permissions you control.** Present agent requests to the user and resolve them explicitly through the session handle.
- **Resources with an owner.** Processes, sockets, and subscriptions follow Effect scopes. Layers supply the transport and platform services.
- **Several ways to deploy.** Connect directly over stdio, relay ACP through a WebSocket bridge, or retain sessions on a server across browser disconnects.
- **Agent authoring included.** Define session and prompt handlers, emit updates, and serve your own agent over ACP.

## Install

```sh
bun add effect-acp effect
```

Or with npm:

```sh
npm install effect-acp effect
```

Built for **Effect 4**, a peer dependency. For terminal applications, add the matching platform package:

```sh
# Bun
bun add @effect/platform-bun

# Node.js
npm install @effect/platform-node
```

Browser clients use web APIs and injected services. The package ships ESM JavaScript and TypeScript declarations, with explicit subpath exports.

## Your first session with Claude

This example starts the [Claude ACP adapter](https://github.com/agentclientprotocol/claude-agent-acp), asks Claude a question, and prints its reply. You need Bun, Node.js 22 or newer, and an Anthropic API key.

Install the client dependencies and the adapter in your project directory:

```sh
bun add effect-acp effect @effect/platform-bun
bun add --dev @agentclientprotocol/claude-agent-acp@0.79.0
```

The adapter provides the `claude-agent-acp` executable used below. Set your API key in the shell where you will run the client; the child process inherits it. See the [Claude Agent SDK authentication setup](https://code.claude.com/docs/en/agent-sdk/quickstart#set-your-api-key) for other providers.

```sh
export ANTHROPIC_API_KEY="your-api-key"
```

Save this as `claude-session.ts` in the same directory:

<!-- example: docs/examples/claude-session.ts -->
```ts
import * as BunRuntime from "@effect/platform-bun/BunRuntime"
import * as BunServices from "@effect/platform-bun/BunServices"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as ChildProcess from "effect/process/ChildProcess"
import { AcpClient } from "effect-acp/AcpClient"
import * as AcpLocalClient from "effect-acp/AcpLocalClient"
import * as Stdio from "effect-acp/transport/Stdio"

// Start the locally installed Claude ACP adapter over stdin/stdout.
const agentProcess = ChildProcess.make("./node_modules/.bin/claude-agent-acp", [], {
  forceKillAfter: "2 seconds"
})

const ClientLive = AcpLocalClient.layer.pipe(
  Layer.provide(Stdio.layer(agentProcess)),
  Layer.provide(BunServices.layer)
)

const program = Effect.gen(function*() {
  // 1. Connect to Claude using ACP v1.
  const client = yield* AcpClient
  const connection = yield* client.connect({
    versions: [1],
    params: { clientInfo: { name: "claude-session", version: "1.0.0" } },
    timeout: "30 seconds",
    interactionTimeout: "30 seconds"
  })
  yield* Console.log(`Connected using ACP v${connection.capabilities.version}`)

  // 2. Open a session in the current directory.
  const session = yield* connection.newSession({ cwd: process.cwd() })

  // 3. Ask a question that needs no file access or tool permissions.
  const submission = yield* session.submit([{
    type: "text",
    text: "Explain Effect scopes in two sentences. Do not read files, run commands, or use tools."
  }])
  yield* submission.outcome

  // 4. Print Claude's completed reply from the session snapshot.
  const snapshot = yield* session.snapshot
  for (const message of snapshot.messages) {
    if (message.kind !== "agent") continue

    const text = message.content
      .flatMap((block) =>
        "text" in block && typeof block.text === "string" ? [block.text] : []
      )
      .join("")
    yield* Console.log(`Claude: ${text}`)
  }
})

if (import.meta.main) {
  // Release the connection and child process when the program finishes.
  BunRuntime.runMain(program.pipe(
    Effect.provide(ClientLive),
    Effect.scoped
  ))
}
```

Run it from your project directory:

```sh
bun claude-session.ts
```

The client prints `Connected using ACP v1`, followed by Claude's answer. The response text varies by model.

`submission.outcome` waits for the foreground turn to finish. The session retains its current snapshot; use `session.observe` to acquire a snapshot and the stream of changes that follows it. For prompts that use coding tools, the [session UI guide](docs/how-to/session-ui.md) shows how to handle streaming updates and permission requests.

See [Claude and Codex setup](docs/how-to/real-agents.md) for more agent options, or the [first-session tutorial](docs/tutorials/first-session.md) for an echo-agent walkthrough that needs no credentials.

## Choose where the session lives

```mermaid
flowchart LR
  Local[Terminal or desktop app] -->|ACP / stdio| Agent1[Agent]
  Browser1[Browser app] -->|ACP / WebSocket| Bridge[Bridge]
  Bridge -->|ACP / stdio| Agent2[Agent]
  Browser2[Browser app] -->|Gateway RPC| Host[Session host]
  Host -->|ACP / stdio| Agent3[Agent]
```

| Deployment | Session lifetime | Start here |
| --- | --- | --- |
| **Direct client** | Your application scope owns the connection and agent process. | [Claude and Codex](docs/how-to/real-agents.md) |
| **WebSocket bridge** | The bridge relays ACP for the browser socket's lifetime. | [Browser bridge](docs/how-to/browser-bridge.md) |
| **Hosted sessions** | The server retains sessions across client disconnects, within its configured limits. | [Hosted sessions](docs/how-to/hosted-sessions.md) |

Hosted retention is in memory and bounded by the host's lifetime. Read the [ownership explanation](docs/explanation/ownership.md) for how connections, sessions, observers, and cancellation fit together.

## Build with the pieces you need

| API | Purpose |
| --- | --- |
| `AcpClient` | Connect to an agent and work with session handles. |
| `AcpApp` | Schemas and types for session snapshots, submissions, and interactions. |
| `AcpConnector` · `AcpTransport` | Connection factory and single scoped transport services. |
| `transport/Stdio` · `transport/WebSocket` · `transport/InMemory` | Compose process, socket, or in-memory connections. |
| `AcpAgent` · `agent/Store` | Define agent behavior and provide session storage. |
| `AcpHost` · `AcpGateway` · `AcpRemoteClient` | Host sessions and attach remote clients to retained state. |
| `protocol/v1` · `protocol/v2` | Use the generated protocol schemas directly. |

Import modules through `effect-acp/<module>`. Start with the session API; the lower-level connection and protocol modules are available when you need raw requests or a custom transport.

## Documentation

| I want to… | Guide |
| --- | --- |
| Run a complete example | [Your first agent session](docs/tutorials/first-session.md) |
| Connect to a coding agent | [Claude and Codex](docs/how-to/real-agents.md) |
| Render chat, tools, and permissions | [Build a session UI](docs/how-to/session-ui.md) |
| Connect a browser to a stdio agent | [Add a WebSocket bridge](docs/how-to/browser-bridge.md) |
| Keep sessions across disconnects | [Retain sessions on a host](docs/how-to/hosted-sessions.md) |
| Implement my own ACP agent | [Write an ACP agent](docs/how-to/write-agent.md) |
| Look up exact behavior | [Client](docs/reference/client.md) · [Transports](docs/reference/transports.md) · [Hosting](docs/reference/hosting.md) · [Protocol](docs/reference/protocol.md) · [Agents](docs/reference/agent.md) |

These guides assume TypeScript and basic Effect familiarity. ACP connects an application to an agent; MCP servers supply tools the agent can use within a session.

## Development

```sh
bun install
bun run check
bun run verify:package
```

The checks cover generated schemas, types, lint, documentation examples, tests, browser imports, and the packed package's public exports. For publishable changes, run `bun run changeset` and commit the generated changeset alongside your implementation.

[Development guide](docs/development.md) · [Publishing guide](docs/publishing.md) · [Architecture](ARCHITECTURE.md) · [Type audit](TYPE_AUDIT.md)

## License

[MIT](LICENSE) © Julia Ortiz
