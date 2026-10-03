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
