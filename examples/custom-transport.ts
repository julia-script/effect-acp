/**
 * Build a custom transport from any message-oriented channel. Here a web
 * `MessagePort` carries one JSON text frame per message, so no extra framing
 * is needed; the same shape fits WebSockets, Electron IPC, or workers.
 *
 *   bun examples/custom-transport.ts
 */
import * as BunRuntime from "@effect/platform-bun/BunRuntime"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"
import * as Stream from "effect/Stream"
import { AcpConnector, AcpError, AcpProtocol, AcpTransport } from "effect-acp"
import { createAgent } from "../test/fixtures/agent.ts"

/** Adapts a MessagePort. The port is closed when the connection's scope closes. */
export const fromMessagePort = (port: MessagePort) =>
  Effect.gen(function*() {
    // MessagePort callbacks cannot backpressure their sender. Overflow is terminal.
    const inbox = yield* Queue.bounded<string, AcpError.AcpTransportError>(256)
    let closed = false
    const fail = (error: AcpError.AcpTransportError) => {
      if (closed) return
      closed = true
      port.onmessage = null
      Queue.failCauseUnsafe(inbox, Cause.fail(error))
      port.close()
    }
    port.onmessage = (event) => {
      if (closed) return
      if (typeof event.data !== "string") {
        fail(new AcpError.AcpTransportError({ reason: "InvalidFrame", message: "non-text frame" }))
      } else if (!Queue.offerUnsafe(inbox, event.data)) {
        fail(new AcpError.AcpTransportError({ reason: "Read", message: "MessagePort inbound capacity exceeded" }))
      }
    }
    yield* Effect.addFinalizer(() => Effect.sync(() => fail(
      new AcpError.AcpTransportError({ reason: "Closed", message: "MessagePort transport is closed" })
    )))
    const transport: AcpTransport.Transport = {
      incoming: Stream.fromQueue(inbox),
      send: (frame) => Effect.suspend(() => closed
        ? Effect.fail(new AcpError.AcpTransportError({ reason: "Closed", message: "MessagePort transport is closed" }))
        : Effect.try({
          try: () => port.postMessage(frame),
          catch: (cause) => new AcpError.AcpTransportError({ reason: "Write", message: "postMessage failed", cause })
        }))
    }
    return transport
  })

const program = Effect.scoped(Effect.gen(function*() {
  const channel = yield* Effect.acquireRelease(
    Effect.sync(() => new MessageChannel()),
    (channel) => Effect.sync(() => {
      channel.port1.close()
      channel.port2.close()
    })
  )
  // An agent on the other port (here: the in-process fixture agent).
  const onLine = createAgent({ version: 1 }, (line) => channel.port2.postMessage(line))
  channel.port2.onmessage = (event) => onLine(event.data)

  const { negotiated } = yield* AcpProtocol.connect({
    params: { clientCapabilities: { fs: { readTextFile: false, writeTextFile: false }, terminal: false } }
  }).pipe(Effect.provide(AcpConnector.layer(fromMessagePort(channel.port1))))
  yield* Effect.log(`connected over MessagePort using ACP v${negotiated.version}`)
}))

if (import.meta.main) {
  BunRuntime.runMain(program)
}
