import { expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import { fromMessagePort } from "../examples/custom-transport.ts"
import { failure } from "./support/failure.ts"

it.effect("MessagePort sample reports overflow and stops sending", () => Effect.scoped(Effect.gen(function*() {
  const channel = new MessageChannel()
  yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
  const transport = yield* fromMessagePort(channel.port1)
  const callback = channel.port1.onmessage
  if (!callback) throw new Error("missing message listener")
  const frame = JSON.stringify({ jsonrpc: "2.0", method: "_ping" })
  // Bun and undici declare different source unions for this same runtime event.
  for (let n = 0; n < 257; n++) callback.call(channel.port1,
    new MessageEvent("message", { data: frame }) as Parameters<typeof callback>[0])
  expect(channel.port1.onmessage).toBeNull()
  expect(yield* failure(Stream.runCollect(transport.incoming))).toMatchObject({ reason: "Read" })
  expect(yield* failure(transport.send("{}"))).toMatchObject({ reason: "Closed" })
})))

it.effect("MessagePort sample preserves frames below capacity", () => Effect.scoped(Effect.gen(function*() {
  const channel = new MessageChannel()
  yield* Effect.addFinalizer(() => Effect.sync(() => channel.port2.close()))
  const transport = yield* fromMessagePort(channel.port1)
  const callback = channel.port1.onmessage
  if (!callback) throw new Error("missing message listener")
  for (const data of ["first", "second"]) callback.call(channel.port1,
    new MessageEvent("message", { data }) as Parameters<typeof callback>[0])
  expect(yield* Stream.runCollect(Stream.take(transport.incoming, 2))).toEqual(["first", "second"])
})))
