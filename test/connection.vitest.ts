import { AcpTransport } from "../src/AcpTransport.ts"
import { field } from "./support/field.ts"
import { failure } from "./support/failure.ts"
import { describe, expect, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import * as AcpConnection from "../src/AcpConnection.ts"
import { AcpRemoteError } from "../src/AcpError.ts"
import * as AcpSchema from "../src/AcpSchema.ts"
import * as V2 from "../src/protocol/v2/Schema.ts"
import * as InMemory from "../src/transport/InMemory.ts"
import { driver, type Driver } from "./support/driver.ts"

const run = <A, E>(effect: Effect.Effect<A, E, Scope.Scope>) => Effect.scoped(effect)

const pollNoFrame = (peer: Driver, ms = 50) => Effect.gen(function*() {
  const polling = yield* Effect.forkChild(peer.poll(ms))
  yield* Effect.yieldNow
  yield* TestClock.adjust(`${ms} millis`)
  return yield* Fiber.join(polling)
})

const Echo = AcpSchema.request("test/echo", Schema.Struct({ text: Schema.String }), Schema.Struct({ text: Schema.String }))
const Ping = AcpSchema.notification("test/ping", Schema.Struct({ n: Schema.Finite }))

const harness = (options?: AcpConnection.Options) =>
  Effect.gen(function*() {
    const scope = yield* Scope.Scope
    const pair = yield* InMemory.make({ capacity: 64 })
    const connectionScope = yield* Scope.fork(scope)
    const driverScope = yield* Scope.fork(scope)
    const connection = yield* Effect.flatMap(pair.left, (t) => AcpConnection.make(options).pipe(Effect.provideService(AcpTransport, t))).pipe(Scope.provide(connectionScope))
    const peer = yield* Effect.flatMap(pair.right, driver).pipe(Scope.provide(driverScope))
    return { connection, peer, connectionScope, driverScope }
  })

describe("review regressions", () => {
  it.effect("child interruption in a notification leaves later delivery and owner cleanup live", () =>
    run(Effect.gen(function*() {
      const seen: Array<string> = []
      const { connection, peer, connectionScope } = yield* harness({ handlers: {
        request: () => Effect.succeed({ ok: true }),
        notification: (method) => method === "_interrupt" ? Effect.gen(function*() {
          const worker = yield* Effect.forkChild(Effect.interrupt)
          return yield* Fiber.join(worker)
        }) : Effect.sync(() => { seen.push(method) })
      } })
      for (const method of ["_before", "_interrupt", "_after"]) yield* peer.send({ jsonrpc: "2.0", method })
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "_barrier" })
      expect(yield* peer.next).toMatchObject({ id: 1, result: { ok: true } })
      yield* connection.drainNotifications
      expect(seen).toEqual(["_before", "_after"])
      yield* connection.setHandlers({ request: () => Effect.succeed(null), notification: () => Effect.sync(() => { seen.push("replaced") }) })
      yield* peer.send({ jsonrpc: "2.0", method: "_later" })
      yield* peer.send({ jsonrpc: "2.0", id: 2, method: "_barrier" })
      yield* peer.next
      yield* connection.drainNotifications
      expect(seen).toEqual(["_before", "_after", "replaced"])
      yield* Scope.close(connectionScope, Exit.void)
      expect((yield* connection.closed).message).toBe("Connection closed")
    })))

  it.effect("invalid constructed remote error codes never reach the wire", () =>
    run(Effect.gen(function*() {
      const { peer } = yield* harness({ handlers: { request: (_method, params) =>
        Effect.fail(new AcpRemoteError({ code: Number(field(params, "code")), message: "custom error", data: { visible: true } }))
      } })
      for (const [id, code] of [[1, 1.5], [2, -32001], [3, 42]] as const) {
        yield* peer.send({ jsonrpc: "2.0", id, method: "_error", params: { code } })
        expect(yield* peer.next).toEqual({ jsonrpc: "2.0", id, error: code === 1.5
          ? { code: -32603, message: "Internal error" }
          : { code, message: "custom error", data: { visible: true } } })
      }
    })))

  it.effect("a committed result survives later handler failure", () =>
    run(Effect.gen(function*() {
      const { peer } = yield* harness({ handlers: { request: (_method, _params, context) =>
        context.commitResult(Effect.succeed({ messageId: "accepted" })).pipe(
          Effect.andThen(Effect.fail(new AcpRemoteError({ code: -32603, message: "storage failed" })))
        )
      } })
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "_insert" })
      expect(yield* peer.next).toEqual({ jsonrpc: "2.0", id: 1, result: { messageId: "accepted" } })
    })))

  it.effect("cancellation before the acceptance boundary interrupts preparation", () =>
    run(Effect.gen(function*() {
      const started = yield* Deferred.make<void>()
      const stopped = yield* Deferred.make<void>()
      const { peer } = yield* harness({ handlers: { request: (_method, _params, context) =>
        Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Effect.never), Effect.onInterrupt(() => Deferred.succeed(stopped, undefined)),
          Effect.andThen(context.commitResult(Effect.succeed({ messageId: "never-inserted" })))
        )
      } })
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "_insert" })
      yield* Deferred.await(started)
      yield* peer.send({ jsonrpc: "2.0", method: "$/cancel_request", params: { requestId: 1 } })
      expect(yield* peer.next).toMatchObject({ id: 1, error: { code: -32800 } })
      yield* Deferred.await(stopped)
    })))
})

describe("in-memory transport", () => {
  it.effect("ordered duplex exchange", () =>
    run(Effect.gen(function*() {
      const [a, b] = yield* InMemory.makePair({ capacity: 2 })
      const fromA = yield* Effect.forkChild(Stream.runCollect(Stream.take(b.incoming, 5)))
      const fromB = yield* Effect.forkChild(Stream.runCollect(Stream.take(a.incoming, 5)))
      yield* Effect.forEach(["1", "2", "3", "4", "5"], (f) => Effect.all([a.send(`a${f}`), b.send(`b${f}`)]), {
        concurrency: 1
      })
      expect(yield* Fiber.join(fromA)).toEqual(["a1", "a2", "a3", "a4", "a5"])
      expect(yield* Fiber.join(fromB)).toEqual(["b1", "b2", "b3", "b4", "b5"])
    })))

  it.effect("closing one end releases blocked writers and readers", () =>
    run(Effect.gen(function*() {
      const scope = yield* Scope.Scope
      const pair = yield* InMemory.make({ capacity: 1 })
      const leftScope = yield* Scope.fork(scope)
      const rightScope = yield* Scope.fork(scope)
      const left = yield* Scope.provide(pair.left, leftScope)
      const right = yield* Scope.provide(pair.right, rightScope)
      yield* left.send("buffered")
      const blockedWriter = yield* Effect.forkChild(left.send("blocked"))
      const blockedReader = yield* Effect.forkChild(Stream.runCollect(left.incoming))
      yield* Effect.yieldNow
      yield* Scope.close(rightScope, Exit.void)
      const writeExit = yield* Fiber.await(blockedWriter)
      expect(Exit.isFailure(writeExit) && Option.getOrUndefined(Cause.findErrorOption(writeExit.cause))).toMatchObject({
        _tag: "AcpTransportError", reason: "Closed"
      })
      expect(yield* Fiber.join(blockedReader)).toEqual([])
      expect(Exit.isFailure(yield* Effect.exit(left.send("after")))).toBe(true)
      void right
    })))
})

describe("envelopes and dispatch", () => {
  const withEcho = AcpConnection.handlers([
    AcpConnection.onRequest(Echo, ({ text }) => Effect.succeed({ text: text.toUpperCase() }))
  ])

  it.effect("parse, invalid request, method not found, and invalid params errors", () =>
    run(Effect.gen(function*() {
      const { peer } = yield* harness({ handlers: withEcho })
      yield* peer.send("{not json")
      expect(yield* peer.next).toEqual({ jsonrpc: "2.0", id: null, error: { code: -32700, message: "Parse error" } })
      yield* peer.send({ jsonrpc: "2.0", id: 3, method: 1 })
      expect(field(yield* peer.next, "error.code")).toBe(-32600)
      yield* peer.send({ foo: 1 })
      expect(yield* peer.next).toEqual({ jsonrpc: "2.0", id: null, error: { code: -32600, message: "Invalid Request" } })
      yield* peer.send({ jsonrpc: "1.0", id: 4, method: "test/echo", params: { text: "x" } })
      expect(yield* peer.next).toMatchObject({ id: null, error: { code: -32600 } })
      yield* peer.send({ jsonrpc: "2.0", id: 5, method: "nope", params: {} })
      expect(yield* peer.next).toEqual({ jsonrpc: "2.0", id: 5, error: { code: -32601, message: "Method not found" } })
      yield* peer.send({ jsonrpc: "2.0", id: "s", method: "test/echo", params: { text: 1 } })
      expect(yield* peer.next).toMatchObject({ jsonrpc: "2.0", id: "s", error: { code: -32602 } })
      yield* peer.send({ jsonrpc: "2.0", id: 6, method: "test/echo", params: { text: "hi" } })
      expect(yield* peer.next).toEqual({ jsonrpc: "2.0", id: 6, result: { text: "HI" } })
    })))

  it.effect("notifications are dispatched in order and never answered", () =>
    run(Effect.gen(function*() {
      const seen: Array<number> = []
      const processed = yield* Deferred.make<void>()
      const { peer } = yield* harness({
        handlers: AcpConnection.handlers([AcpConnection.onNotification(Ping, ({ n }) =>
          Effect.sync(() => seen.push(n)).pipe(Effect.andThen(n === 2 ? Deferred.succeed(processed, undefined) : Effect.void)))])
      })
      yield* peer.send({ jsonrpc: "2.0", method: "test/ping", params: { n: 1 } })
      yield* peer.send({ jsonrpc: "2.0", method: "test/ping", params: { n: "bad" } })
      yield* peer.send({ jsonrpc: "2.0", method: "unknown/notification", params: {} })
      yield* peer.send({ jsonrpc: "2.0", method: "test/ping", params: { n: 2 } })
      yield* Deferred.await(processed)
      expect(yield* pollNoFrame(peer, 100)).toEqual(Option.none())
      expect(seen).toEqual([1, 2])
    })))

  it.effect("handler failures map to error responses without leaking defects", () =>
    run(Effect.gen(function*() {
      const { peer } = yield* harness({
        handlers: {
          request: (method) =>
            method === "remote"
              ? Effect.fail(new AcpRemoteError({ code: -32000, message: "Authentication required", data: { hint: 1 } }))
              : Effect.die(new Error("secret"))
        }
      })
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "remote" })
      expect(yield* peer.next).toEqual({
        jsonrpc: "2.0",
        id: 1,
        error: { code: -32000, message: "Authentication required", data: { hint: 1 } }
      })
      yield* peer.send({ jsonrpc: "2.0", id: 2, method: "boom" })
      const response = yield* peer.next
      expect(response).toEqual({ jsonrpc: "2.0", id: 2, error: { code: -32603, message: "Internal error" } })
    })))

  it.effect("incoming messages wait for handlers to be installed", () =>
    run(Effect.gen(function*() {
      const { connection, peer } = yield* harness()
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "test/echo", params: { text: "a" } })
      expect(yield* pollNoFrame(peer, 50)).toEqual(Option.none())
      yield* connection.setHandlers(withEcho)
      expect(yield* peer.next).toEqual({ jsonrpc: "2.0", id: 1, result: { text: "A" } })
    })))
})

describe("bidirectional requests", () => {
  it.effect("a reverse request with a colliding id resolves independently", () =>
    run(Effect.gen(function*() {
      const { connection, peer } = yield* harness({
        handlers: AcpConnection.handlers([
          AcpConnection.onRequest(
            V2.clientMethods["session/request_permission"],
            () => Effect.succeed({ outcome: { outcome: "selected", optionId: "allow" } })
          )
        ])
      })
      const prompt = yield* Effect.forkChild(
        connection.request(V2.agentMethods["session/prompt"], { sessionId: "s", prompt: [{ type: "text", text: "hi" }] })
      )
      const sent = yield* peer.next
      expect(sent).toMatchObject({ jsonrpc: "2.0", id: 0, method: "session/prompt" })
      yield* peer.send({
        jsonrpc: "2.0",
        id: field(sent, "id"),
        method: "session/request_permission",
        params: { sessionId: "s", title: "Run?", options: [{ optionId: "allow", name: "Allow", kind: "allow_once" }] }
      })
      expect(yield* peer.next).toEqual({ jsonrpc: "2.0", id: 0, result: { outcome: { outcome: "selected", optionId: "allow" } } })
      yield* peer.send({ jsonrpc: "2.0", id: field(sent, "id"), result: { messageId: "m-1" } })
      expect(yield* Fiber.join(prompt)).toEqual({ messageId: "m-1" })
    })))

  it.effect("a handler can call back into the peer without deadlock", () =>
    run(Effect.gen(function*() {
      const conn = yield* Deferred.make<AcpConnection.Service>()
      const { connection, peer } = yield* harness({
        handlers: AcpConnection.handlers([
          AcpConnection.onRequest(Echo, (params) =>
            Deferred.await(conn).pipe(
              Effect.flatMap((c) => c.request(Echo, params)),
              Effect.mapError((e) => new AcpRemoteError({ code: -32603, message: e._tag }))
            ))
        ])
      })
      yield* Deferred.succeed(conn, connection)
      yield* peer.send({ jsonrpc: "2.0", id: 7, method: "test/echo", params: { text: "nested" } })
      const nested = yield* peer.next
      expect(nested).toMatchObject({ method: "test/echo", params: { text: "nested" } })
      yield* peer.send({ jsonrpc: "2.0", id: field(nested, "id"), result: { text: "inner" } })
      expect(yield* peer.next).toEqual({ jsonrpc: "2.0", id: 7, result: { text: "inner" } })
    })))
})

describe("batches", () => {
  const handlers = AcpConnection.handlers([
    AcpConnection.onRequest(Echo, ({ text }) => Effect.succeed({ text })),
    AcpConnection.onNotification(Ping, () => Effect.void)
  ])

  it.effect("empty batch gets a single invalid-request error", () =>
    run(Effect.gen(function*() {
      const { peer } = yield* harness({ handlers })
      yield* peer.send("[]")
      expect(yield* peer.next).toEqual({ jsonrpc: "2.0", id: null, error: { code: -32600, message: "Invalid Request" } })
    })))

  it.effect("mixed batch answers requests and invalid entries only", () =>
    run(Effect.gen(function*() {
      const pinged: Array<number> = []
      const { peer } = yield* harness({
        handlers: AcpConnection.handlers([
          AcpConnection.onRequest(Echo, ({ text }) => Effect.succeed({ text })),
          AcpConnection.onNotification(Ping, ({ n }) => Effect.sync(() => pinged.push(n)))
        ])
      })
      yield* peer.send([
        { jsonrpc: "2.0", id: 1, method: "test/echo", params: { text: "a" } },
        { jsonrpc: "2.0", method: "test/ping", params: { n: 9 } },
        1
      ])
      const response = yield* peer.next
      expect(Array.isArray(response)).toBe(true)
      expect(response).toHaveLength(2)
      expect(response).toContainEqual({ jsonrpc: "2.0", id: 1, result: { text: "a" } })
      expect(response).toContainEqual({ jsonrpc: "2.0", id: null, error: { code: -32600, message: "Invalid Request" } })
      expect(pinged).toEqual([9])
    })))

  it.effect("a handler that throws synchronously still yields its batch entry", () =>
    run(Effect.gen(function*() {
      const { peer } = yield* harness({
        handlers: {
          request: (method) => {
            if (method === "throws") throw new Error("synchronous")
            return Effect.succeed({ ok: true })
          }
        }
      })
      yield* peer.send([
        { jsonrpc: "2.0", id: 1, method: "fine" },
        { jsonrpc: "2.0", id: 2, method: "throws" }
      ])
      const response = yield* peer.next
      expect(response).toHaveLength(2)
      expect(response).toContainEqual({ jsonrpc: "2.0", id: 1, result: { ok: true } })
      expect(response).toContainEqual({ jsonrpc: "2.0", id: 2, error: { code: -32603, message: "Internal error" } })
    })))

  it.effect("all-notification batch gets no response", () =>
    run(Effect.gen(function*() {
      const processed = yield* Deferred.make<void>()
      let handled = 0
      const { peer } = yield* harness({ handlers: AcpConnection.handlers([
        AcpConnection.onRequest(Echo, ({ text }) => Effect.succeed({ text })),
        AcpConnection.onNotification(Ping, () => Effect.sync(() => ++handled).pipe(
          Effect.flatMap((count) => count === 2 ? Deferred.succeed(processed, undefined) : Effect.void)
        ))
      ]) })
      yield* peer.send([{ jsonrpc: "2.0", method: "test/ping", params: { n: 1 } }, {
        jsonrpc: "2.0",
        method: "test/ping",
        params: { n: 2 }
      }])
      yield* Deferred.await(processed)
      expect(handled).toBe(2)
      expect(yield* pollNoFrame(peer, 100)).toEqual(Option.none())
    })))

  it.effect("out-of-order batched responses correlate by id", () =>
    run(Effect.gen(function*() {
      const { connection, peer } = yield* harness({ handlers })
      const first = yield* connection.send("test/echo", { text: "first" })
      const second = yield* connection.send("test/echo", { text: "second" })
      yield* peer.next
      yield* peer.next
      yield* peer.send([
        { jsonrpc: "2.0", id: second.id, result: { text: "2" } },
        { jsonrpc: "2.0", id: first.id, error: { code: -32002, message: "Resource not found" } }
      ])
      expect(yield* second.response).toEqual({ text: "2" })
      const error = yield* failure(first.response)
      expect(error).toMatchObject({ _tag: "AcpRemoteError", code: -32002 })
      expect(yield* pollNoFrame(peer, 50)).toEqual(Option.none())
    })))
})

describe("cancellation, deadlines, capacity, and termination", () => {
  it.effect("explicit request cancellation is confirmed only by the remote response", () =>
    run(Effect.gen(function*() {
      const { connection, peer } = yield* harness({ handlers: {} })
      const pending = yield* connection.send("test/slow", {})
      yield* peer.next
      yield* connection.cancelRequest(pending.id)
      expect(yield* peer.next).toEqual({ jsonrpc: "2.0", method: "$/cancel_request", params: { requestId: pending.id } })
      expect(yield* connection.pendingRequests).toBe(1)
      yield* peer.send({ jsonrpc: "2.0", id: pending.id, error: { code: -32800, message: "Request cancelled" } })
      expect(yield* failure(pending.response)).toMatchObject({ _tag: "AcpRemoteError", code: -32800 })
    })))

  it.effect("incoming $/cancel_request interrupts the handler and answers -32800", () =>
    run(Effect.gen(function*() {
      const interrupted = yield* Deferred.make<void>()
      const { peer } = yield* harness({
        handlers: { request: () => Effect.never.pipe(Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined))) }
      })
      yield* peer.send({ jsonrpc: "2.0", id: 4, method: "anything", params: {} })
      yield* peer.send({ jsonrpc: "2.0", method: "$/cancel_request", params: { requestId: 4 } })
      expect(yield* peer.next).toEqual({ jsonrpc: "2.0", id: 4, error: { code: -32800, message: "Request cancelled" } })
      yield* Deferred.await(interrupted)
    })))

  it.effect("an interrupted handler answers once while the connection remains live", () =>
    run(Effect.gen(function*() {
      const started = yield* Deferred.make<void>()
      const { peer } = yield* harness({ handlers: { request: (_method, _params, { id }) =>
        id === 1 ? Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.interrupt)) : Effect.succeed({ ok: true })
      } })
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "interrupt" })
      yield* Deferred.await(started)
      expect(yield* peer.next).toMatchObject({ id: 1, error: { code: -32800 } })
      yield* peer.send({ jsonrpc: "2.0", id: 2, method: "healthy" })
      expect(yield* peer.next).toMatchObject({ id: 2, result: { ok: true } })
      expect(peer.received.map((frame) => JSON.parse(frame)).filter((frame) => frame.id === 1)).toHaveLength(1)
    })))

  it.effect("a defect combined with interruption is an Internal error", () =>
    run(Effect.gen(function*() {
      const { peer } = yield* harness({ handlers: { request: () => Effect.failCause(Cause.fromReasons([
        Cause.makeInterruptReason(0), Cause.makeDieReason(new Error("private defect"))
      ])) } })
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "broken" })
      expect(yield* peer.next).toEqual({ jsonrpc: "2.0", id: 1, error: { code: -32603, message: "Internal error" } })
    })))

  it.effect("an interrupted batch member retains the other response", () =>
    run(Effect.gen(function*() {
      const { peer } = yield* harness({ handlers: { request: (_method, _params, { id }) =>
        id === 1 ? Effect.interrupt : Effect.succeed({ ok: true })
      } })
      yield* peer.send([
        { jsonrpc: "2.0", id: 1, method: "interrupt" },
        { jsonrpc: "2.0", id: 2, method: "healthy" }
      ])
      const response = yield* peer.next
      expect(Array.isArray(response) ? response : []).toEqual(expect.arrayContaining([
        expect.objectContaining({ id: 1, error: expect.objectContaining({ code: -32800 }) }),
        expect.objectContaining({ id: 2, result: { ok: true } })
      ]))
      expect(Array.isArray(response) ? response : []).toHaveLength(2)
    })))

  it.effect("a deadline fails locally without fabricating remote cancellation", () =>
    run(Effect.gen(function*() {
      const { connection, peer } = yield* harness({ handlers: {} })
      const call = yield* Effect.forkChild(connection.request(Echo, { text: "x" }, { timeout: "1 second" }))
      yield* peer.next
      yield* TestClock.adjust("2 seconds")
      expect(yield* failure(Fiber.join(call))).toMatchObject({ _tag: "AcpTimeoutError", method: "test/echo", requestId: 0 })
      expect(yield* connection.pendingRequests).toBe(1)
      expect(peer.received).toHaveLength(1)
      yield* peer.send({ jsonrpc: "2.0", id: 0, result: { text: "late" } })
      yield* peer.send({ jsonrpc: "2.0", id: "barrier", method: "missing" })
      expect(yield* peer.next).toMatchObject({ id: "barrier", error: { code: -32601 } })
      expect(yield* connection.pendingRequests).toBe(0)
    }).pipe(Effect.provide(TestClock.layer()))))

  it.effect("interrupting a wait keeps correlation and sends nothing", () =>
    run(Effect.gen(function*() {
      const { connection, peer } = yield* harness({ handlers: {} })
      const call = yield* Effect.forkChild(connection.request(Echo, { text: "x" }))
      yield* peer.next
      yield* Fiber.interrupt(call)
      expect(yield* connection.pendingRequests).toBe(1)
      expect(yield* pollNoFrame(peer, 50)).toEqual(Option.none())
    })))

  it.effect("pending-request capacity rejects admission before sending", () =>
    run(Effect.gen(function*() {
      const { connection, peer } = yield* harness({ handlers: {}, maxPendingRequests: 2 })
      yield* connection.send("a")
      yield* connection.send("b")
      expect(yield* failure(connection.send("c"))).toMatchObject({ _tag: "AcpCapacityError", limit: 2 })
      yield* peer.next
      yield* peer.next
      expect(yield* pollNoFrame(peer, 50)).toEqual(Option.none())
    })))

  it.effect("incoming capacity overflow terminates explicitly", () =>
    run(Effect.gen(function*() {
      const { connection, peer } = yield* harness({ handlers: { request: () => Effect.never }, maxIncomingRequests: 2 })
      for (const id of [1, 2, 3]) yield* peer.send({ jsonrpc: "2.0", id, method: "x" })
      expect((yield* connection.closed).message).toContain("capacity")
    })))

  it.effect("nothing is admitted or dispatched after termination", () =>
    run(Effect.gen(function*() {
      const executed: Array<import("../src/AcpSchema.ts").RequestId> = []
      const { connection, peer } = yield* harness({
        handlers: {
          request: (_method, _params, { id }) =>
            id === "late" ? Effect.sync(() => executed.push(id)) : Effect.never
        },
        maxIncomingRequests: 1
      })
      for (const id of [1, 2]) yield* peer.send({ jsonrpc: "2.0", id, method: "x" })
      yield* connection.closed
      yield* peer.send({ jsonrpc: "2.0", id: "late", method: "x" }).pipe(Effect.ignore)
      yield* Effect.yieldNow
      expect(executed).toEqual([])
      expect(peer.received).toEqual([])
    })))

  it.effect("a deadline also covers a send blocked on backpressure", () =>
    run(Effect.gen(function*() {
      const pair = yield* InMemory.make({ capacity: 1 })
      const transport = yield* pair.left
      yield* pair.right // never read
      const connection = yield* AcpConnection.make({ handlers: {} }).pipe(Effect.provideService(AcpTransport, transport))
      yield* connection.send("fills-the-buffer")
      const call = yield* Effect.forkChild(connection.request(Echo, { text: "x" }, { timeout: "10 millis" }))
      yield* TestClock.adjust("10 millis")
      expect(yield* failure(Fiber.join(call))).toMatchObject({ _tag: "AcpTimeoutError", method: "test/echo", requestId: null })
      expect(yield* connection.pendingRequests).toBe(1)
    })))

  for (const [cause, trigger] of [
    ["remote EOF", "eof"],
    ["capacity overflow", "overflow"]
  ] as const) {
    it.effect(`a running notification handler is interrupted on termination by ${cause}`, () =>
      run(Effect.gen(function*() {
        const started = yield* Deferred.make<void>()
        const finalized = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        let sideEffect = false
        const { connection, peer, driverScope } = yield* harness({
          maxIncomingRequests: 1,
          handlers: {
            request: () => Effect.never,
            notification: () =>
              Deferred.succeed(started, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.andThen(Effect.sync(() => {
                  sideEffect = true
                })),
                Effect.onInterrupt(() => Deferred.succeed(finalized, undefined))
              )
          }
        })
        yield* peer.send({ jsonrpc: "2.0", method: "test/ping", params: { n: 1 } })
        yield* Deferred.await(started)
        if (trigger === "eof") yield* Scope.close(driverScope, Exit.void)
        else for (const id of [1, 2]) yield* peer.send({ jsonrpc: "2.0", id, method: "x" })
        yield* connection.closed
        yield* Deferred.await(finalized).pipe(Effect.timeoutOrElse({ duration: "1 second", orElse: () => Effect.die("handler not interrupted") }))
        yield* Deferred.succeed(release, undefined)
        expect(sideEffect).toBe(false)
      })))
  }

  it.effect("transport closure fails pending calls and later calls", () =>
    run(Effect.gen(function*() {
      const { connection, peer, driverScope } = yield* harness({ handlers: {} })
      const call = yield* Effect.forkChild(connection.request(Echo, { text: "x" }))
      yield* peer.next
      yield* Scope.close(driverScope, Exit.void)
      expect(yield* failure(Fiber.join(call))).toMatchObject({ _tag: "AcpConnectionClosed" })
      expect((yield* connection.closed)._tag).toBe("AcpConnectionClosed")
      expect(yield* failure(connection.request(Echo, { text: "y" }))).toMatchObject({ _tag: "AcpConnectionClosed" })
      expect(yield* connection.pendingRequests).toBe(0)
    })))

  it.effect("closing the connection scope settles waits and releases handler fibers", () =>
    run(Effect.gen(function*() {
      const released = yield* Deferred.make<void>()
      const { connection, peer, connectionScope } = yield* harness({
        handlers: { request: () => Effect.never.pipe(Effect.onInterrupt(() => Deferred.succeed(released, undefined))) }
      })
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "x" })
      const call = yield* Effect.forkChild(connection.request(Echo, { text: "x" }))
      yield* peer.next
      yield* Scope.close(connectionScope, Exit.void)
      expect(yield* failure(Fiber.join(call))).toMatchObject({ _tag: "AcpConnectionClosed" })
      yield* Deferred.await(released)
    })))
})

describe("JSON encoding failures", () => {
  it.effect("invalid outgoing data fails with AcpProtocolError and releases pending capacity", () => run(Effect.gen(function*() {
    const { connection, peer } = yield* harness({ maxPendingRequests: 1, handlers: {} })
    const circular: Record<string, unknown> = {}
    circular.self = circular
    expect(yield* failure(connection.send("_circular", circular))).toMatchObject({ _tag: "AcpProtocolError" })
    expect(yield* connection.pendingRequests).toBe(0)
    expect(yield* failure(connection.notifyRaw("_bigint", { value: 1n }))).toMatchObject({ _tag: "AcpProtocolError" })
    expect(yield* pollNoFrame(peer)).toEqual(Option.none())
    const sent = yield* connection.send("_valid", {})
    expect(yield* peer.next).toMatchObject({ id: sent.id, method: "_valid" })
    yield* peer.send({ jsonrpc: "2.0", id: sent.id, result: "ok" })
    expect(yield* sent.response).toBe("ok")
  })))

  it.effect("non-serializable handler results produce an Internal error without losing batch siblings", () => run(Effect.gen(function*() {
    const circular: Record<string, unknown> = {}
    circular.self = circular
    const { peer } = yield* harness({ handlers: { request: (method) => Effect.succeed(method === "_bad" ? circular : "ok") } })
    yield* peer.send([
      { jsonrpc: "2.0", id: 1, method: "_bad" },
      { jsonrpc: "2.0", id: 2, method: "_good" }
    ])
    expect(yield* peer.next).toEqual([
      { jsonrpc: "2.0", id: 1, error: { code: -32603, message: "Internal error" } },
      { jsonrpc: "2.0", id: 2, result: "ok" }
    ])
  })))
})

it.effect("draining notifications waits for handlers without blocking response delivery", () => run(Effect.gen(function*() {
  const entered = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const { connection, peer } = yield* harness({ handlers: {
    notification: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
  } })
  const request = yield* connection.send("test/echo", {})
  yield* peer.next
  yield* peer.send({ jsonrpc: "2.0", method: "test/ping", params: {} })
  yield* Deferred.await(entered)
  const drain = yield* Effect.forkChild(connection.drainNotifications)
  yield* peer.send({ jsonrpc: "2.0", id: request.id, result: "answered" })
  expect(yield* request.response).toBe("answered")
  expect(drain.pollUnsafe()).toBeUndefined()
  yield* Deferred.succeed(release, undefined)
  yield* Fiber.join(drain)
})))
