import { AcpTransport } from "../src/AcpTransport.ts"
import { field } from "./support/field.ts"
import * as Json from "../src/internal/json.ts"
/**
 * AcpAgent behavior, checked from the wire with the scripted `driver` rather
 * than with the library's own client: the point is what the agent puts on the
 * socket, not that it agrees with itself.
 */
import { describe, expect, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Context from "effect/Context"
import * as Clock from "effect/Clock"
import * as DateTime from "effect/DateTime"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Logger from "effect/Logger"
import * as Scope from "effect/Scope"
import * as Sink from "effect/Sink"
import * as Stdio from "effect/Stdio"
import * as Stream from "effect/Stream"
import * as AcpAgent from "../src/AcpAgent.ts"
import * as Store from "../src/agent/Store.ts"
import * as Schema from "effect/Schema"
import * as V1 from "../src/protocol/v1/Schema.ts"
import * as V2 from "../src/protocol/v2/Schema.ts"
import * as InMemory from "../src/transport/InMemory.ts"
import * as ProcessStdio from "../src/transport/ProcessStdio.ts"
import { driver } from "./support/driver.ts"
import { singleFailureOf } from "./support/failure.ts"

const run = <A, E>(effect: Effect.Effect<A, E, Scope.Scope | Store.Store>) =>
  Effect.scoped(effect).pipe(Effect.provide(Store.layer))

const agentFailure = (exit: Exit.Exit<unknown, unknown>) => {
  if (Exit.isSuccess(exit)) throw new Error("Expected an agent failure")
  const error = singleFailureOf(exit)
  if (!(error instanceof AcpAgent.AcpAgentError)) {
    throw new Error(Cause.pretty(exit.cause))
  }
  return error
}

it("rejects a typed failure mixed with a defect", () => {
  const exit = Effect.runSync(Effect.exit(
    Effect.fail("expected").pipe(Effect.ensuring(Effect.die("probe defect")))
  ))
  expect(Exit.isFailure(exit) && exit.cause.reasons.map((reason) => reason._tag)).toEqual(["Fail", "Die"])
  expect(() => singleFailureOf(exit)).toThrow("Expected exactly one typed failure")
})

/** Minimal handlers any test can start from. */
const baseOptions = (): AcpAgent.Options => ({
  info: { name: "test-agent", version: "0.0.1" },
  versions: [2, 1],
  session: { create: () => Effect.succeed({ sessionId: "s-1" }) },
  prompt: {
    insert: () => Effect.succeed({ messageId: "m-1" }),
    execute: ({ emit }) => Effect.as(emit.agentChunk("m-1", { type: "text", text: "hi" }), "end_turn")
  }
})

/** Serves `agent` on one end of an in-memory pair and drives the other. */
const connect = (agent: AcpAgent.AcpAgent) =>
  Effect.gen(function*() {
    const [left, right] = yield* InMemory.makePair()
    yield* Effect.forkScoped(Effect.ignore(agent.serve.pipe(Effect.provideService(AcpTransport, right))))
    return yield* driver(left)
  })

const initialize = (version: 1 | 2, capabilities?: unknown) =>
  version === 2
    ? {
      jsonrpc: "2.0",
      id: 0,
      method: "initialize",
      params: { protocolVersion: 2, info: { name: "test-client", version: "1.0.0" }, capabilities }
    }
    : {
      jsonrpc: "2.0",
      id: 0,
      method: "initialize",
      params: { protocolVersion: 1, clientInfo: { name: "test-client", version: "1.0.0" } }
    }

describe("capability validation", () => {
  it("safe construction succeeds for a valid configuration", () => {
    const options = baseOptions()
    const agent = Effect.runSync(AcpAgent.make(options))
    expect(agent.options).toBe(options)
  })

  it("safe construction exposes the missing handler as a typed failure", () => {
    const invalid = { ...baseOptions(), prompt: { insert: baseOptions().prompt.insert } } as AcpAgent.Options
    const exit = Effect.runSync(Effect.exit(AcpAgent.make(invalid)))
    expect(exit._tag).toBe("Failure")
    if (exit._tag === "Failure") {
      expect(exit.cause.reasons).toEqual([
        expect.objectContaining({ _tag: "Fail", error: expect.objectContaining({
          _tag: "AcpAgentConfigError", missing: "prompt.execute"
        }) })
      ])
    }
  })

  it("safe construction keeps unexpected validation defects as defects", () => {
    const defect = new Error("broken options getter")
    const options = baseOptions()
    Object.defineProperty(options, "auth", { get: () => { throw defect } })
    const exit = Effect.runSync(Effect.exit(AcpAgent.make(options)))
    expect(exit._tag).toBe("Failure")
    if (exit._tag === "Failure") {
      expect(exit.cause.reasons).toEqual([expect.objectContaining({ _tag: "Die", defect })])
    }
  })

  it("unsafe construction throws the original configuration error", () => {
    expect(() => AcpAgent.makeUnsafe({ ...baseOptions(), session: {} as never }))
      .toThrow(AcpAgent.AcpAgentConfigError)
  })

  it("advertising sessions without a create handler fails before serving", () => {
    // Deliberately bypass the public type to exercise validation for JS callers.
    expect(() =>
      AcpAgent.makeUnsafe({ ...baseOptions(), session: {} as never })
    ).toThrow(/session.create/)
  })

  it("advertising authentication methods without a logout handler fails", () => {
    expect(() =>
      AcpAgent.makeUnsafe({
        ...baseOptions(),
        auth: { methods: [{ methodId: "token", name: "Token" }], login: () => Effect.void }
      })
    ).toThrow(/auth.logout/)
  })

  it("advertising authentication methods without a login handler fails", () => {
    expect(() =>
      AcpAgent.makeUnsafe({
        ...baseOptions(),
        auth: { methods: [{ methodId: "token", name: "Token" }], logout: () => Effect.void }
      })
    ).toThrow(/auth.login/)
  })

  it("an empty method list needs no login or logout", () => {
    expect(() => AcpAgent.makeUnsafe({ ...baseOptions(), auth: { methods: [] } })).not.toThrow()
  })

  it.effect("only surfaces with installed handlers are advertised", () =>
    run(Effect.gen(function*() {
      const agent = AcpAgent.makeUnsafe({ ...baseOptions(), session: { create: () => Effect.succeed({ sessionId: "s-1" }) } })
      const peer = yield* connect(agent)
      yield* peer.send(initialize(2))
      const response = yield* peer.next
      // No delete handler, so no delete capability.
      expect(field(response, "result.capabilities.session.delete")).toBeUndefined()
      expect(field(response, "result.info.name")).toBe("test-agent")
    })))
})

describe("version negotiation", () => {
  it.effect("a v1 client gets the v1 advertisement shape", () =>
    run(Effect.gen(function*() {
      const peer = yield* connect(AcpAgent.makeUnsafe(baseOptions()))
      yield* peer.send(initialize(1))
      const response = yield* peer.next
      expect(field(response, "result.protocolVersion")).toBe(1)
      expect(field(response, "result.agentInfo.name")).toBe("test-agent")
      expect(field(response, "result.capabilities")).toBeUndefined()
    })))

  it.effect("a version outside the enabled set is answered with our own, not accepted", () =>
    run(Effect.gen(function*() {
      const agent = AcpAgent.makeUnsafe({ ...baseOptions(), versions: [1] })
      const peer = yield* connect(agent)
      yield* peer.send({ jsonrpc: "2.0", id: 0, method: "initialize", params: { protocolVersion: 2, info: { name: "test", version: "1" } } })
      const response = yield* peer.next
      expect(field(response, "result.protocolVersion")).toBe(1)
    })))

  it.effect("requests before initialize are rejected", () =>
    run(Effect.gen(function*() {
      const peer = yield* connect(AcpAgent.makeUnsafe(baseOptions()))
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp" } })
      const response = yield* peer.next
      expect(field(response, "error.message")).toMatch(/Not initialized/)
    })))
})

describe("prompt insertion and execution", () => {
  it.effect("v2 answers with the inserted message id and keeps emitting afterwards", () =>
    run(Effect.gen(function*() {
      const peer = yield* connect(AcpAgent.makeUnsafe(baseOptions()))
      yield* peer.send(initialize(2))
      yield* peer.next
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp" } })
      const created = yield* peer.next
      yield* peer.send({
        jsonrpc: "2.0",
        id: 2,
        method: "session/prompt",
        params: { sessionId: field(created, "result.sessionId"), prompt: [{ type: "text", text: "hey" }] }
      })

      // Everything until the response, then everything after it.
      const frames: Array<unknown> = []
      for (let i = 0; i < 4; i++) frames.push(yield* peer.next)
      const response = frames.find((frame) => field(frame, "id") === 2)
      expect(field(response, "result.messageId")).toBe("m-1")
      expect(field(response, "result.stopReason")).toBeUndefined()

      const updates = frames.filter((frame) => field(frame, "method") === "session/update")
      const kinds = updates.map((frame) => field(frame, "params.update.sessionUpdate"))
      expect(kinds).toContain("agent_message_chunk")
      // The turn ends with an idle state update, not with the response.
      const idle = updates.find((frame) => field(frame, "params.update.state") === "idle")
      expect(field(idle, "params.update.stopReason")).toBe("end_turn")
      expect(frames.indexOf(response)).toBeLessThan(frames.indexOf(idle))
    })))

  it.effect("a prompt whose insertion fails produces no acknowledgement", () =>
    run(Effect.gen(function*() {
      let executed = false
      const agent = AcpAgent.makeUnsafe({
        ...baseOptions(),
        prompt: {
          insert: () => Effect.fail(new AcpAgent.AcpAgentError({ message: "cannot insert" })),
          execute: () =>
            Effect.sync(() => {
              executed = true
              return "end_turn" as const
            })
        }
      })
      const peer = yield* connect(agent)
      yield* peer.send(initialize(2))
      yield* peer.next
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp" } })
      yield* peer.next
      yield* peer.send({
        jsonrpc: "2.0",
        id: 2,
        method: "session/prompt",
        params: { sessionId: "s-1", prompt: [{ type: "text", text: "hey" }] }
      })
      const response = yield* peer.next
      expect(field(response, "error.message")).toBe("cannot insert")
      expect(field(response, "result")).toBeUndefined()
      expect(executed).toBe(false)
    })))

  it.effect("v1 waits for the turn and answers with a stop reason", () =>
    run(Effect.gen(function*() {
      const peer = yield* connect(AcpAgent.makeUnsafe(baseOptions()))
      yield* peer.send(initialize(1))
      yield* peer.next
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp", mcpServers: [] } })
      yield* peer.next
      yield* peer.send({
        jsonrpc: "2.0",
        id: 2,
        method: "session/prompt",
        params: { sessionId: "s-1", prompt: [{ type: "text", text: "hey" }] }
      })
      const update = yield* peer.next
      // v1 chunks carry no messageId: the version has no message identities.
      expect(field(update, "params.update.sessionUpdate")).toBe("agent_message_chunk")
      expect(field(update, "params.update.messageId")).toBeUndefined()
      const response = yield* peer.next
      expect(field(response, "id")).toBe(2)
      expect(field(response, "result.stopReason")).toBe("end_turn")
      expect(field(response, "result.messageId")).toBeUndefined()
    })))

  it.effect("prompting an unknown session is rejected", () =>
    run(Effect.gen(function*() {
      const peer = yield* connect(AcpAgent.makeUnsafe(baseOptions()))
      yield* peer.send(initialize(2))
      yield* peer.next
      yield* peer.send({
        jsonrpc: "2.0",
        id: 1,
        method: "session/prompt",
        params: { sessionId: "nope", prompt: [] }
      })
      const response = yield* peer.next
      expect(field(response, "error.code")).toBe(-32002)
      expect(field(response, "error.message")).toMatch(/Unknown session nope/)
    })))

  it.effect("a handler defect becomes a bare Internal error, leaking no cause", () => {
    const defect = new Error("database password is hunter2")
    const logs: Array<Logger.Options<unknown>> = []
    const logger = Logger.make<unknown, void>((entry) => { logs.push(entry) })
    return run(Effect.gen(function*() {
      const agent = AcpAgent.makeUnsafe({
        ...baseOptions(),
        session: { create: () => Effect.die(defect) }
      })
      const peer = yield* connect(agent)
      yield* peer.send(initialize(2))
      yield* peer.next
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp" } })
      const response = yield* peer.next
      expect(field(response, "error.code")).toBe(-32603)
      expect(field(response, "error.message")).toBe("Internal error")
      expect((yield* Json.encode(response))).not.toContain("hunter2")
      const diagnostic = logs.filter((entry) => Array.isArray(entry.message) && entry.message[0] === "Agent handler failed")
      expect(diagnostic).toHaveLength(1)
      expect(diagnostic[0]!.cause.reasons).toEqual([expect.objectContaining({ _tag: "Die", defect })])
    }).pipe(Effect.provide(Logger.layer([logger]))))
  })
})

it.effect("an expected prompt failure logs its original cause before returning refusal", () => {
  const failure = new AcpAgent.AcpAgentError({ code: -32603, message: "private prompt failure", data: null })
  const logs: Array<Logger.Options<unknown>> = []
  const logger = Logger.make<unknown, void>((entry) => { logs.push(entry) })
  return run(Effect.gen(function*() {
    const agent = AcpAgent.makeUnsafe({ ...baseOptions(), prompt: {
      insert: () => Effect.succeed({ messageId: "m-1" }), execute: () => Effect.fail(failure)
    } })
    const peer = yield* connect(agent)
    yield* peer.send(initialize(1)); yield* peer.next
    yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp", mcpServers: [] } }); yield* peer.next
    yield* peer.send({ jsonrpc: "2.0", id: 2, method: "session/prompt", params: { sessionId: "s-1", prompt: [] } })
    const response = yield* peer.next
    expect(field(response, "result.stopReason")).toBe("refusal")
    expect((yield* Json.encode(response))).not.toContain("private prompt failure")
    const diagnostic = logs.filter((entry) => Array.isArray(entry.message) && entry.message[0] === "Agent execution failed")
    expect(diagnostic).toHaveLength(1)
    expect(diagnostic[0]!.cause.reasons).toEqual([expect.objectContaining({ _tag: "Fail", error: failure })])
  }).pipe(Effect.provide(Logger.layer([logger]))))
})

it.effect("a private store failure is logged before becoming a bare wire error", () => {
  const failure = new Store.StoreError({ kind: "Corrupt", message: "private store secret", cause: new Error("storage stack") })
  const logs: Array<Logger.Options<unknown>> = []
  const logger = Logger.make<unknown, void>((entry) => { logs.push(entry) })
  return run(Effect.gen(function*() {
    const agent = AcpAgent.makeUnsafe({ ...baseOptions(), session: { create: () => Effect.fail(failure) } })
    const peer = yield* connect(agent)
    yield* peer.send(initialize(2)); yield* peer.next
    yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp" } })
    const response = yield* peer.next
    expect(field(response, "error")).toEqual({ code: -32603, message: "Internal error" })
    expect((yield* Json.encode(response))).not.toContain("private store secret")
    const diagnostic = logs.filter((entry) => Array.isArray(entry.message) && entry.message[0] === "Agent store operation failed")
    expect(diagnostic).toHaveLength(1)
    expect(diagnostic[0]!.cause.reasons).toEqual([expect.objectContaining({ _tag: "Fail", error: failure })])
  }).pipe(Effect.provide(Logger.layer([logger]))))
})

describe("client interactions", () => {
  it.effect("a permission request uses the negotiated shape and does not block other traffic", () =>
    run(Effect.gen(function*() {
      const agent = AcpAgent.makeUnsafe({
        ...baseOptions(),
        list: true,
        prompt: {
          insert: () => Effect.succeed({ messageId: "m-1" }),
          execute: ({ client }) =>
            Effect.map(
              client.requestPermission({
                title: "Edit file",
                options: [{ optionId: "allow", name: "Allow", kind: "allow_once" }]
              }),
              (choice) => choice === "allow" ? "end_turn" : "refusal"
            )
        }
      })
      const peer = yield* connect(agent)
      yield* peer.send(initialize(2))
      yield* peer.next
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp" } })
      yield* peer.next
      yield* peer.send({
        jsonrpc: "2.0",
        id: 2,
        method: "session/prompt",
        params: { sessionId: "s-1", prompt: [{ type: "text", text: "hey" }] }
      })

      const frames: Array<unknown> = []
      let permission: unknown
      while (permission === undefined) {
        const frame = yield* peer.next
        frames.push(frame)
        if (field(frame, "method") === "session/request_permission") permission = frame
      }
      expect(field(permission, "params.title")).toBe("Edit file")
      // The prompt was already acknowledged: the agent is not blocked on us.
      expect(frames.some((frame) => field(frame, "id") === 2 && field(frame, "result.messageId") === "m-1")).toBe(true)

      // Unrelated traffic is answered while the permission request is pending:
      // the waiting handler fiber does not hold up the connection.
      yield* peer.send({ jsonrpc: "2.0", id: 3, method: "session/list", params: {} })
      let other: unknown
      while (other === undefined) {
        const frame = yield* peer.next
        if (field(frame, "id") === 3) other = frame
      }
      expect(field(other, "result.sessions")).toHaveLength(1)

      yield* peer.send({
        jsonrpc: "2.0",
        id: field(permission, "id"),
        result: { outcome: { outcome: "selected", optionId: "allow" } }
      })
      let idle: unknown
      while (idle === undefined) {
        const frame = yield* peer.next
        if (field(frame, "params.update.state") === "idle") idle = frame
      }
      expect(field(idle, "params.update.stopReason")).toBe("end_turn")
    })))

  it.effect("an elicitation mode the client never advertised is rejected before sending", () =>
    run(Effect.gen(function*() {
      const attempted = Deferred.makeUnsafe<import("effect/Exit").Exit<import("../src/protocol/v2/Schema.ts").CreateElicitationResponse, AcpAgent.HandlerError>>()
      const agent = AcpAgent.makeUnsafe({
        ...baseOptions(),
        prompt: {
          insert: () => Effect.succeed({ messageId: "m-1" }),
          execute: ({ client }) =>
            client.elicit({ mode: "url", message: "open this" }).pipe(
              Effect.exit,
              Effect.flatMap((exit) => Deferred.succeed(attempted, exit)),
              Effect.as("end_turn")
            )
        }
      })
      const peer = yield* connect(agent)
      // Advertise form elicitation only.
      yield* peer.send(initialize(2, { elicitation: { form: {} } }))
      yield* peer.next
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp" } })
      yield* peer.next
      yield* peer.send({
        jsonrpc: "2.0",
        id: 2,
        method: "session/prompt",
        params: { sessionId: "s-1", prompt: [] }
      })
      const outcome = yield* Deferred.await(attempted)
      expect(agentFailure(outcome)).toMatchObject({
        _tag: "AcpAgentError", code: -32600, message: "Client does not support url elicitation"
      })
      // Nothing about elicitation reached the wire.
      expect(peer.received.some((frame) => frame.includes("elicitation/create"))).toBe(false)
    })))
})

describe("cancellation", () => {
  it.effect("does not admit a new prompt while cancellation is still finishing", () =>
    run(Effect.gen(function*() {
      const started = yield* Deferred.make<void>()
      const cancelling = yield* Deferred.make<void>()
      const finishCancellation = yield* Deferred.make<void>()
      let nextMessage = 0
      const agent = AcpAgent.makeUnsafe({
        ...baseOptions(),
        session: {
          create: () => Effect.succeed({ sessionId: "s-1" }),
          cancel: () => Deferred.succeed(cancelling, undefined).pipe(Effect.andThen(Deferred.await(finishCancellation)))
        },
        prompt: {
          insert: () => Effect.sync(() => ({ messageId: `m-${++nextMessage}` })),
          execute: () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never), Effect.as("end_turn"))
        }
      })
      const peer = yield* connect(agent)
      const responseFor = (id: number) => Effect.gen(function*() {
        while (true) {
          const frame = yield* peer.next
          if (field(frame, "id") === id) return frame
        }
      })
      yield* peer.send(initialize(2))
      yield* responseFor(0)
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp" } })
      yield* responseFor(1)
      const prompt = (id: number) => peer.send({
        jsonrpc: "2.0", id, method: "session/prompt",
        params: { sessionId: "s-1", prompt: [] }
      })
      yield* prompt(2)
      yield* responseFor(2)
      yield* Deferred.await(started)
      yield* peer.send({ jsonrpc: "2.0", method: "session/cancel", params: { sessionId: "s-1" } })
      yield* Deferred.await(cancelling)
      yield* prompt(3)
      const duringCancellation = yield* responseFor(3)
      yield* Deferred.succeed(finishCancellation, undefined)
      expect(field(duringCancellation, "error.message")).toBe("Session is busy")
      let idle: unknown
      while (idle === undefined) {
        const frame = yield* peer.next
        if (field(frame, "params.update.state") === "idle") idle = frame
      }
      yield* prompt(4)
      const afterCancellation = yield* responseFor(4)
      expect(field(afterCancellation, "result.messageId")).toBe("m-2")
    })))

  it.effect("cancelling a v2 session emits final updates before the cancelled idle state", () =>
    run(Effect.gen(function*() {
      const started = Deferred.makeUnsafe<void>()
      const agent = AcpAgent.makeUnsafe({
        ...baseOptions(),
        prompt: {
          insert: () => Effect.succeed({ messageId: "m-1" }),
          execute: ({ emit }) =>
            Effect.andThen(Deferred.succeed(started, undefined), Effect.never).pipe(
              // A final update drained on interruption, before completion.
              Effect.onInterrupt(() => Effect.ignore(emit.agentChunk("m-1", { type: "text", text: "partial" }))),
              Effect.as("end_turn")
            )
        }
      })
      const peer = yield* connect(agent)
      yield* peer.send(initialize(2))
      yield* peer.next
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp" } })
      yield* peer.next
      yield* peer.send({
        jsonrpc: "2.0",
        id: 2,
        method: "session/prompt",
        params: { sessionId: "s-1", prompt: [] }
      })
      yield* Deferred.await(started)
      yield* peer.send({ jsonrpc: "2.0", method: "session/cancel", params: { sessionId: "s-1" } })

      const seen: Array<unknown> = []
      let idle: unknown
      while (idle === undefined) {
        const frame = yield* peer.next
        seen.push(frame)
        if (field(frame, "params.update.state") === "idle") idle = frame
      }
      expect(field(idle, "params.update.stopReason")).toBe("cancelled")
      const partial = seen.find((frame) => field(frame, "params.update.content.text") === "partial")
      expect(partial).toBeDefined()
      expect(seen.indexOf(partial)).toBeLessThan(seen.indexOf(idle))
    })))
})

it.effect("serve shutdown closes a session registered after cleanup begins", () =>
  run(Effect.gen(function*() {
    const backing = yield* Store.InMemory
    const lateLookup = yield* Deferred.make<void>()
    const releaseLookup = yield* Deferred.make<void>()
    const firstStarted = yield* Deferred.make<void>()
    const firstClosing = yield* Deferred.make<void>()
    const releaseFirst = yield* Deferred.make<void>()
    const lateStarted = yield* Deferred.make<void>()
    const lateStopped = yield* Deferred.make<void>()
    const releaseLate = yield* Deferred.make<void>()
    const store = Store.Store.of({
      ...backing,
      get: (sessionId) => sessionId === "late"
        ? Deferred.succeed(lateLookup, undefined).pipe(
          Effect.andThen(Deferred.await(releaseLookup)),
          Effect.andThen(backing.get(sessionId))
        )
        : backing.get(sessionId)
    })
    let created = 0
    const agent = AcpAgent.makeUnsafe({ ...baseOptions(), session: {
      create: () => Effect.sync(() => ({ sessionId: ++created === 1 ? "first" : "late" }))
    }, prompt: {
      insert: ({ sessionId }) => Effect.succeed({ messageId: sessionId }),
      execute: ({ sessionId }) => sessionId === "first"
        ? Deferred.succeed(firstStarted, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.onInterrupt(() => Deferred.succeed(firstClosing, undefined).pipe(Effect.andThen(Deferred.await(releaseFirst)))),
          Effect.as("end_turn")
        )
        : Deferred.succeed(lateStarted, undefined).pipe(
          Effect.andThen(Deferred.await(releaseLate)),
          Effect.onInterrupt(() => Deferred.succeed(lateStopped, undefined)),
          Effect.as("end_turn")
        )
    } })
    const [left, right] = yield* InMemory.makePair()
    const serving = yield* Effect.forkScoped(agent.serve.pipe(
      Effect.provideService(AcpTransport, right),
      Effect.provideService(Store.Store, store),
      Effect.ignore
    ))
    const peer = yield* driver(left)
    const responseFor = (id: number) => Effect.gen(function*() {
      while (true) {
        const frame = yield* peer.next
        if (field(frame, "id") === id) return frame
      }
    })
    yield* peer.send(initialize(2))
    yield* responseFor(0)
    for (const [id, sessionId] of [[1, "first"], [2, "late"]] as const) {
      yield* peer.send({ jsonrpc: "2.0", id, method: "session/new", params: { cwd: "/tmp" } })
      expect(field(yield* responseFor(id), "result.sessionId")).toBe(sessionId)
    }
    const prompt = (id: number, sessionId: string) => peer.send({
      jsonrpc: "2.0", id, method: "session/prompt", params: { sessionId, prompt: [] }
    })
    yield* prompt(3, "first")
    yield* responseFor(3)
    yield* Deferred.await(firstStarted)
    yield* prompt(4, "late")
    yield* Deferred.await(lateLookup)
    const shuttingDown = yield* Effect.forkChild(Fiber.interrupt(serving))
    yield* Deferred.await(firstClosing)
    yield* Deferred.succeed(releaseLookup, undefined)
    yield* Deferred.await(lateStarted)
    yield* Deferred.succeed(releaseFirst, undefined)
    yield* Fiber.join(shuttingDown)
    const stopped = yield* Deferred.isDone(lateStopped)
    yield* Deferred.succeed(releaseLate, undefined)
    expect(stopped).toBe(true)
  })))

describe("store-backed replay", () => {
  it.effect("records each message's first real time across chunks and replacements, then replays in that order", () =>
    run(Effect.gen(function*() {
      const store = yield* Store.Store
      const liveClock = yield* Clock.Clock
      let now = 10_000
      const clock: Clock.Clock = {
        currentTimeMillisUnsafe: () => now,
        currentTimeMillis: Effect.sync(() => now),
        currentTimeNanosUnsafe: () => liveClock.currentTimeNanosUnsafe(),
        currentTimeNanos: liveClock.currentTimeNanos,
        monotonicTimeNanosUnsafe: () => liveClock.monotonicTimeNanosUnsafe(),
        monotonicTimeNanos: liveClock.monotonicTimeNanos,
        sleep: (duration) => liveClock.sleep(duration)
      }
      const agent = AcpAgent.makeUnsafe({
        ...baseOptions(),
        session: { create: () => Effect.succeed({ sessionId: "s-1" }), resume: () => Effect.void },
        prompt: {
          insert: () => Effect.succeed({ messageId: "prompt" }),
          execute: ({ emit }) => Effect.gen(function*() {
            now = 20_000
            yield* emit.agentChunk("agent", { type: "text", text: "first" })
            now = 30_000
            yield* emit.thoughtChunk("thought", { type: "text", text: "thinking" })
            now = 40_000
            yield* emit.userChunk("user", { type: "text", text: "follow-up" })
            now = 50_000
            yield* emit.message("agent", "agent", [{ type: "text", text: "revised" }])
            now = 60_000
            yield* emit.agentChunk("agent", { type: "text", text: "tail" })
            return "end_turn" as const
          })
        }
      })
      const peer = yield* connect(agent).pipe(Effect.provideService(Clock.Clock, clock))
      yield* peer.send(initialize(2))
      yield* peer.next
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp" } })
      yield* peer.next
      yield* peer.send({ jsonrpc: "2.0", id: 2, method: "session/prompt", params: { sessionId: "s-1", prompt: [] } })
      while (field(yield* peer.next, "params.update.state") !== "idle") {}

      const retained = yield* store.retained("s-1")
      expect(retained.map((message) => message.messageId)).toEqual(["prompt", "agent", "thought", "user"])
      expect(retained.map((message) => DateTime.toEpochMillis(message.recordedAt))).toEqual([10_000, 20_000, 30_000, 40_000])
      expect(retained[1]?.replacement).toEqual([{ type: "text", text: "revised" }])
      expect(retained[1]?.chunks).toEqual([{ type: "text", text: "tail" }])

      // A later inserted historical record must replay at its recorded time.
      yield* store.retain({ sessionId: "s-1", messageId: "backfill", role: "agent",
        replacement: [{ type: "text", text: "earlier" }], chunks: [], recordedAt: DateTime.makeUnsafe(5_000) })
      yield* peer.send({ jsonrpc: "2.0", id: 3, method: "session/resume", params: { sessionId: "s-1", cwd: "/tmp", replayFrom: { type: "start" } } })
      const replayed: Array<string> = []
      while (true) {
        const frame = yield* peer.next
        if (field(frame, "id") === 3) break
        const update = field(frame, "params.update.sessionUpdate")
        if (update === "user_message" || update === "agent_message" || update === "agent_thought") {
          replayed.push(field(frame, "params.update.messageId") as string)
        }
      }
      expect(replayed).toEqual(["backfill", "prompt", "agent", "thought", "user"])
    })))

  it.effect("replay preserves the message id and resets content before appending chunks", () =>
    run(Effect.gen(function*() {
      const store = yield* Effect.service(Store.Store)
      const agent = AcpAgent.makeUnsafe({
        ...baseOptions(),
        session: {
          create: () => Effect.succeed({ sessionId: "s-1" }),
          resume: () => Effect.void
        }
      })
      const peer = yield* connect(agent)
      yield* peer.send(initialize(2))
      yield* peer.next
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp" } })
      yield* peer.next

      yield* store.retain({
        sessionId: "s-1",
        messageId: "kept-1",
        role: "agent",
        replacement: [{ type: "text", text: "final" }],
        chunks: [{ type: "text", text: " more" }],
        recordedAt: DateTime.makeUnsafe("2020-01-01T00:00:00Z")
      })

      yield* peer.send({
        jsonrpc: "2.0",
        id: 2,
        method: "session/resume",
        params: { sessionId: "s-1", cwd: "/tmp", replayFrom: { type: "start" } }
      })

      const updates: Array<unknown> = []
      let response: unknown
      while (response === undefined) {
        const frame = yield* peer.next
        if (field(frame, "id") === 2) response = frame
        else updates.push(frame)
      }
      expect(field(response, "error")).toBeUndefined()
      const full = updates.find((frame) => field(frame, "params.update.sessionUpdate") === "agent_message")
      const chunk = updates.find((frame) => field(frame, "params.update.sessionUpdate") === "agent_message_chunk")
      // The replacement resets prior content; the chunk appends to it. Both
      // carry the original identity.
      expect(field(full, "params.update.messageId")).toBe("kept-1")
      expect(field(full, "params.update.content")).toEqual([{ type: "text", text: "final" }])
      expect(field(chunk, "params.update.messageId")).toBe("kept-1")
      expect(updates.indexOf(full)).toBeLessThan(updates.indexOf(chunk))
    })))

  it.effect("a full message replacement is refused on v1, which has no message identities", () =>
    run(Effect.gen(function*() {
      const outcome = Deferred.makeUnsafe<import("effect/Exit").Exit<void, AcpAgent.HandlerError>>()
      const agent = AcpAgent.makeUnsafe({
        ...baseOptions(),
        versions: [1],
        prompt: {
          insert: () => Effect.succeed({ messageId: "m-1" }),
          execute: ({ emit }) =>
            emit.message("agent", "m-1", [{ type: "text", text: "x" }]).pipe(
              Effect.exit,
              Effect.flatMap((exit) => Deferred.succeed(outcome, exit)),
              Effect.as("end_turn")
            )
        }
      })
      const peer = yield* connect(agent)
      yield* peer.send(initialize(1))
      yield* peer.next
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp", mcpServers: [] } })
      yield* peer.next
      yield* peer.send({ jsonrpc: "2.0", id: 2, method: "session/prompt", params: { sessionId: "s-1", prompt: [] } })
      const exit = yield* Deferred.await(outcome)
      expect(agentFailure(exit)).toMatchObject({
        _tag: "AcpAgentError", message: "Full message replacement requires protocol v2"
      })
    })))

  it.effect("session/list is advertised and answered only when enabled", () =>
    run(Effect.gen(function*() {
      const peer = yield* connect(AcpAgent.makeUnsafe({ ...baseOptions(), list: true }))
      yield* peer.send(initialize(2))
      yield* peer.next
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp" } })
      yield* peer.next
      yield* peer.send({ jsonrpc: "2.0", id: 2, method: "session/list", params: {} })
      const response = yield* peer.next
      expect(field(response, "result.sessions")).toEqual([{ sessionId: "s-1", cwd: "/tmp", title: null, updatedAt: null }])
    })))

  it.effect("v2 session/list is part of the baseline even without an optional list flag", () =>
    run(Effect.gen(function*() {
      const peer = yield* connect(AcpAgent.makeUnsafe(baseOptions()))
      yield* peer.send(initialize(2))
      yield* peer.next
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/list", params: {} })
      const response = yield* peer.next
      expect(field(response, "result.sessions")).toEqual([])
    })))
})

describe("process stdio serving", () => {
  const decode = (chunk: string | Uint8Array) => typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk)

  /** A `Stdio` layer over fixed stdin bytes that captures stdout and stderr. */
  const testStdio = (input: string, written: Deferred.Deferred<void>) => {
    const out: Array<string> = []
    const err: Array<string> = []
    const into = (buffer: Array<string>) => Sink.forEach((chunk: string | Uint8Array) => Effect.gen(function*() {
      buffer.push(decode(chunk))
      if (buffer === out && out.join("").includes("\n")) yield* Deferred.succeed(written, undefined)
    }))
    const layer = Layer.succeed(
      Stdio.Stdio,
      Stdio.make({
        args: Effect.succeed([]),
        stdin: Stream.make(new TextEncoder().encode(input)),
        stdout: () => into(out),
        stderr: () => into(err)
      })
    )
    return { out, err, layer }
  }

  it.effect("stdout carries only ACP frames and diagnostics go to stderr", () =>
    run(Effect.gen(function*() {
      const written = yield* Deferred.make<void>()
      const { err, layer, out } = testStdio(
        `${(yield* Json.encode(initialize(2)))}\n` +
          `${(yield* Json.encode({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp" } }))}\n`,
        written
      )
      const agent = AcpAgent.makeUnsafe(baseOptions())
      yield* Effect.provide(
        Effect.gen(function*() {
          yield* ProcessStdio.diagnostic("starting up")
          const transport = yield* ProcessStdio.make()
          yield* Effect.forkScoped(Effect.ignore(agent.serve.pipe(Effect.provideService(AcpTransport, transport))))
          yield* Deferred.await(written)
        }),
        layer
      )
      const frames = out.join("").split("\n").filter((line) => line.trim() !== "")
      expect(frames.length).toBeGreaterThan(0)
      // Every stdout line is a JSON-RPC frame and nothing else.
      for (const frame of frames) expect(field(yield* Json.decode(frame), "jsonrpc")).toBe("2.0")
      expect(err.join("")).toContain("starting up")
      expect(out.join("")).not.toContain("starting up")
    })))
})

describe("author dependencies", () => {
  class Greeter extends Context.Service<Greeter, {
    readonly greet: (name: string) => string
  }>()("test/Greeter") {}
  const GreeterLayer = Layer.succeed(Greeter, Greeter.of({ greet: (name) => `hello ${name}` }))

  it.effect("a handler's own services stay in the agent's requirements and are provided at serve time", () =>
    run(Effect.gen(function*() {
      // `R` is inferred from the handlers, not declared: the agent needs Greeter.
      const agent = AcpAgent.makeUnsafe({
        ...baseOptions(),
        session: {
          create: () =>
            Effect.map(Effect.service(Greeter), (greeter) => ({ sessionId: greeter.greet("session") }))
        }
      })
      const [left, right] = yield* InMemory.makePair()
      yield* Effect.forkScoped(
        Effect.ignore(Effect.provide(agent.serve.pipe(Effect.provideService(AcpTransport, right)), GreeterLayer))
      )
      const peer = yield* driver(left)
      yield* peer.send(initialize(2))
      yield* peer.next
      yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp" } })
      const response = yield* peer.next
      expect(field(response, "result.sessionId")).toBe("hello session")
    })))
})

it.effect("v1 session cancellation interrupts the owned turn and returns cancelled after final updates", () => run(Effect.gen(function*() {
  const started = yield* Deferred.make<void>()
  const agent = AcpAgent.makeUnsafe({ ...baseOptions(), prompt: {
    insert: () => Effect.succeed({ messageId: "m" }),
    execute: ({ emit }) => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never),
      Effect.onInterrupt(() => Effect.ignore(emit.agentChunk("m", { type: "text", text: "final" }))), Effect.as("end_turn"))
  } })
  const peer = yield* connect(agent)
  yield* peer.send(initialize(1)); yield* peer.next
  yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp", mcpServers: [] } }); yield* peer.next
  yield* peer.send({ jsonrpc: "2.0", id: 2, method: "session/prompt", params: { sessionId: "s-1", prompt: [] } })
  yield* Deferred.await(started)
  yield* peer.send({ jsonrpc: "2.0", method: "session/cancel", params: { sessionId: "s-1" } })
  const final = yield* peer.next
  expect(field(final, "params.update.content.text")).toBe("final")
  const response = yield* peer.next
  expect(field(response, "result.stopReason")).toBe("cancelled")
})))

for (const method of ["session/close", "session/delete"] as const) {
  it.effect(`v1 in-flight prompt completes as cancelled during ${method}`, () => run(Effect.gen(function*() {
    const started = yield* Deferred.make<void>()
    const agent = AcpAgent.makeUnsafe({ ...baseOptions(), session: {
      create: () => Effect.succeed({ sessionId: "s-1" }),
      close: () => Effect.void,
      delete: () => Effect.void
    }, prompt: {
      insert: () => Effect.succeed({ messageId: "m" }),
      execute: () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never))
    } })
    const peer = yield* connect(agent)
    yield* peer.send(initialize(1)); yield* peer.next
    yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp", mcpServers: [] } }); yield* peer.next
    yield* peer.send({ jsonrpc: "2.0", id: 2, method: "session/prompt", params: { sessionId: "s-1", prompt: [] } })
    yield* Deferred.await(started)
    yield* peer.send({ jsonrpc: "2.0", id: 3, method, params: { sessionId: "s-1" } })
    const replies = [yield* peer.next, yield* peer.next]
    expect(replies).toEqual(expect.arrayContaining([
      expect.objectContaining({ id: 2, result: { stopReason: "cancelled" } }),
      expect.objectContaining({ id: 3, result: {} })
    ]))
  })))
}

it.effect("v1 close does not turn an interruption-time defect into cancellation", () => run(Effect.gen(function*() {
  const started = yield* Deferred.make<void>()
  const agent = AcpAgent.makeUnsafe({ ...baseOptions(), session: {
    create: () => Effect.succeed({ sessionId: "s-1" }),
    close: () => Effect.void
  }, prompt: {
    insert: () => Effect.succeed({ messageId: "m" }),
    execute: () => Deferred.succeed(started, undefined).pipe(
      Effect.andThen(Effect.never),
      Effect.onInterrupt(() => Effect.die(new Error("private defect")))
    )
  } })
  const peer = yield* connect(agent)
  yield* peer.send(initialize(1)); yield* peer.next
  yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/tmp", mcpServers: [] } }); yield* peer.next
  yield* peer.send({ jsonrpc: "2.0", id: 2, method: "session/prompt", params: { sessionId: "s-1", prompt: [] } })
  yield* Deferred.await(started)
  yield* peer.send({ jsonrpc: "2.0", id: 3, method: "session/close", params: { sessionId: "s-1" } })
  const replies = [yield* peer.next, yield* peer.next]
  expect(replies).toEqual(expect.arrayContaining([
    expect.objectContaining({ id: 2, error: { code: -32603, message: "Internal error" } }),
    expect.objectContaining({ id: 3, result: {} })
  ]))
})))

it.effect("store appends chunks without losing previous chunks or replacement content", () => run(Effect.gen(function*() {
  const store = yield* Store.Store
  yield* store.create({ sessionId: "s", cwd: "/tmp" })
  const message = { sessionId: "s", messageId: "m", role: "agent" as const, recordedAt: DateTime.makeUnsafe("2026-01-01T00:00:00Z") }
  yield* store.retain({ ...message, replacement: [{ type: "text", text: "base" }], chunks: [] })
  for (const text of ["one", "two"]) yield* store.retain({ ...message, replacement: null, chunks: [{ type: "text", text }] })
  const [retained] = yield* store.retained("s")
  expect(retained!.replacement).toEqual([{ type: "text", text: "base" }])
  expect(retained!.chunks).toEqual([{ type: "text", text: "one" }, { type: "text", text: "two" }])
})))

it.effect("store orders first recordings by time, keeps equal-time insertion order, and scopes ids by session", () => run(Effect.gen(function*() {
  const store = yield* Store.Store
  const at = (millis: number) => DateTime.makeUnsafe(millis)
  yield* store.create({ sessionId: "one", cwd: "/tmp" })
  yield* store.create({ sessionId: "two", cwd: "/tmp" })
  const retain = (sessionId: string, messageId: string, millis: number, replacement: Store.RetainedMessage["replacement"] = null) =>
    store.retain({ sessionId, messageId, role: "agent", replacement, chunks: [], recordedAt: at(millis) })

  yield* retain("one", "late", 3_000)
  yield* retain("one", "first", 1_000)
  yield* retain("one", "equal-a", 2_000)
  yield* retain("one", "equal-b", 2_000)
  yield* retain("two", "first", 4_000)
  yield* retain("one", "first", 9_000, [{ type: "text", text: "replaced" }])
  yield* store.retain({ sessionId: "one", messageId: "first", role: "agent", replacement: null,
    chunks: [{ type: "text", text: "chunk" }], recordedAt: at(10_000) })
  yield* retain("one", "equal-b", 500)
  yield* retain("one", "late", 500)

  const one = yield* store.retained("one")
  const two = yield* store.retained("two")
  expect(one.map((message) => message.messageId)).toEqual(["first", "equal-a", "equal-b", "late"])
  expect(one.map((message) => DateTime.toEpochMillis(message.recordedAt))).toEqual([1_000, 2_000, 2_000, 3_000])
  expect(one[0]?.replacement).toEqual([{ type: "text", text: "replaced" }])
  expect(one[0]?.chunks).toEqual([{ type: "text", text: "chunk" }])
  expect(two.map((message) => [message.messageId, DateTime.toEpochMillis(message.recordedAt)])).toEqual([["first", 4_000]])
})))

const validAgentFrame = (frame: unknown, version: 1 | 2) => {
  if (field(frame, "method") === "session/update") {
    expect(Schema.is(version === 1 ? V1.SessionNotification : V2.UpdateSessionNotification)(field(frame, "params"))).toBe(true)
  }
}
const untilFrame = (peer: Effect.Success<ReturnType<typeof connect>>, predicate: (frame: unknown) => boolean) =>
  Effect.gen(function*() {
    const frames: Array<unknown> = []
    while (true) {
      const frame = yield* peer.next
      frames.push(frame)
      if (predicate(frame)) return frames
    }
  })

for (const outcome of ["end_turn", "refusal", "error", "typed failure", "defect"] as const) {
  it.effect(`v2 distinguishes ${outcome} from other completion reasons`, () => run(Effect.gen(function*() {
    const agent = yield* AcpAgent.make({ ...baseOptions(), prompt: {
      insert: () => Effect.succeed({ messageId: "accepted" }),
      execute: () => {
        if (outcome === "typed failure") return Effect.fail(AcpAgent.authRequired())
        if (outcome === "defect") return Effect.die(new Error("private execution defect"))
        return Effect.succeed(outcome)
      }
    } })
    const peer = yield* connect(agent)
    yield* peer.send(initialize(2)); yield* peer.next
    yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/work" } }); yield* peer.next
    yield* peer.send({ jsonrpc: "2.0", id: 2, method: "session/prompt", params: { sessionId: "s-1", prompt: [] } })
    const frames: Array<unknown> = []
    while (!frames.some((frame) => field(frame, "id") === 2) || !frames.some((frame) => field(frame, "params.update.state") === "idle")) {
      frames.push(yield* peer.next)
    }
    frames.forEach((frame) => validAgentFrame(frame, 2))
    expect(frames.find((frame) => field(frame, "id") === 2)).toEqual({ jsonrpc: "2.0", id: 2, result: { messageId: "accepted" } })
    const idle = frames.find((frame) => field(frame, "params.update.state") === "idle")
    expect(field(idle, "params.update.stopReason")).toBe(outcome === "typed failure" || outcome === "defect" ? "error" : outcome)
    if (outcome === "typed failure") expect(field(idle, "params.update.error.code")).toBe(-32000)
    if (outcome === "defect") {
      expect(field(idle, "params.update.error.message")).toBe("Internal error")
      expect(JSON.stringify(idle)).not.toContain("private execution defect")
    }
  })))
}

for (const version of [1, 2] as const) {
  for (const support of ["absent", "empty", "disabled", "enabled"] as const) {
    it.effect(`v${version} gates terminal authentication on ${support} peer capability`, () => run(Effect.gen(function*() {
      const agent = yield* AcpAgent.make({ ...baseOptions(), auth: {
        methods: [{ methodId: "terminal", name: "Terminal", type: "terminal" }, { methodId: "normal", name: "Agent" }],
        login: () => Effect.void, logout: () => Effect.void
      } })
      const peer = yield* connect(agent)
      let terminal: boolean | object | null = null
      if (version === 1) terminal = support === "enabled"
      else if (support === "enabled") terminal = {}
      let capability: unknown = {}
      if (support === "empty") capability = { auth: {} }
      if (support === "disabled" || support === "enabled") capability = { auth: { terminal } }
      const params = version === 1
        ? { protocolVersion: 1, clientCapabilities: capability }
        : { protocolVersion: 2, info: { name: "client", version: "1" }, capabilities: capability }
      expect(Schema.is(version === 1 ? V1.InitializeRequest : V2.InitializeRequest)(params)).toBe(true)
      yield* peer.send({ jsonrpc: "2.0", id: 0, method: "initialize", params })
      const response = yield* peer.next
      expect(Schema.is(version === 1 ? V1.InitializeResponse : V2.InitializeResponse)(field(response, "result"))).toBe(true)
      const methods = field(response, "result.authMethods")
      expect(methods).toEqual(support === "enabled" ? [
        expect.objectContaining({ type: "terminal" }), expect.objectContaining({ type: "agent" })
      ] : [expect.objectContaining({ type: "agent" })])
    })))
  }
}

for (const customClose of [false, true]) {
  it.effect(`v2 close drains cancellation before freeing a session (close handler ${customClose})`, () => run(Effect.gen(function*() {
    const started = yield* Deferred.make<void>()
    const lifecycle: Array<string> = []
    const agent = yield* AcpAgent.make({ ...baseOptions(), session: {
      create: () => Effect.succeed({ sessionId: "s-1" }),
      cancel: () => Effect.sync(() => { lifecycle.push("cancel") }),
      ...(customClose ? { close: () => Effect.sync(() => { lifecycle.push("close") }) } : {})
    }, prompt: {
      insert: () => Effect.succeed({ messageId: "accepted" }),
      execute: ({ emit }) => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never),
        Effect.ensuring(Effect.ignore(emit.agentChunk("final", { type: "text", text: "final output" })).pipe(
          Effect.andThen(Effect.sync(() => { lifecycle.push("finalized") })))))
    } })
    const peer = yield* connect(agent)
    yield* peer.send(initialize(2)); yield* peer.next
    yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/work" } }); yield* peer.next
    yield* peer.send({ jsonrpc: "2.0", id: 2, method: "session/prompt", params: { sessionId: "s-1", prompt: [] } })
    yield* Deferred.await(started)
    yield* untilFrame(peer, (frame) => field(frame, "id") === 2)
    yield* peer.send({ jsonrpc: "2.0", id: 3, method: "session/close", params: { sessionId: "s-1" } })
    const frames = yield* untilFrame(peer, (frame) => field(frame, "id") === 3)
    frames.forEach((frame) => validAgentFrame(frame, 2))
    const final = frames.findIndex((frame) => field(frame, "params.update.content.text") === "final output")
    const idle = frames.findIndex((frame) => field(frame, "params.update.stopReason") === "cancelled")
    expect(final).toBeGreaterThanOrEqual(0)
    expect(idle).toBeGreaterThan(final)
    expect(idle).toBeLessThan(frames.length - 1)
    expect(lifecycle).toEqual(customClose ? ["finalized", "cancel", "close"] : ["finalized", "cancel"])
    expect(frames.at(-1)).toEqual({ jsonrpc: "2.0", id: 3, result: {} })
  })))
}

for (const failure of ["typed", "defect"] as const) {
  it.effect(`v2 retention ${failure} failure preserves insertion acknowledgement and stops execution`, () => run(Effect.gen(function*() {
    const backing = yield* Store.Store
    const retained = Store.Store.of({ ...backing, retain: () => failure === "typed"
      ? Effect.fail(new Store.StoreError({ kind: "Corrupt", message: "storage unavailable" })) : Effect.die("storage defect") })
    const conversation: Array<string> = []
    let executions = 0
    const agent = yield* AcpAgent.make({ ...baseOptions(), prompt: {
      insert: () => Effect.sync(() => { conversation.push("accepted"); return { messageId: "accepted" } }),
      execute: () => Effect.sync(() => { executions++; return "end_turn" as const })
    } })
    const peer = yield* connect(agent).pipe(Effect.provideService(Store.Store, retained))
    yield* peer.send(initialize(2)); yield* peer.next
    yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/work" } }); yield* peer.next
    yield* peer.send({ jsonrpc: "2.0", id: 2, method: "session/prompt", params: { sessionId: "s-1", prompt: [{ type: "text", text: "input" }] } })
    const frames: Array<unknown> = []
    while (!frames.some((frame) => field(frame, "id") === 2) || !frames.some((frame) => field(frame, "params.update.state") === "idle")) frames.push(yield* peer.next)
    frames.forEach((frame) => validAgentFrame(frame, 2))
    expect(frames.find((frame) => field(frame, "id") === 2)).toEqual({ jsonrpc: "2.0", id: 2, result: { messageId: "accepted" } })
    expect(field(frames.find((frame) => field(frame, "params.update.state") === "idle"), "params.update.stopReason")).toBe("error")
    expect(conversation).toEqual(["accepted"])
    expect(executions).toBe(0)
  })))
}

for (const cancellation of ["session", "request", "close"] as const) {
  it.effect(`v2 ${cancellation} cancellation after insertion preserves ack and owns foreground correctly`, () => run(Effect.gen(function*() {
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const backing = yield* Store.Store
    let interrupted = false
    let executions = 0
    const retained = Store.Store.of({ ...backing, retain: (message) => Deferred.succeed(entered, undefined).pipe(
      Effect.andThen(Deferred.await(release)), Effect.andThen(backing.retain(message)),
      Effect.onInterrupt(() => Effect.sync(() => { interrupted = true }))) })
    const agent = yield* AcpAgent.make({ ...baseOptions(), prompt: {
      insert: () => Effect.succeed({ messageId: "accepted" }),
      execute: () => Effect.sync(() => { executions++; return "end_turn" as const })
    } })
    const peer = yield* connect(agent).pipe(Effect.provideService(Store.Store, retained))
    yield* peer.send(initialize(2)); yield* peer.next
    yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/work" } }); yield* peer.next
    yield* peer.send({ jsonrpc: "2.0", id: 2, method: "session/prompt", params: { sessionId: "s-1", prompt: [] } })
    yield* Deferred.await(entered)
    const before = yield* untilFrame(peer, (frame) => field(frame, "id") === 2)
    expect(before.find((frame) => field(frame, "id") === 2)).toEqual({ jsonrpc: "2.0", id: 2, result: { messageId: "accepted" } })
    let cancellationFrame: unknown
    if (cancellation === "session") cancellationFrame = { jsonrpc: "2.0", method: "session/cancel", params: { sessionId: "s-1" } }
    else if (cancellation === "close") cancellationFrame = { jsonrpc: "2.0", id: 4, method: "session/close", params: { sessionId: "s-1" } }
    else cancellationFrame = { jsonrpc: "2.0", method: "$/cancel_request", params: { requestId: 2 } }
    yield* peer.send(cancellationFrame)
    if (cancellation === "request") yield* Deferred.succeed(release, undefined)
    const after = yield* untilFrame(peer, (frame) => field(frame, "params.update.state") === "idle")
    expect(field(after.at(-1), "params.update.stopReason")).toBe(cancellation === "request" ? "end_turn" : "cancelled")
    expect(interrupted).toBe(cancellation !== "request")
    expect(executions).toBe(cancellation === "request" ? 1 : 0)
    if (cancellation === "close") {
      const closed = yield* untilFrame(peer, (frame) => field(frame, "id") === 4)
      expect(closed.at(-1)).toEqual({ jsonrpc: "2.0", id: 4, result: {} })
    }
    yield* peer.send({ jsonrpc: "2.0", id: 3, method: "session/list", params: {} })
    const barrier = yield* untilFrame(peer, (frame) => field(frame, "id") === 3)
    expect([...before, ...after, ...barrier].filter((frame) => field(frame, "id") === 2)).toHaveLength(1)
  })))
}

it.effect("v2 insertion rejection remains an RPC error without starting foreground work", () => run(Effect.gen(function*() {
  let executions = 0
  const agent = yield* AcpAgent.make({ ...baseOptions(), prompt: {
    insert: () => Effect.fail(AcpAgent.authRequired()),
    execute: () => Effect.sync(() => { executions++; return "end_turn" as const })
  } })
  const peer = yield* connect(agent)
  yield* peer.send(initialize(2)); yield* peer.next
  yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/work" } }); yield* peer.next
  yield* peer.send({ jsonrpc: "2.0", id: 2, method: "session/prompt", params: { sessionId: "s-1", prompt: [] } })
  expect(yield* peer.next).toEqual({ jsonrpc: "2.0", id: 2, error: { code: -32000, message: "Authentication required" } })
  expect(executions).toBe(0)
})))

for (const insertion of ["interruptible", "committing"] as const) {
  it.effect(`v2 request cancellation during ${insertion} insertion respects the acceptance boundary`, () => run(Effect.gen(function*() {
    const entered = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const conversation: Array<string> = []
    let executions = 0
    const insert = Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)),
      Effect.andThen(Effect.sync(() => { conversation.push("accepted"); return { messageId: "accepted" } })))
    const agent = yield* AcpAgent.make({ ...baseOptions(), prompt: {
      insert: () => insertion === "committing" ? Effect.uninterruptible(insert) : insert,
      execute: () => Effect.sync(() => { executions++; return "end_turn" as const })
    } })
    const peer = yield* connect(agent)
    yield* peer.send(initialize(2)); yield* peer.next
    yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/work" } }); yield* peer.next
    yield* peer.send({ jsonrpc: "2.0", id: 2, method: "session/prompt", params: { sessionId: "s-1", prompt: [] } })
    yield* Deferred.await(entered)
    yield* peer.send({ jsonrpc: "2.0", method: "$/cancel_request", params: { requestId: 2 } })
    // A request on the same reader proves cancellation was received before
    // the author's uninterruptible conversation transaction is released.
    yield* peer.send({ jsonrpc: "2.0", id: 3, method: "session/list", params: {} })
    const frames = yield* untilFrame(peer, (frame) => field(frame, "id") === 3)
    yield* Deferred.succeed(release, undefined)
    while (!frames.some((frame) => field(frame, "id") === 2)) frames.push(yield* peer.next)
    const response = frames.find((frame) => field(frame, "id") === 2)
    expect(response).toEqual({ jsonrpc: "2.0", id: 2, result: { messageId: "accepted" } })
    expect(conversation).toEqual(["accepted"])
    while (!frames.some((frame) => field(frame, "params.update.state") === "idle")) frames.push(yield* peer.next)
    expect(executions).toBe(1)
    expect(field(frames.find((frame) => field(frame, "params.update.state") === "idle"), "params.update.stopReason")).toBe("end_turn")
  })))
}

it.effect("v2 close joins cancellation already draining finalizers", () => run(Effect.gen(function*() {
  const started = yield* Deferred.make<void>()
  const finalizing = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  let cancels = 0
  const agent = yield* AcpAgent.make({ ...baseOptions(), session: {
    create: () => Effect.succeed({ sessionId: "s-1" }), cancel: () => Effect.sync(() => { cancels++ })
  }, prompt: {
    insert: () => Effect.succeed({ messageId: "accepted" }),
    execute: ({ emit }) => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never),
      Effect.ensuring(Deferred.succeed(finalizing, undefined).pipe(Effect.andThen(Deferred.await(release)),
        Effect.andThen(Effect.ignore(emit.agentChunk("final", { type: "text", text: "final output" }))))))
  } })
  const peer = yield* connect(agent)
  yield* peer.send(initialize(2)); yield* peer.next
  yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/work" } }); yield* peer.next
  yield* peer.send({ jsonrpc: "2.0", id: 2, method: "session/prompt", params: { sessionId: "s-1", prompt: [] } })
  yield* Deferred.await(started)
  yield* untilFrame(peer, (frame) => field(frame, "id") === 2)
  yield* peer.send({ jsonrpc: "2.0", method: "session/cancel", params: { sessionId: "s-1" } })
  yield* Deferred.await(finalizing)
  yield* peer.send({ jsonrpc: "2.0", id: 3, method: "session/close", params: { sessionId: "s-1" } })
  yield* peer.send({ jsonrpc: "2.0", id: 4, method: "session/list", params: {} })
  yield* untilFrame(peer, (frame) => field(frame, "id") === 4)
  yield* Deferred.succeed(release, undefined)
  const frames = yield* untilFrame(peer, (frame) => field(frame, "id") === 3)
  expect(cancels).toBe(1)
  expect(frames.filter((frame) => field(frame, "params.update.state") === "idle")).toHaveLength(1)
  const final = frames.findIndex((frame) => field(frame, "params.update.content.text") === "final output")
  expect(final).toBeGreaterThanOrEqual(0)
  expect(frames.findIndex((frame) => field(frame, "params.update.stopReason") === "cancelled")).toBeGreaterThan(final)
  expect(frames.at(-1)).toEqual({ jsonrpc: "2.0", id: 3, result: {} })
})))

it.effect("v2 close reports an interruption-time finalizer failure as error", () => run(Effect.gen(function*() {
  const started = yield* Deferred.make<void>()
  const agent = yield* AcpAgent.make({ ...baseOptions(), prompt: {
    insert: () => Effect.succeed({ messageId: "accepted" }),
    execute: () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never),
      Effect.onInterrupt(() => Effect.die("private finalizer failure")))
  } })
  const peer = yield* connect(agent)
  yield* peer.send(initialize(2)); yield* peer.next
  yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/work" } }); yield* peer.next
  yield* peer.send({ jsonrpc: "2.0", id: 2, method: "session/prompt", params: { sessionId: "s-1", prompt: [] } })
  yield* Deferred.await(started)
  yield* untilFrame(peer, (frame) => field(frame, "id") === 2)
  yield* peer.send({ jsonrpc: "2.0", id: 3, method: "session/close", params: { sessionId: "s-1" } })
  const frames = yield* untilFrame(peer, (frame) => field(frame, "id") === 3)
  const idles = frames.filter((frame) => field(frame, "params.update.state") === "idle")
  expect(idles).toHaveLength(1)
  expect(field(idles[0], "params.update.stopReason")).toBe("error")
  expect(field(idles[0], "params.update.error.message")).toBe("Internal error")
  expect(JSON.stringify(idles)).not.toContain("private finalizer failure")
  expect(frames.at(-1)).toEqual({ jsonrpc: "2.0", id: 3, result: {} })
})))

it.effect("v2 commits only the validated canonical insertion identity", () => run(Effect.gen(function*() {
  const agent = yield* AcpAgent.make({ ...baseOptions(), prompt: {
    insert: () => Effect.succeed({ messageId: "accepted", privateState: "author-only" }),
    execute: () => Effect.succeed("end_turn")
  } })
  const peer = yield* connect(agent)
  yield* peer.send(initialize(2)); yield* peer.next
  yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/work" } }); yield* peer.next
  yield* peer.send({ jsonrpc: "2.0", id: 2, method: "session/prompt", params: { sessionId: "s-1", prompt: [] } })
  const frames = yield* untilFrame(peer, (frame) => field(frame, "id") === 2)
  expect(frames.at(-1)).toEqual({ jsonrpc: "2.0", id: 2, result: { messageId: "accepted" } })
})))

it.effect("cancelling the v2 close RPC still drains the started session cancellation", () => run(Effect.gen(function*() {
  const started = yield* Deferred.make<void>()
  const finalizing = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  let inserts = 0
  const agent = yield* AcpAgent.make({ ...baseOptions(), prompt: {
    insert: () => Effect.sync(() => ({ messageId: `accepted-${++inserts}` })),
    execute: ({ emit }) => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never),
      Effect.ensuring(Deferred.succeed(finalizing, undefined).pipe(Effect.andThen(Deferred.await(release)),
        Effect.andThen(Effect.ignore(emit.agentChunk("final", { type: "text", text: "final output" }))))))
  } })
  const peer = yield* connect(agent)
  yield* peer.send(initialize(2)); yield* peer.next
  yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/work" } }); yield* peer.next
  yield* peer.send({ jsonrpc: "2.0", id: 2, method: "session/prompt", params: { sessionId: "s-1", prompt: [] } })
  yield* Deferred.await(started)
  yield* untilFrame(peer, (frame) => field(frame, "id") === 2)
  yield* peer.send({ jsonrpc: "2.0", id: 3, method: "session/close", params: { sessionId: "s-1" } })
  yield* Deferred.await(finalizing)
  yield* peer.send({ jsonrpc: "2.0", method: "$/cancel_request", params: { requestId: 3 } })
  yield* peer.send({ jsonrpc: "2.0", id: 4, method: "session/prompt", params: { sessionId: "s-1", prompt: [] } })
  const during = yield* untilFrame(peer, (frame) => field(frame, "id") === 4)
  expect(field(during.at(-1), "error.message")).toBe("Session is busy")
  expect(inserts).toBe(1)
  yield* Deferred.succeed(release, undefined)
  const frames = [...during]
  while (!frames.some((frame) => field(frame, "id") === 3) || !frames.some((frame) => field(frame, "params.update.state") === "idle")) frames.push(yield* peer.next)
  const final = frames.findIndex((frame) => field(frame, "params.update.content.text") === "final output")
  expect(final).toBeGreaterThanOrEqual(0)
  expect(frames.findIndex((frame) => field(frame, "params.update.stopReason") === "cancelled")).toBeGreaterThan(final)
  expect(field(frames.find((frame) => field(frame, "id") === 3), "error.code")).toBe(-32800)
})))

it.effect("disconnecting the owning peer interrupts a blocked acceptance phase", () => run(Effect.gen(function*() {
  const entered = yield* Deferred.make<void>()
  const interrupted = yield* Deferred.make<void>()
  let executions = 0
  const agent = yield* AcpAgent.make({ ...baseOptions(), prompt: {
    insert: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never),
      Effect.onInterrupt(() => Effect.asVoid(Deferred.succeed(interrupted, undefined)))),
    execute: () => Effect.sync(() => { executions++; return "end_turn" as const })
  } })
  const parent = yield* Scope.Scope
  const clientScope = yield* Scope.fork(parent)
  const pair = yield* InMemory.make()
  const client = yield* Scope.provide(pair.left, clientScope)
  const server = yield* pair.right
  const serving = yield* Effect.forkScoped(agent.serve.pipe(Effect.provideService(AcpTransport, server)))
  const peer = yield* driver(client)
  yield* peer.send(initialize(2)); yield* peer.next
  yield* peer.send({ jsonrpc: "2.0", id: 1, method: "session/new", params: { cwd: "/work" } }); yield* peer.next
  yield* peer.send({ jsonrpc: "2.0", id: 2, method: "session/prompt", params: { sessionId: "s-1", prompt: [] } })
  yield* Deferred.await(entered)
  yield* Scope.close(clientScope, Exit.void)
  yield* Deferred.await(interrupted)
  expect(Exit.isSuccess(yield* Fiber.await(serving))).toBe(true)
  expect(executions).toBe(0)
})))
