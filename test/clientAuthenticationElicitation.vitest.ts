import { describe, expect, it } from "@effect/vitest"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as TestClock from "effect/testing/TestClock"
import { AcpClient, type ConnectOptions, type ElicitationCallback } from "../src/AcpClient.ts"
import * as Local from "../src/AcpLocalClient.ts"
import * as V1 from "../src/protocol/v1/Schema.ts"
import * as V2 from "../src/protocol/v2/Schema.ts"
import { AcpProtocolError } from "../src/AcpError.ts"
import { scriptedAgent } from "./support/sessionAgent.ts"
import { awaitSnapshot, failureOf } from "./support/sessionContract.ts"
import { field } from "./support/field.ts"

type Modes = { readonly form?: {} | null; readonly url?: {} | null }
const open = (version: 1 | 2, modes: Modes | undefined, options: Omit<ConnectOptions, "versions" | "params"> = {}, terminal = false) =>
  Effect.gen(function*() {
    const method = terminal
      ? { type: "terminal", name: "Terminal", args: ["--login"], ...(version === 1 ? { id: "login", env: {} } : { methodId: "login", env: [] }) }
      : { name: "Login", ...(version === 1 ? { id: "login" } : { type: "agent", methodId: "login" }) }
    const initialize = version === 1
      ? { protocolVersion: 1, agentCapabilities: {}, authMethods: [method] }
      : { protocolVersion: 2, info: { name: "agent", version: "1" }, capabilities: { session: {} }, authMethods: [method] }
    const agent = yield* scriptedAgent({ version, initialize, uniqueSessions: true })
    const client = yield* AcpClient.pipe(Effect.provide(Local.layer.pipe(Layer.provide(agent.connector))))
    const capabilities = modes === undefined ? {} : { elicitation: modes }
    const params: ConnectOptions = version === 1
      ? { ...options, versions: [1], params: { clientInfo: { name: "test", version: "1" }, clientCapabilities: capabilities } }
      : { ...options, versions: [2], params: { info: { name: "test", version: "1" }, capabilities } }
    const connection = yield* client.connect(params)
    const advertised = field((yield* agent.received).find((message) => message.method === "initialize"),
      version === 1 ? "params.clientCapabilities.elicitation" : "params.capabilities.elicitation")
    expect(advertised).toEqual(modes)
    return { agent, client, connection, params }
  })

const elicitation = (mode: "form" | "url", scope: { readonly requestId: unknown } | { readonly sessionId: string }) => ({
  ...scope, mode, message: "Input needed", _meta: { correlation: "retained" },
  ...(mode === "url" ? { url: "https://example.test/auth", elicitationId: "oauth" }
    : { requestedSchema: { type: "object", properties: { branch: { type: "string" } } } })
})

for (const version of [1, 2] as const) {
  describe(`v${version} authentication and elicitation`, () => {
    it.effect("terminal authentication releases the old transport and requires fresh initialization", () => Effect.scoped(Effect.gen(function*() {
      let callbacks = 0
      const { agent, client, connection, params } = yield* open(version, undefined, { terminalAuth: () => Effect.sync(() => { callbacks++ }) }, true)
      const session = yield* connection.newSession({ cwd: "/work" })
      yield* connection.authenticate("login")
      expect(callbacks).toBe(1)
      expect((yield* connection.closed)._tag).toBe("AcpConnectionClosed")
      expect((yield* agent.received).some((message) => ["authenticate", "auth/login"].includes(String(message.method)))).toBe(false)
      expect(failureOf(yield* Effect.exit(connection.request("_closed", {})))).toMatchObject({ _tag: "AcpConnectionClosed" })
      expect(failureOf(yield* Effect.exit(session.submit([{ type: "text", text: "closed" }])))).toMatchObject({ _tag: "AcpConnectionClosed" })
      expect(failureOf(yield* Effect.exit(connection.authenticate("login")))).toMatchObject({ _tag: "AcpConnectionClosed" })
      expect(callbacks).toBe(1)
      const fresh = yield* client.connect(params)
      expect((yield* agent.received).filter((message) => message.method === "initialize")).toHaveLength(2)
      expect((yield* fresh.newSession({ cwd: "/work" })).sessionId).toBe("sess-2")
    })))

    it.effect("agent authentication still uses the negotiated login request", () => Effect.scoped(Effect.gen(function*() {
      const { agent, connection } = yield* open(version, undefined)
      const method = version === 1 ? "authenticate" : "auth/login"
      const authenticating = yield* connection.authenticate("login").pipe(Effect.forkChild)
      expect(yield* agent.awaitRequest(method)).toEqual({ methodId: "login" })
      yield* agent.respond(method, {})
      yield* Fiber.join(authenticating)
    })))

    for (const [label, modes, mode] of [
      ["absent", undefined, "form"], ["empty", {}, "form"], ["null form", { form: null }, "form"],
      ["form only", { form: {} }, "url"], ["url only", { url: {} }, "form"]
    ] as const) {
      it.effect(`rejects ${mode} with ${label} capabilities before offering a decision`, () => Effect.scoped(Effect.gen(function*() {
        let calls = 0
        const { agent, connection } = yield* open(version, modes, { onElicitation: () => Effect.sync(() => { calls++; return { _tag: "decline" } }) })
        const session = yield* connection.newSession({ cwd: "/work" })
        for (const [index, scope] of [{ sessionId: session.sessionId }, { requestId: 42 }].entries()) {
          const params = elicitation(mode, scope)
          expect(Schema.is(version === 1 ? V1.CreateElicitationRequest : V2.CreateElicitationRequest)(params)).toBe(true)
          yield* agent.send({ jsonrpc: "2.0", id: `unsupported-${index}`, method: "elicitation/create", params })
          expect(yield* agent.awaitReply(`unsupported-${index}`)).toMatchObject({ error: { code: -32602 } })
        }
        expect(calls).toBe(0)
        expect((yield* session.snapshot).interactions).toEqual({})
      })))
    }

    for (const count of [0, 1, 2]) {
      for (const mode of ["form", "url"] as const) {
        it.effect(`handles request-scoped ${mode} with ${count} sessions without misattribution`, () => Effect.scoped(Effect.gen(function*() {
          const seen: Array<Parameters<ElicitationCallback>[0]> = []
          const { agent, connection } = yield* open(version, { form: {}, url: {} }, {
            onElicitation: (request, selected) => Effect.sync(() => {
              expect(selected).toBe(version); seen.push(request); return { _tag: "decline" }
            })
          })
          const sessions = []
          for (let n = 0; n < count; n++) sessions.push(yield* connection.newSession({ cwd: "/work" }))
          const method = version === 1 ? "authenticate" : "auth/login"
          const authenticating = yield* connection.authenticate("login").pipe(Effect.forkChild)
          yield* agent.awaitRequest(method)
          const requestId = (yield* agent.received).find((message) => message.method === method)!.id
          const params = elicitation(mode, { requestId })
          yield* agent.send({ jsonrpc: "2.0", id: "scoped", method: "elicitation/create", params })
          expect(yield* agent.awaitReply("scoped")).toMatchObject({ result: { action: "decline" } })
          expect(seen).toEqual([params])
          for (const session of sessions) expect((yield* session.snapshot).interactions).toEqual({})
          yield* agent.respond(method, {})
          yield* Fiber.join(authenticating)
        })))
      }
    }

    it.effect("advertised session elicitation still uses bounded session interactions", () => Effect.scoped(Effect.gen(function*() {
      const { agent, connection } = yield* open(version, { form: {} })
      const session = yield* connection.newSession({ cwd: "/work" })
      yield* agent.send({ jsonrpc: "2.0", id: "session-form", method: "elicitation/create", params: elicitation("form", { sessionId: session.sessionId }) })
      const snapshot = yield* awaitSnapshot(session, (value) => Object.values(value.interactions).some((interaction) => interaction.status === "pending"))
      const interaction = Object.values(snapshot.interactions)[0]!
      yield* session.resolveInteraction(interaction.interactionId, { _tag: "accept", content: { branch: "main" } })
      expect(yield* agent.awaitReply("session-form")).toMatchObject({ result: { action: "accept", content: { branch: "main" } } })
    })))

    it.effect("request callback errors are sanitized and a missing callback cancels", () => Effect.scoped(Effect.gen(function*() {
      for (const onElicitation of [undefined, () => Effect.fail(new AcpProtocolError({ message: "private-handler-secret" }))]) {
        const { agent } = yield* open(version, { url: {} }, { onElicitation })
        yield* agent.send({ jsonrpc: "2.0", id: "callback-failure", method: "elicitation/create", params: elicitation("url", { requestId: 42 }) })
        const reply = yield* agent.awaitReply("callback-failure")
        expect(reply).toMatchObject(onElicitation === undefined ? { result: { action: "cancel" } } : { error: { code: -32603 } })
        expect(JSON.stringify(reply)).not.toContain("private-handler-secret")
      }
    })))

    it.effect("request cancellation interrupts the callback and releases its capacity", () => Effect.scoped(Effect.gen(function*() {
      const started = yield* Deferred.make<void>()
      let cancelled = 0
      const { agent } = yield* open(version, { url: {} }, { limits: { interactions: 1 }, onElicitation: (request) =>
        request.requestId === 1 ? Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never), Effect.onInterrupt(() => Effect.sync(() => { cancelled++ })))
          : Effect.succeed({ _tag: "decline" }) })
      yield* agent.send({ jsonrpc: "2.0", id: "held", method: "elicitation/create", params: elicitation("url", { requestId: 1 }) })
      yield* Deferred.await(started)
      yield* agent.send({ jsonrpc: "2.0", id: "capacity", method: "elicitation/create", params: elicitation("url", { requestId: 2 }) })
      expect(yield* agent.awaitReply("capacity")).toMatchObject({ error: { code: -32600 } })
      yield* agent.send({ jsonrpc: "2.0", method: "$/cancel_request", params: { requestId: "held" } })
      expect(yield* agent.awaitReply("held")).toMatchObject({ error: { code: -32800 } })
      expect(cancelled).toBe(1)
      yield* agent.send({ jsonrpc: "2.0", id: "reused", method: "elicitation/create", params: elicitation("url", { requestId: 3 }) })
      expect(yield* agent.awaitReply("reused")).toMatchObject({ result: { action: "decline" } })
    })))

    for (const ending of ["completion", "deadline"] as const) {
      it.effect(`${ending} cancels a pending URL callback and releases its capacity`, () => Effect.scoped(Effect.gen(function*() {
        const started = yield* Deferred.make<void>()
        let cancelled = 0
        const { agent } = yield* open(version, { url: {} }, { limits: { interactions: 1 },
          ...(ending === "deadline" ? { interactionTimeout: "50 millis" } : {}),
          onElicitation: (request) => request.requestId === 1
            ? Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never), Effect.onInterrupt(() => Effect.sync(() => { cancelled++ })))
            : Effect.succeed({ _tag: "decline" }) })
        yield* agent.send({ jsonrpc: "2.0", id: "ending", method: "elicitation/create", params: elicitation("url", { requestId: 1 }) })
        yield* Deferred.await(started)
        if (ending === "completion") yield* agent.send({ jsonrpc: "2.0", method: "elicitation/complete", params: { elicitationId: "oauth" } })
        else yield* TestClock.adjust("50 millis")
        expect(yield* agent.awaitReply("ending")).toMatchObject({ result: { action: "cancel" } })
        expect(cancelled).toBe(1)
        yield* agent.send({ jsonrpc: "2.0", id: "after-ending", method: "elicitation/create", params: elicitation("url", { requestId: 2 }) })
        expect(yield* agent.awaitReply("after-ending")).toMatchObject({ result: { action: "decline" } })
      })))
    }
  })
}
