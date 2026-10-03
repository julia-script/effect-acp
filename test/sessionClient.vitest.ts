import { describe, expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import * as Schema from "effect/Schema"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as TestClock from "effect/testing/TestClock"
import type { AcpSession } from "../src/AcpClient.ts"
import { type OperationError, type ConnectError, AcpClient } from "../src/AcpClient.ts"
import * as AcpLocalClient from "../src/AcpLocalClient.ts"
import * as V1 from "../src/protocol/v1/Schema.ts"
import * as V2 from "../src/protocol/v2/Schema.ts"
import { type ScriptedAgent, scriptedAgent } from "./support/sessionAgent.ts"

import { awaitSnapshot, harness, text, failureOf, completePrompt, sessionContract, type Harness } from "./support/sessionContract.ts"

const run = <A, E>(effect: Effect.Effect<A, E, Scope.Scope>) => Effect.scoped(effect)
const hasText = (session: { readonly messages: ReadonlyArray<{ readonly content: ReadonlyArray<{ readonly type: string }> }> }, value: string) =>
  session.messages.some((message) => message.content.some((part) => part.type === "text" && "text" in part && part.text === value))

/** Opens a session on a fresh harness. */
const withSession = (
  version: 1 | 2,
  options: Parameters<typeof harness>[1] = {}
): Effect.Effect<Harness & { readonly session: AcpSession }, OperationError | ConnectError | import("effect/rpc/RpcClientError").RpcClientError, Scope.Scope> =>
  Effect.gen(function*() {
    const open = yield* harness(version, options)
    const session = yield* open.connection.newSession({ cwd: "/work" })
    return { ...open, session }
  })

/** Uses the actual initialize advertisement rather than the default harness options. */
const withElicitationSession = (version: 1 | 2) => Effect.gen(function*() {
  const agent = yield* scriptedAgent({ version })
  const client = yield* AcpClient.pipe(Effect.provide(AcpLocalClient.layer.pipe(Layer.provide(agent.connector))))
  const elicitation = { form: {}, url: {} }
  const connection = yield* client.connect(version === 1
    ? { versions: [1], params: { clientInfo: { name: "test", version: "1" }, clientCapabilities: { elicitation } } }
    : { versions: [2], params: { info: { name: "test", version: "1" }, capabilities: { elicitation } } })
  const session = yield* connection.newSession({ cwd: "/work" })
  return { agent, connection, session }
})

for (const version of [1, 2] as const) {
  it.effect(`v${version} terminal authentication advertises the version's capability shape`, () => run(Effect.gen(function*() {
    const { agent } = yield* harness(version, { connect: { terminalAuth: () => Effect.void } })
    const initialize = (yield* agent.received).find((message) => message.method === "initialize")
    expect(initialize).toMatchObject({ params: version === 1
      ? { clientCapabilities: { auth: { terminal: true } } }
      : { capabilities: { auth: { terminal: {} } } }
    })
  })))

  it.effect(`v${version} completed submission stays successful after owner closure`, () => run(Effect.gen(function*() {
    const owner = yield* Scope.make()
    const { agent, session } = yield* Scope.provide(withSession(version), owner)
    const submission = yield* session.submit([text("done")])
    yield* agent.awaitRequest("session/prompt")
    yield* completePrompt(agent, version)
    expect((yield* submission.outcome).status).toEqual({ _tag: "completed" })
    yield* session.release
    expect((yield* submission.outcome).status).toEqual({ _tag: "completed" })
    yield* Scope.close(owner, Exit.void)
    expect((yield* submission.outcome).status).toEqual({ _tag: "completed" })
    if (version === 1) expect(failureOf(yield* Effect.exit(submission.accepted))).toMatchObject({ _tag: "AcpCapabilityUnsupported" })
    else expect(yield* submission.accepted).toBe("m-1")
  })))
}

for (const version of [1, 2] as const) {
  it.effect(`v${version} external submission waiters settle when the owning scope closes before a prompt reply`, () => run(Effect.gen(function*() {
    const owner = yield* Scope.make()
    const { agent, session } = yield* Scope.provide(withSession(version), owner)
    const submission = yield* session.submit([text("held")])
    const accepted = yield* Effect.exit(submission.accepted).pipe(Effect.forkChild)
    const outcome = yield* Effect.exit(submission.outcome).pipe(Effect.forkChild)
    yield* agent.awaitRequest("session/prompt")
    yield* Scope.close(owner, Exit.void)
    const acceptedExit = yield* Fiber.join(accepted)
    const outcomeExit = yield* Fiber.join(outcome)
    expect(Exit.isFailure(acceptedExit)).toBe(true)
    expect(Exit.isFailure(outcomeExit)).toBe(true)
    if (version === 1) expect(failureOf(acceptedExit)).toMatchObject({ _tag: "AcpCapabilityUnsupported" })
    else expect(failureOf(acceptedExit)).toMatchObject({ _tag: "AcpConnectionClosed" })
    expect(failureOf(outcomeExit)).toMatchObject({ _tag: "AcpConnectionClosed" })
  })))
}

for (const version of [1, 2] as const) {
  it.effect(`v${version} released session handles cannot send new commands`, () => run(Effect.gen(function*() {
    const { agent, session } = yield* withSession(version)
    yield* session.release
    yield* session.release
    expect(failureOf(yield* Effect.exit(session.submit([text("after release")])))).toMatchObject({ _tag: "AcpConnectionClosed" })
    expect(failureOf(yield* Effect.exit(session.cancel))).toMatchObject({ _tag: "AcpConnectionClosed" })
    expect((yield* session.snapshot).sessionId).toBe("sess-1")
    const sent = yield* agent.received
    expect(sent.filter((message) => message.method === "session/prompt" || message.method === "session/cancel")).toHaveLength(0)
  })))

  it.effect(`v${version} releasing a session settles its admitted submission`, () => run(Effect.gen(function*() {
    const { agent, session } = yield* withSession(version)
    const submission = yield* session.submit([text("held")])
    const accepted = yield* Effect.exit(submission.accepted).pipe(Effect.forkChild)
    const outcome = yield* Effect.exit(submission.outcome).pipe(Effect.forkChild)
    yield* agent.awaitRequest("session/prompt")
    yield* session.release
    const acceptedError = failureOf(yield* Fiber.join(accepted))
    expect(acceptedError).toMatchObject({ _tag: version === 1 ? "AcpCapabilityUnsupported" : "AcpConnectionClosed" })
    expect(failureOf(yield* Fiber.join(outcome))).toMatchObject({ _tag: "AcpConnectionClosed" })
  })))
}

it.effect("v2 accepted remains successful when ownership ends before the turn completes", () => run(Effect.gen(function*() {
  const owner = yield* Scope.make()
  const { agent, session } = yield* Scope.provide(withSession(2), owner)
  const submission = yield* session.submit([text("held")])
  const outcome = yield* Effect.exit(submission.outcome).pipe(Effect.forkChild)
  yield* agent.awaitRequest("session/prompt")
  yield* agent.respond("session/prompt", { messageId: "accepted" })
  expect(yield* submission.accepted).toBe("accepted")
  yield* Scope.close(owner, Exit.void)
  expect(failureOf(yield* Fiber.join(outcome))).toMatchObject({ _tag: "AcpConnectionClosed" })
})))

it.effect("v2 accepted remains successful after explicit session release", () => run(Effect.gen(function*() {
  const { agent, session } = yield* withSession(2)
  const submission = yield* session.submit([text("held")])
  const outcome = yield* Effect.exit(submission.outcome).pipe(Effect.forkChild)
  yield* agent.awaitRequest("session/prompt")
  yield* agent.respond("session/prompt", { messageId: "accepted" })
  expect(yield* submission.accepted).toBe("accepted")
  yield* session.release
  expect(yield* submission.accepted).toBe("accepted")
  expect(failureOf(yield* Fiber.join(outcome))).toMatchObject({ _tag: "AcpConnectionClosed" })
})))

const resumeOptions = (version: 1 | 2): Parameters<typeof harness>[1] => version === 2
  ? { agent: { version, initialize: {
    protocolVersion: 2,
    info: { name: "scripted-agent", version: "1.0.0" },
    capabilities: { session: { resume: {}, prompt: { image: {} } } }
  } } }
  : {}

/** A live route must still accept updates and permission requests. */
const expectLiveRoute = (agent: ScriptedAgent, session: AcpSession, version: 1 | 2, suffix: string) =>
  Effect.gen(function*() {
    const id = `permission-${suffix}`
    const observed = yield* session.observe
    yield* agent.update("sess-1", {
      sessionUpdate: "agent_message_chunk",
      ...(version === 2 ? { messageId: `update-${suffix}` } : {}),
      content: text(suffix)
    })
    yield* agent.send({
      jsonrpc: "2.0",
      id,
      method: "session/request_permission",
      params: {
        sessionId: "sess-1",
        title: "Edit file",
        ...(version === 1 ? { toolCall: { toolCallId: "t-1", title: "Edit file" } } : {}),
        options: [{ optionId: "allow", name: "Allow", kind: "allow_once" }]
      }
    })
    const first = yield* Effect.raceFirst(
      Stream.runCollect(Stream.take(Stream.filter(observed.changes, (event) =>
        Object.values(event.snapshot.interactions).some((interaction) => interaction.status === "pending")
      ), 1)).pipe(Effect.as("pending" as const)),
      agent.awaitReply(id).pipe(Effect.as("rejected" as const))
    )
    expect(first).toBe("pending")
    const after = yield* session.observe
    if (!hasText(after.snapshot, suffix)) {
      yield* Stream.runCollect(Stream.take(Stream.filter(after.changes, (event) =>
        hasText(event.snapshot, suffix)
      ), 1))
    }
    const snapshot = yield* session.snapshot
    expect(hasText(snapshot, suffix)).toBe(true)
    const interaction = Object.values(snapshot.interactions).find((value) => value.status === "pending")!
    yield* session.resolveInteraction(interaction.interactionId, { _tag: "selected", optionId: "allow" })
    expect(yield* agent.awaitReply(id)).toMatchObject({ result: { outcome: { outcome: "selected", optionId: "allow" } } })
  })

sessionContract(1)
sessionContract(2)

// -----------------------------------------------------------------------------
// Version-specific behavior
// -----------------------------------------------------------------------------

describe("v2 acceptance and completion are separate", () => {
  it.effect("a second turn stays busy after a chunk until its own idle update", () =>
    run(Effect.gen(function*() {
      const { agent, session } = yield* withSession(2)
      const first = yield* session.submit([text("one")])
      yield* agent.awaitRequest("session/prompt")
      yield* completePrompt(agent, 2)
      yield* first.outcome

      const second = yield* session.submit([text("two")])
      yield* agent.awaitRequest("session/prompt")
      const observed = yield* session.observe
      const chunk = yield* Effect.forkChild(Stream.runCollect(Stream.take(Stream.filter(
        observed.changes,
        (event) => event.snapshot.messages.some((message) => message.id === "m-2" && message.content.length > 0)
      ), 1)))
      yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", messageId: "m-2", content: text("working") })
      yield* Fiber.join(chunk)
      yield* agent.respond("session/prompt", { messageId: "m-2" })
      yield* second.accepted

      const outcome = yield* Effect.forkChild(second.outcome)
      const cancelArrived = yield* Effect.forkChild(agent.awaitRequest("session/cancel"))
      const cancelling = yield* Effect.forkChild(session.cancel)
      yield* Fiber.join(cancelArrived)
      expect(outcome.pollUnsafe()).toBeUndefined()
      expect(cancelling.pollUnsafe()).toBeUndefined()
      expect((yield* session.snapshot).activeSubmissionId).toBe(second.id)
      expect(failureOf(yield* Effect.exit(session.submit([text("three")])))).toMatchObject({ _tag: "AcpSessionBusy" })
      expect((yield* agent.received).filter((message) => message.method === "session/prompt")).toHaveLength(2)

      yield* agent.update("sess-1", { sessionUpdate: "state_update", state: "idle", stopReason: "end_turn" })
      expect((yield* Fiber.join(outcome)).status).toEqual({ _tag: "completed" })
      expect(yield* Fiber.join(cancelling)).toBeUndefined()
    })))

  it.effect("an idle update before the v2 prompt response still completes the turn", () =>
    run(Effect.gen(function*() {
      const { agent, session } = yield* withSession(2)
      const first = yield* session.submit([text("one")])
      yield* agent.awaitRequest("session/prompt")
      yield* completePrompt(agent, 2)
      yield* first.outcome

      const submission = yield* session.submit([text("two")])
      yield* agent.awaitRequest("session/prompt")
      const observed = yield* session.observe
      const idle = yield* Effect.forkChild(Stream.runCollect(Stream.take(Stream.filter(
        observed.changes,
        (event) => event.snapshot.foreground.state === "idle"
      ), 1)))
      yield* agent.update("sess-1", { sessionUpdate: "state_update", state: "idle", stopReason: "end_turn" })
      yield* Fiber.join(idle)
      yield* agent.respond("session/prompt", { messageId: "m-1" })
      expect((yield* submission.outcome).status).toEqual({ _tag: "completed" })
    })))

  // Spec: "User update precedes acknowledgement".
  it.effect("acceptance carries the agent message id and does not end foreground work", () =>
    run(Effect.gen(function*() {
      const { agent, session } = yield* withSession(2)
      const submission = yield* session.submit([text("hi")])
      yield* agent.awaitRequest("session/prompt")

      // The user-message update arrives before the prompt response.
      yield* agent.update("sess-1", { sessionUpdate: "user_message", messageId: "m-1", content: [text("hi")] })
      yield* awaitSnapshot(session, (snapshot) =>
        snapshot.messages.some((message) => message.id === "m-1"))
      yield* agent.respond("session/prompt", { messageId: "m-1" })

      expect(yield* submission.accepted).toBe("m-1")
      const accepted = yield* session.snapshot
      // No duplicate message, and the foreground is still busy.
      expect(accepted.messages.filter((message) => message.id === "m-1")).toHaveLength(1)
      expect(accepted.submissions[submission.id]!.status).toEqual({ _tag: "accepted" })
      expect(accepted.activeSubmissionId).toBe(submission.id)

      yield* agent.update("sess-1", { sessionUpdate: "state_update", state: "idle", stopReason: "end_turn" })
      const outcome = yield* submission.outcome
      expect(outcome.status).toEqual({ _tag: "completed" })
      expect((yield* session.snapshot).activeSubmissionId).toBeNull()
    })))

  it.effect("setMode is refused on v2", () =>
    run(Effect.gen(function*() {
      const { agent, session } = yield* withSession(2)
      const exit = yield* Effect.exit(session.setMode("architect"))
      expect(failureOf(exit)).toMatchObject({ _tag: "AcpCapabilityUnsupported" })
      expect((yield* agent.received).filter((message) => message.method === "session/set_mode")).toHaveLength(0)
    })))
})

describe("v1 compatibility", () => {
  // Spec: "V1 prompt remains pending".
  it.effect("acceptance is unavailable and the prompt response is turn completion", () =>
    run(Effect.gen(function*() {
      const { agent, session } = yield* withSession(1)
      const submission = yield* session.submit([text("hi")])
      yield* agent.awaitRequest("session/prompt")

      const running = yield* session.snapshot
      expect(running.submissions[submission.id]!.acceptanceUnavailable).toBe(true)
      // Nothing has been reported by the agent, so this is explicitly inferred.
      expect(running.foreground).toEqual({ state: "running", provenance: "inferred" })

      yield* agent.respond("session/prompt", { stopReason: "end_turn" })
      const acceptance = yield* Effect.exit(submission.accepted)
      expect(failureOf(acceptance)).toMatchObject({ _tag: "AcpCapabilityUnsupported" })

      const outcome = yield* submission.outcome
      expect(outcome.status).toEqual({ _tag: "completed" })
      expect(outcome.agentMessageId).toBeNull()
      expect((yield* session.snapshot).foreground).toEqual({ state: "idle", stopReason: "end_turn" })
    })))

  // Spec: "Missing message ID during replay".
  it.effect("ID-less replay messages use local identities and are not matched to submissions", () =>
    run(Effect.gen(function*() {
      const { agent, session } = yield* withSession(1)
      const submission = yield* session.submit([text("same text")])
      yield* agent.awaitRequest("session/prompt")
      // The agent echoes identical text with no message id.
      yield* agent.update("sess-1", { sessionUpdate: "user_message_chunk", content: text("same text") })
      const snapshot = yield* awaitSnapshot(session, (snapshot) => snapshot.messages.length === 1)
      expect(snapshot.messages[0]!.provenance).toEqual({ _tag: "local" })
      // Identical content must not be treated as the submission's message.
      expect(snapshot.submissions[submission.id]!.agentMessageId).toBeNull()
    })))

  // Spec: "Execution handler absent".
  it.effect("terminal support is not advertised without an installed handler", () =>
    run(Effect.gen(function*() {
      const { connection } = yield* harness(1)
      expect(connection.capabilities.terminal).toBe(false)
      expect(connection.capabilities.filesystem).toBe(false)
    })))

  it.effect("installed handlers are advertised and actually serve agent requests", () =>
    run(Effect.gen(function*() {
      const { agent, connection } = yield* harness(1, {
        connect: {
          v1Handlers: {
            readTextFile: () => Effect.succeed({ content: "hello from disk" }),
            createTerminal: () => Effect.succeed({ terminalId: "term-1" }),
            terminalOutput: () => Effect.succeed({ output: "", truncated: false }),
            waitForTerminalExit: () => Effect.succeed({ exitCode: 0 }),
            killTerminal: () => Effect.succeed({}),
            releaseTerminal: () => Effect.succeed({})
          }
        }
      })
      expect(connection.capabilities.filesystem).toBe(true)
      expect(connection.capabilities.terminal).toBe(true)

      yield* connection.newSession({ cwd: "/work" })
      yield* agent.send({
        jsonrpc: "2.0",
        id: "fs-1",
        method: "fs/read_text_file",
        params: { sessionId: "sess-1", path: "/work/a.ts" }
      })
      const reply = yield* agent.awaitReply("fs-1")
      expect(reply).toMatchObject({ result: { content: "hello from disk" } })
    })))

  // Spec: "Version-aware history request".
  it.effect("resume reports when history recovery is unavailable", () =>
    run(Effect.gen(function*() {
      const { agent, connection } = yield* harness(1, {
        agent: {
          version: 1,
          // Neither loadSession nor session/resume advertised.
          initialize: { protocolVersion: 1, agentCapabilities: {} }
        }
      })
      const exit = yield* Effect.exit(connection.resumeSession({ sessionId: "sess-1", cwd: "/work" }))
      expect(failureOf(exit)).toMatchObject({ _tag: "AcpHistoryUnavailable" })
      const sent = yield* agent.received
      expect(sent.filter((message) => message.method === "session/load")).toHaveLength(0)
    })))

  it.effect("resume uses session/load when the agent advertises loadSession", () =>
    run(Effect.gen(function*() {
      const { agent, connection } = yield* harness(1, {
        agent: {
          version: 1,
          initialize: { protocolVersion: 1, agentCapabilities: { loadSession: true } }
        }
      })
      const resuming = yield* Effect.forkChild(connection.resumeSession({ sessionId: "sess-1", cwd: "/work" }))
      yield* agent.awaitRequest("session/load")
      // Replay arrives before the load response and must still be retained.
      yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", content: text("replayed") })
      yield* agent.respond("session/load", { modes: { currentModeId: "code", availableModes: [] } })

      const session = yield* Fiber.join(resuming)
      const snapshot = yield* session.snapshot
      expect(snapshot.messages[0]!.content).toEqual([text("replayed")])
      expect(snapshot.config["acp/modes"]).toBeDefined()
    })))

  it.effect("setMode dispatches on v1", () =>
    run(Effect.gen(function*() {
      const { agent, session } = yield* withSession(1)
      const setting = yield* Effect.forkChild(session.setMode("architect"))
      const params = yield* agent.awaitRequest("session/set_mode")
      expect(params).toEqual({ sessionId: "sess-1", modeId: "architect" })
      yield* agent.respond("session/set_mode", {})
      yield* Fiber.join(setting)
    })))
})

describe("interaction deadlines and withdrawal", () => {
  const permission = (version: 1 | 2) => ({
    jsonrpc: "2.0",
    id: "perm-1",
    method: "session/request_permission",
    params: {
      sessionId: "sess-1",
      title: "Edit file",
      ...(version === 1 ? { toolCall: { toolCallId: "t-1", title: "Edit file" } } : {}),
      options: [{ optionId: "allow", name: "Allow", kind: "allow_once" }]
    }
  })

  // Spec: "User input is delayed" — the deadline half.
  it.effect("a configured deadline settles the request with the protocol's cancelled outcome", () =>
    run(Effect.gen(function*() {
      const { agent, session } = yield* withSession(2, { connect: { interactionTimeout: "50 millis" } })
      yield* agent.send(permission(2))
      yield* awaitSnapshot(session, (snapshot) => Object.keys(snapshot.interactions).length === 1)

      const interactionId = Object.keys((yield* session.snapshot).interactions)[0]!
      // Nobody answers; the deadline elapses.
      const observed = yield* session.observe
      yield* TestClock.adjust("50 millis")
      if (observed.snapshot.interactions[interactionId]!.status !== "expired") {
        yield* Stream.runHead(Stream.filter(observed.changes, (event) =>
          event.snapshot.interactions[interactionId]?.status === "expired"))
      }

      // The agent still gets a well-formed response rather than silence.
      const reply = yield* agent.awaitReply("perm-1")
      expect(reply).toMatchObject({ result: { outcome: { outcome: "cancelled" } } })

      // Resolving an expired interaction is refused, distinctly from a
      // duplicate resolution.
      const exit = yield* Effect.exit(session.resolveInteraction(interactionId, { _tag: "selected", optionId: "allow" }))
      expect(failureOf(exit)).toMatchObject({ _tag: "AcpInteractionExpired" })
    })))

  it.effect("an incoming request cancellation settles the interaction as cancelled", () =>
    run(Effect.gen(function*() {
      const { agent, session } = yield* withSession(2)
      yield* agent.send(permission(2))
      yield* awaitSnapshot(session, (snapshot) => Object.keys(snapshot.interactions).length === 1)
      const interactionId = Object.keys((yield* session.snapshot).interactions)[0]!

      // The agent withdraws its own request.
      yield* agent.send({ jsonrpc: "2.0", method: "$/cancel_request", params: { requestId: "perm-1" } })
      yield* awaitSnapshot(session, (snapshot) => snapshot.interactions[interactionId]?.status === "cancelled")

      // Unrelated traffic still flows afterwards.
      yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", messageId: "m-1", content: text("after") })
      yield* awaitSnapshot(session, (snapshot) => snapshot.messages.length === 1)
    })))

  it.effect("an elicitation is exposed as a pending interaction and resolved once", () =>
    run(Effect.gen(function*() {
      const { agent, session } = yield* withElicitationSession(2)
      yield* agent.send({
        jsonrpc: "2.0",
        id: "elicit-1",
        method: "elicitation/create",
        params: { sessionId: "sess-1", message: "Which branch?", mode: "form", requestedSchema: { type: "object", properties: { branch: { type: "string" } } } }
      })
      yield* awaitSnapshot(session, (snapshot) => Object.keys(snapshot.interactions).length === 1)

      const interaction = Object.values((yield* session.snapshot).interactions)[0]!
      expect(interaction.kind).toBe("elicitation")

      yield* session.resolveInteraction(interaction.interactionId, { _tag: "accept", content: { branch: "main" } })
      expect((yield* agent.awaitReply("elicit-1")).result).toEqual({
        action: "accept",
        content: { branch: "main" }
      })

      const again = yield* Effect.exit(session.resolveInteraction(interaction.interactionId, { _tag: "decline" }))
      expect(failureOf(again)).toMatchObject({ _tag: "AcpInteractionAlreadyResolved" })
    })))
})

describe("provisional routing bounds", () => {
  it.effect("overflowing the provisional buffer fails explicitly instead of dropping updates", () =>
    run(Effect.gen(function*() {
      const agent = yield* scriptedAgent({ version: 2, holdNewSession: true })
      const client = yield* Effect.provide(
        AcpClient,
        AcpLocalClient.layer.pipe(Layer.provide(agent.connector))
      )
      const connection = yield* client.connect({
        versions: [2],
        params: { info: { name: "t", version: "1" } },
        // A deliberately tiny bound, to reach it with a few updates.
        provisional: { updates: 2 }
      })

      const opening = yield* Effect.forkChild(connection.newSession({ cwd: "/work" }))
      yield* agent.awaitRequest("session/new")
      for (let index = 0; index < 5; index++) {
        yield* agent.update("sess-1", {
          sessionUpdate: "agent_message_chunk",
          messageId: `m-${index}`,
          content: text(String(index))
        })
      }
      yield* agent.releaseNewSession({ sessionId: "sess-1" })

      // Silently losing required deltas is exactly what the bound forbids.
      const exit = yield* Fiber.await(opening)
      expect(failureOf(exit)).toMatchObject({ _tag: "AcpProvisionalOverflow" })
    })))

  it.effect("overflowed new sessions leave no route under the returned session id", () =>
    run(Effect.gen(function*() {
      const { agent, connection } = yield* harness(2, {
        agent: { version: 2, holdNewSession: true },
        connect: { provisional: { updates: 2 } }
      })
      const first = yield* Effect.forkChild(connection.newSession({ cwd: "/work" }))
      yield* agent.awaitRequest("session/new")
      for (let index = 0; index < 3; index++) {
        yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", messageId: `overflow-${index}`, content: text(String(index)) })
      }
      yield* agent.releaseNewSession({ sessionId: "sess-1" })
      expect(failureOf(yield* Fiber.await(first))).toMatchObject({ _tag: "AcpProvisionalOverflow" })

      const second = yield* Effect.forkChild(connection.newSession({ cwd: "/work" }))
      yield* agent.awaitRequest("session/new")
      yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", messageId: "fresh", content: text("fresh") })
      yield* agent.releaseNewSession({ sessionId: "sess-1" })
      const session = yield* Fiber.join(second)
      expect(hasText(yield* session.snapshot, "fresh")).toBe(true)
    })))

  for (const version of [1, 2] as const) {
    for (const cursor of version === 2 ? ["start", "_cursor"] as const : ["start"] as const) {
      it.effect(`v${version} failed ${cursor} replay leaves prior history unchanged`, () =>
        run(Effect.gen(function*() {
          const { agent, connection, session } = yield* withSession(version, resumeOptions(version))
          const observed = yield* session.observe
          if (version === 2) {
            // State updates are retained in raw, but are not conversation history.
            yield* agent.update("sess-1", { sessionUpdate: "state_update", state: "idle" })
          }
          yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", ...(version === 2 ? { messageId: "history" } : {}), content: text("history") })
          yield* Stream.runCollect(Stream.take(Stream.filter(observed.changes, (event) => hasText(event.snapshot, "history")), 1))

          const resuming = yield* Effect.forkChild(connection.resumeSession({ sessionId: "sess-1", cwd: "/work", replayFrom: { type: cursor } }))
          const method = version === 1 ? "session/load" : "session/resume"
          yield* agent.awaitRequest(method)
          yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", ...(version === 2 ? { messageId: "history" } : {}), content: text("history") })
          yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", ...(version === 2 ? { messageId: "history" } : {}), content: text("history") })
          yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", ...(version === 2 ? { messageId: "fresh" } : {}), content: text("fresh") })
          yield* agent.respondError(method, -32603, "replay failed")
          expect(failureOf(yield* Fiber.await(resuming))).toMatchObject({
            _tag: "AcpRemoteError", code: -32603, message: "replay failed"
          })
          const content = (yield* session.snapshot).messages.flatMap((message) => message.content)
          expect(content.filter((part) => "text" in part && part.text === "history")).toHaveLength(1)
          expect(content.filter((part) => "text" in part && part.text === "fresh")).toHaveLength(0)
          const observedAfter = yield* session.observe
          yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", ...(version === 2 ? { messageId: "after-failure" } : {}), content: text("after-failure") })
          yield* Stream.runCollect(Stream.take(Stream.filter(observedAfter.changes, (event) => hasText(event.snapshot, "after-failure")), 1))
          expect(hasText(yield* session.snapshot, "after-failure")).toBe(true)
        })))
    }

    it.effect(`v${version} failed resume keeps the prior route and live events`, () =>
      run(Effect.gen(function*() {
        const { agent, connection, session } = yield* withSession(version, resumeOptions(version))
        const resuming = yield* Effect.forkChild(connection.resumeSession({ sessionId: "sess-1", cwd: "/work" }))
        yield* agent.awaitRequest("session/resume")
        yield* expectLiveRoute(agent, session, version, `during-resume-v${version}`)
        yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", ...(version === 2 ? { messageId: "during-failure" } : {}), content: text("during-failure") })
        yield* agent.respondError("session/resume", -32603, "resume failed")
        expect(failureOf(yield* Fiber.await(resuming))).toMatchObject({
          _tag: "AcpRemoteError", code: -32603, message: "resume failed"
        })
        yield* expectLiveRoute(agent, session, version, `after-failure-v${version}`)
        const snapshot = yield* session.snapshot
        expect(snapshot.messages.flatMap((message) => message.content).filter((part) => "text" in part && part.text === "during-failure")).toHaveLength(1)
      })))

    it.effect(`v${version} interrupted resume restores the prior route`, () =>
      run(Effect.gen(function*() {
        const { agent, connection, session } = yield* withSession(version, resumeOptions(version))
        const resuming = yield* Effect.forkChild(connection.resumeSession({ sessionId: "sess-1", cwd: "/work" }))
        yield* agent.awaitRequest("session/resume")
        yield* Fiber.interrupt(resuming)
        yield* expectLiveRoute(agent, session, version, `after-interrupt-v${version}`)
      })))

    it.effect(`v${version} resume overflow preserves an eligible prior route`, () =>
      run(Effect.gen(function*() {
        const { agent, connection, session } = yield* withSession(version, {
          ...resumeOptions(version), connect: { provisional: { updates: 2 } }
        })
        const resuming = yield* Effect.forkChild(connection.resumeSession({ sessionId: "sess-1", cwd: "/work" }))
        yield* agent.awaitRequest("session/resume")
        for (let index = 0; index < 3; index++) {
          yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", ...(version === 2 ? { messageId: `replay-${index}` } : {}), content: text(`replay-${index}`) })
        }
        yield* agent.respond("session/resume", {})
        expect(failureOf(yield* Fiber.await(resuming))).toMatchObject({ _tag: "AcpProvisionalOverflow" })
        yield* expectLiveRoute(agent, session, version, `after-overflow-v${version}`)
        const snapshot = yield* session.snapshot
        for (let index = 0; index < 3; index++) expect(hasText(snapshot, `replay-${index}`)).toBe(true)
      })))

    it.effect(`v${version} successful resume owns its route after the old handle releases`, () =>
      run(Effect.gen(function*() {
        const { agent, connection, session: previous } = yield* withSession(version, resumeOptions(version))
        const resuming = yield* Effect.forkChild(connection.resumeSession({ sessionId: "sess-1", cwd: "/work" }))
        yield* agent.awaitRequest("session/resume")
        yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", ...(version === 2 ? { messageId: "resume-replay" } : {}), content: text("resume-replay") })
        yield* agent.respond("session/resume", {})
        const current = yield* Fiber.join(resuming)
        expect(hasText(yield* current.snapshot, "resume-replay")).toBe(true)
        yield* previous.release
        yield* expectLiveRoute(agent, current, version, `after-success-v${version}`)
      })))
  }

  for (const framing of ["batch", "consecutive frames"] as const) {
    it.effect(`an update after a failed replay response reaches the prior runtime in ${framing}`, () =>
      run(Effect.gen(function*() {
        const { agent, connection, session } = yield* withSession(2, resumeOptions(2))
        const observed = yield* session.observe
        yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", messageId: "history", content: text("history") })
        yield* Stream.runCollect(Stream.take(Stream.filter(observed.changes, (event) => hasText(event.snapshot, "history")), 1))
        const resuming = yield* Effect.forkChild(connection.resumeSession({ sessionId: "sess-1", cwd: "/work", replayFrom: { type: "start" } }))
        yield* agent.awaitRequest("session/resume")
        yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", messageId: "history", content: text("history") })
        const request = (yield* agent.received).find((message) => message.method === "session/resume")!
        const response = { jsonrpc: "2.0", id: request.id, error: { code: -32603, message: "replay failed" } }
        const update = { jsonrpc: "2.0", method: "session/update", params: {
          sessionId: "sess-1", update: { sessionUpdate: "agent_message_chunk", messageId: "after-response", content: text("after-response") }
        } }
        if (framing === "batch") yield* agent.send([response, update])
        else {
          yield* agent.send(response)
          yield* agent.send(update)
        }
        expect(failureOf(yield* Fiber.await(resuming))).toMatchObject({
          _tag: "AcpRemoteError", code: -32603, message: "replay failed"
        })
        yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", messageId: "sentinel", content: text("sentinel") })
        yield* awaitSnapshot(session, (snapshot) => hasText(snapshot, "sentinel"))
        const content = (yield* session.snapshot).messages.flatMap((message) => message.content)
        expect(content.filter((part) => "text" in part && part.text === "history")).toHaveLength(1)
        expect(content.filter((part) => "text" in part && part.text === "after-response")).toHaveLength(1)
      })))
  }

  it.effect("failed replay does not duplicate history evicted from raw updates", () =>
    run(Effect.gen(function*() {
      const { agent, connection, session } = yield* withSession(2, {
        ...resumeOptions(2), connect: { limits: { rawUpdates: 1 } }
      })
      const observed = yield* session.observe
      for (const value of ["A", "B"]) {
        yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", messageId: value, content: text(value) })
      }
      yield* Stream.runCollect(Stream.take(Stream.filter(observed.changes, (event) => hasText(event.snapshot, "B")), 1))
      expect((yield* session.snapshot).raw).toHaveLength(1)

      const resuming = yield* Effect.forkChild(connection.resumeSession({ sessionId: "sess-1", cwd: "/work", replayFrom: { type: "start" } }))
      yield* agent.awaitRequest("session/resume")
      for (const value of ["A", "B"]) {
        yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", messageId: value, content: text(value) })
      }
      yield* agent.respondError("session/resume", -32603, "replay failed")
      expect(failureOf(yield* Fiber.await(resuming))).toMatchObject({
        _tag: "AcpRemoteError", code: -32603, message: "replay failed"
      })
      const content = (yield* session.snapshot).messages.flatMap((message) => message.content)
      for (const value of ["A", "B"]) {
        expect(content.filter((part) => "text" in part && part.text === value)).toHaveLength(1)
      }
    })))

  for (const version of [1, 2] as const) {
    it.effect(`v${version} successful explicit replay promotes the buffered candidate`, () =>
      run(Effect.gen(function*() {
        const { agent, connection, session: previous } = yield* withSession(version, resumeOptions(version))
        const resuming = yield* Effect.forkChild(connection.resumeSession({ sessionId: "sess-1", cwd: "/work", replayFrom: { type: "start" } }))
        const method = version === 1 ? "session/load" : "session/resume"
        yield* agent.awaitRequest(method)
        yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", ...(version === 2 ? { messageId: "replayed" } : {}), content: text("replayed") })
        yield* agent.respond(method, {})
        const current = yield* Fiber.join(resuming)
        expect(hasText(yield* current.snapshot, "replayed")).toBe(true)
        expect(hasText(yield* previous.snapshot, "replayed")).toBe(false)
      })))
  }

  it.effect("releasing the prior session during resume prevents route restoration", () =>
    run(Effect.gen(function*() {
      const { agent, connection, session } = yield* withSession(2, resumeOptions(2))
      const resuming = yield* Effect.forkChild(connection.resumeSession({ sessionId: "sess-1", cwd: "/work" }))
      yield* agent.awaitRequest("session/resume")
      const observed = yield* session.observe
      yield* agent.send({
        jsonrpc: "2.0", id: "released-inflight-permission", method: "session/request_permission",
        params: { sessionId: "sess-1", title: "Edit file", options: [{ optionId: "allow", name: "Allow", kind: "allow_once" }] }
      })
      yield* Stream.runCollect(Stream.take(Stream.filter(observed.changes, (event) =>
        Object.values(event.snapshot.interactions).some((interaction) => interaction.status === "pending")
      ), 1))
      yield* session.release
      expect(yield* agent.awaitReply("released-inflight-permission")).toMatchObject({ result: { outcome: { outcome: "cancelled" } } })
      yield* agent.respondError("session/resume", -32603, "resume failed")
      expect(failureOf(yield* Fiber.await(resuming))).toMatchObject({
        _tag: "AcpRemoteError", code: -32603, message: "resume failed"
      })
      yield* agent.send({
        jsonrpc: "2.0", id: "released-permission", method: "session/request_permission",
        params: { sessionId: "sess-1", title: "Edit file", options: [{ optionId: "allow", name: "Allow", kind: "allow_once" }] }
      })
      expect(yield* agent.awaitReply("released-permission")).toMatchObject({ error: { code: -32602 } })
    })))

  for (const version of [1, 2] as const) {
    for (const kind of ["permission", "elicitation"] as const) {
      it.effect(`v${version} ordinary release answers a pending ${kind} with cancellation`, () =>
        run(Effect.gen(function*() {
          const { agent, session } = yield* (kind === "elicitation" ? withElicitationSession(version) : withSession(version))
          const observed = yield* session.observe
          const id = `release-${kind}`
          yield* agent.send({
            jsonrpc: "2.0", id,
            method: kind === "permission" ? "session/request_permission" : "elicitation/create",
            params: kind === "permission"
              ? { sessionId: "sess-1", title: "Edit file",
                ...(version === 1 ? { toolCall: { toolCallId: "t-1", title: "Edit file" } } : {}),
                options: [{ optionId: "allow", name: "Allow", kind: "allow_once" }] }
              : { sessionId: "sess-1", message: "Continue?", mode: "form",
                requestedSchema: { type: "object", properties: { answer: { type: "string" } } } }
          })
          yield* Stream.runCollect(Stream.take(Stream.filter(observed.changes, (event) =>
            Object.values(event.snapshot.interactions).some((interaction) => interaction.status === "pending")
          ), 1))
          yield* session.release
          const reply = yield* agent.awaitReply(id)
          expect(reply).toMatchObject(kind === "permission"
            ? { result: { outcome: { outcome: "cancelled" } } }
            : { result: { action: "cancel" } })
          if (kind === "permission") {
            if (version === 1) yield* Schema.decodeUnknownEffect(V1.RequestPermissionResponse)(reply["result"])
            else yield* Schema.decodeUnknownEffect(V2.RequestPermissionResponse)(reply["result"])
          } else {
            if (version === 1) yield* Schema.decodeUnknownEffect(V1.CreateElicitationResponse)(reply["result"])
            else yield* Schema.decodeUnknownEffect(V2.CreateElicitationResponse)(reply["result"])
          }
        })))
    }
  }

  for (const version of [1, 2] as const) {
    it.effect(`v${version} permission admitted during resume receives a response after promotion`, () =>
      run(Effect.gen(function*() {
        const { agent, connection, session } = yield* withSession(version, resumeOptions(version))
        const resuming = yield* Effect.forkChild(connection.resumeSession({ sessionId: "sess-1", cwd: "/work" }))
        yield* agent.awaitRequest("session/resume")
        const observed = yield* session.observe
        yield* agent.send({
          jsonrpc: "2.0", id: "promotion-permission", method: "session/request_permission",
          params: {
            sessionId: "sess-1", title: "Edit file",
            ...(version === 1 ? { toolCall: { toolCallId: "t-1", title: "Edit file" } } : {}),
            options: [{ optionId: "allow", name: "Allow", kind: "allow_once" }]
          }
        })
        yield* Stream.runCollect(Stream.take(Stream.filter(observed.changes, (event) =>
          Object.values(event.snapshot.interactions).some((interaction) => interaction.status === "pending")
        ), 1))
        yield* agent.respond("session/resume", {})
        yield* Fiber.join(resuming)
        expect(yield* agent.awaitReply("promotion-permission")).toMatchObject({ result: { outcome: { outcome: "cancelled" } } })
      })))
  }
})

describe("resource ownership", () => {
  it.effect("closing the client scope releases the connection and its sessions", () =>
    run(Effect.gen(function*() {
      const outer = yield* Scope.Scope
      const clientScope = yield* Scope.fork(outer)
      const { session } = yield* Scope.provide(withSession(2), clientScope)
      yield* Scope.close(clientScope, Exit.void)
      // The handle survives as a value; the runtime behind it is gone.
      expect(session.sessionId).toBe("sess-1")
    })))
})

it.effect("v1 acceptance is immediately unavailable while the turn is still running", () => run(Effect.gen(function*() {
  const { agent, session } = yield* withSession(1)
  const submission = yield* session.submit([text("wait")])
  expect(failureOf(yield* Effect.exit(submission.accepted.pipe(Effect.timeout("50 millis"))))).toMatchObject({ _tag: "AcpCapabilityUnsupported" })
  yield* agent.awaitRequest("session/prompt")
  expect((yield* agent.received).some((m) => m.method === "session/prompt")).toBe(true)
})))

it.effect("malformed prompt validation does not leave a phantom busy session", () => run(Effect.gen(function*() {
  const { agent, session } = yield* withSession(2)
  // Deliberately invalid input from an untyped caller: text requires `text`.
  expect(failureOf(yield* Effect.exit(session.submit([{ type: "text" } as never])))).toMatchObject({ _tag: "AcpProtocolError" })
  const valid = yield* session.submit([text("valid")])
  yield* agent.awaitRequest("session/prompt")
  yield* completePrompt(agent, 2)
  expect((yield* valid.outcome).status._tag).toBe("completed")
})))

it.effect("absent v2 session surface fails before sending session/new", () => run(Effect.gen(function*() {
  const { agent, connection } = yield* harness(2, { agent: { version: 2, initialize: { protocolVersion: 2, info: { name: "extensions", version: "1" }, capabilities: {} } } })
  expect(failureOf(yield* Effect.exit(connection.newSession({ cwd: "/work" })))).toMatchObject({ _tag: "AcpCapabilityUnsupported" })
  expect((yield* agent.received).some((m) => m.method === "session/new")).toBe(false)
})))

it.effect("invalid permission selection leaves the pending request available for a valid answer", () => run(Effect.gen(function*() {
  const { agent, session } = yield* withSession(2)
  yield* agent.send({ jsonrpc: "2.0", id: "p", method: "session/request_permission", params: { sessionId: "sess-1", title: "Permission", options: [{ optionId: "allow", name: "Allow", kind: "allow_once" }] } })
  yield* awaitSnapshot(session, (snapshot) => Object.keys(snapshot.interactions).length === 1)
  const id = Object.keys((yield* session.snapshot).interactions)[0]!
  expect(failureOf(yield* Effect.exit(session.resolveInteraction(id, { _tag: "selected", optionId: "invented" })))).toMatchObject({ _tag: "AcpProtocolError" })
  expect((yield* session.snapshot).interactions[id]!.status).toBe("pending")
  yield* session.resolveInteraction(id, { _tag: "selected", optionId: "allow" })
})))

it.effect("submission handles retain their completed record after snapshot retention evicts it", () => run(Effect.gen(function*() {
  const { agent, session } = yield* withSession(2, { connect: { limits: { submissions: 1 } } })
  const first = yield* session.submit([text("first")])
  yield* agent.awaitRequest("session/prompt"); yield* completePrompt(agent, 2); yield* first.outcome
  const second = yield* session.submit([text("second")])
  yield* agent.awaitRequest("session/prompt"); yield* completePrompt(agent, 2); yield* second.outcome
  expect((yield* session.snapshot).submissions[first.id]).toBeUndefined()
  expect((yield* first.snapshot).status._tag).toBe("completed")
})))

it.effect("v1 outcome includes updates queued before the prompt response", () => run(Effect.gen(function*() {
  const { agent, session } = yield* withSession(1)
  const submission = yield* session.submit([text("hello")])
  yield* agent.awaitRequest("session/prompt")
  const request = (yield* agent.received).find((message) => message.method === "session/prompt")
  expect(request).toBeDefined()
  yield* agent.send([
    { jsonrpc: "2.0", method: "session/update", params: { sessionId: session.sessionId,
      update: { sessionUpdate: "agent_message_chunk", content: text("first ") } } },
    { jsonrpc: "2.0", method: "session/update", params: { sessionId: session.sessionId,
      update: { sessionUpdate: "agent_message_chunk", content: text("last") } } },
    { jsonrpc: "2.0", id: request?.id, result: { stopReason: "end_turn" } }
  ])
  yield* submission.outcome
  const snapshot = yield* session.snapshot
  expect(snapshot.messages.flatMap((message) => message.content)).toEqual([text("first "), text("last")])
})))

it.effect("second v1 turn resets idle state and cancellation waits for its prompt response", () => run(Effect.gen(function*() {
  const { agent, session } = yield* withSession(1)
  const first = yield* session.submit([text("first")])
  yield* agent.awaitRequest("session/prompt")
  yield* completePrompt(agent, 1)
  yield* first.outcome

  const second = yield* session.submit([text("second")])
  yield* agent.awaitRequest("session/prompt")
  expect((yield* session.snapshot).foreground).toEqual({ state: "running", provenance: "inferred" })
  const cancelArrived = yield* Effect.forkChild(agent.awaitRequest("session/cancel"))
  const cancelling = yield* Effect.forkChild(session.cancel)
  yield* Fiber.join(cancelArrived)
  yield* agent.update(session.sessionId, {
    sessionUpdate: "agent_message_chunk", content: text("Still stopping")
  })
  yield* awaitSnapshot(session, (snapshot) => hasText(snapshot, "Still stopping"))
  expect(cancelling.pollUnsafe()).toBeUndefined()
  expect((yield* session.snapshot).activeSubmissionId).toBe(second.id)

  yield* agent.respond("session/prompt", { stopReason: "cancelled" })
  yield* Fiber.join(cancelling)
  expect((yield* session.snapshot).activeSubmissionId).toBeNull()
  expect((yield* session.snapshot).foreground).toEqual({ state: "idle", stopReason: "cancelled" })
})))

describe("explicit history replay", () => {
  it.effect("v1 replay uses load even when resume is advertised", () => run(Effect.gen(function*() {
    const { agent, connection } = yield* harness(1, { agent: {
      version: 1,
      initialize: { protocolVersion: 1, agentCapabilities: { loadSession: true, sessionCapabilities: { resume: {} } } }
    } })
    const loading = yield* Effect.forkChild(connection.resumeSession({ sessionId: "sess-1", cwd: "/work", replayFrom: { type: "start" } }))
    yield* agent.awaitRequest("session/load")
    yield* agent.update("sess-1", { sessionUpdate: "user_message_chunk", content: text("old question") })
    yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", content: text("old answer") })
    yield* agent.respond("session/load", {})
    const session = yield* Fiber.join(loading)
    expect((yield* session.snapshot).messages.map(message => message.content)).toEqual([[text("old question")], [text("old answer")]])
    const next = yield* session.submit([text("next question")])
    yield* agent.awaitRequest("session/prompt")
    yield* agent.update("sess-1", { sessionUpdate: "agent_message_chunk", content: text("next answer") })
    yield* agent.respond("session/prompt", { stopReason: "end_turn" })
    yield* next.outcome
    expect((yield* session.snapshot).messages.map(message => message.content)).toEqual([[text("old question")], [text("old answer")], [text("next answer")]])
    expect((yield* agent.received).filter(message => message.method === "session/resume")).toHaveLength(0)
  })))

  it.effect("v1 cannot silently omit requested history or downgrade a cursor", () => run(Effect.gen(function*() {
    const { agent, connection } = yield* harness(1, { agent: {
      version: 1,
      initialize: { protocolVersion: 1, agentCapabilities: { sessionCapabilities: { resume: {} } } }
    } })
    for (const type of ["start", "_cursor"]) {
      const result = yield* Effect.exit(connection.resumeSession({ sessionId: "sess-1", cwd: "/work", replayFrom: { type } }))
      expect(failureOf(result)).toMatchObject({ _tag: "AcpHistoryUnavailable" })
    }
    expect((yield* agent.received).filter(message => message.method === "session/resume" || message.method === "session/load")).toHaveLength(0)
  })))

  it.effect("v1 resume without requested history keeps its existing behavior", () => run(Effect.gen(function*() {
    const { agent, connection } = yield* harness(1, { agent: {
      version: 1,
      initialize: { protocolVersion: 1, agentCapabilities: { loadSession: true, sessionCapabilities: { resume: {} } } }
    } })
    const resuming = yield* Effect.forkChild(connection.resumeSession({ sessionId: "sess-1", cwd: "/work" }))
    yield* agent.awaitRequest("session/resume")
    yield* agent.respond("session/resume", {})
    yield* Fiber.join(resuming)
    expect((yield* agent.received).filter(message => message.method === "session/load")).toHaveLength(0)
  })))
})
