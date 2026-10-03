/**
 * Pure reducer behavior: v2 patch/chunk/replacement semantics, v1 adaptation,
 * submission lifecycles, and retention bounds. No transport is involved, so
 * these pin the interpretation rules on their own.
 */
import { describe, expect, it } from "@effect/vitest"
import { defaultContentLimits, SessionSnapshot, TerminalSnapshot, type SubmissionSnapshot } from "../src/AcpApp.ts"
import * as Schema from "effect/Schema"
import * as V1 from "../src/protocol/v1/Schema.ts"
import * as V2 from "../src/protocol/v2/Schema.ts"
import * as State from "../src/AcpSessionState.ts"

const v2 = () => State.empty("sess-1", 2)
const v1 = () => State.empty("sess-1", 1)

const update = (update: unknown): State.Event => ({ _tag: "update", update })
const text = (value: string) => ({ type: "text", text: value })

const submission = (id: string, overrides: Partial<SubmissionSnapshot> = {}): SubmissionSnapshot => ({
  id,
  prompt: [text("hi")],
  status: { _tag: "pending" },
  requestId: null,
  agentMessageId: null,
  acceptanceUnavailable: false,
  foreground: "inferred",
  ...overrides
})

describe("versioned tool and terminal projections", () => {
  it("preserves mixed v1 content and diffs on introduction and replacement", () => {
    const diff = { type: "diff", path: "/work/a.ts", oldText: "old", newText: "new" }
    const content = [{ type: "content", content: text("result") }, diff]
    const initial = { sessionUpdate: "tool_call", toolCallId: "edit", title: "Edit", content }
    expect(Schema.is(V1.SessionUpdate)(initial)).toBe(true)
    const seeded = State.reduce(v1(), update(initial))
    expect(seeded.toolCalls.edit!.content).toEqual(content)
    const replaced = State.reduce(seeded, update({ sessionUpdate: "tool_call_update", toolCallId: "edit", content: [diff] }))
    expect(replaced.toolCalls.edit!.content).toEqual([diff])
    expect(Schema.is(SessionSnapshot)(replaced)).toBe(true)
  })

  it("keeps v1 null and omitted names, while concrete names replace", () => {
    const seeded = State.reduce(v1(), update({ sessionUpdate: "tool_call", toolCallId: "edit", title: "Edit", name: "edit_file" }))
    const patch = { sessionUpdate: "tool_call_update", toolCallId: "edit", name: null }
    expect(Schema.is(V1.SessionUpdate)(patch)).toBe(true)
    const unchanged = State.reduceAll(seeded, [update(patch), update({ sessionUpdate: "tool_call_update", toolCallId: "edit", status: "completed" })])
    expect(unchanged.toolCalls.edit!.name).toBe("edit_file")
    const replaced = State.reduce(unchanged, update({ sessionUpdate: "tool_call_update", toolCallId: "edit", name: "write_file" }))
    expect(replaced.toolCalls.edit!.name).toBe("write_file")
  })

  it("represents unknown terminal exits independently of nullable exit details", () => {
    const seeded = State.reduce(v2(), update({ sessionUpdate: "terminal_update", terminalId: "tty", command: "run" }))
    expect(seeded.terminals.tty!.exited).toBe(false)
    for (const exitStatus of [{}, { exitCode: null, signal: null }]) {
      const patch = { sessionUpdate: "terminal_update", terminalId: "tty", exitStatus }
      expect(Schema.is(V2.SessionUpdate)(patch)).toBe(true)
      const exited = State.reduce(seeded, update(patch))
      expect(exited.terminals.tty).toMatchObject({ exited: true, exitCode: null, exitSignal: null })
      const appended = State.reduce(exited, update({ sessionUpdate: "terminal_output_chunk", terminalId: "tty", data: "YQ==" }))
      expect(appended.terminals.tty!.exited).toBe(true)
      const preserved = State.reduce(appended, update({ sessionUpdate: "terminal_update", terminalId: "tty", cwd: "/work" }))
      expect(preserved.terminals.tty!.exited).toBe(true)
      const reset = State.reduce(preserved, update({ sessionUpdate: "terminal_update", terminalId: "tty", exitStatus: null }))
      expect(reset.terminals.tty!.exited).toBe(false)
    }
    const legacy = { ...seeded.terminals.tty! }
    delete legacy.exited
    expect(Schema.decodeSync(TerminalSnapshot)(legacy).exited).toBe(false)
  })
})

describe("v2 messages", () => {
  it("chunks append in order under one message id", () => {
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "agent_message_chunk", messageId: "m-1", content: text("a") }),
      update({ sessionUpdate: "agent_message_chunk", messageId: "m-1", content: text("b") })
    ])
    expect(next.messages).toHaveLength(1)
    expect(next.messages[0]!.content).toEqual([text("a"), text("b")])
    expect(next.messages[0]!.provenance).toEqual({ _tag: "agent", agentId: "m-1" })
  })

  // Spec: "Replace accumulated chunks".
  it("a whole-message update replaces prior chunks, then later chunks append", () => {
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "agent_message_chunk", messageId: "m-1", content: text("draft") }),
      update({ sessionUpdate: "agent_message", messageId: "m-1", content: [text("final")] }),
      update({ sessionUpdate: "agent_message_chunk", messageId: "m-1", content: text("!") })
    ])
    expect(next.messages).toHaveLength(1)
    expect(next.messages[0]!.content).toEqual([text("final"), text("!")])
  })

  it("an explicit null content clears the message", () => {
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "agent_message_chunk", messageId: "m-1", content: text("a") }),
      update({ sessionUpdate: "agent_message", messageId: "m-1", content: null })
    ])
    expect(next.messages[0]!.content).toEqual([])
  })

  it("distinct message ids are distinct messages", () => {
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "agent_message_chunk", messageId: "m-1", content: text("a") }),
      update({ sessionUpdate: "agent_message_chunk", messageId: "m-2", content: text("b") })
    ])
    expect(next.messages.map((message) => message.id)).toEqual(["m-1", "m-2"])
  })

  it("user, agent, and thought messages keep their kinds", () => {
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "user_message", messageId: "u-1", content: [text("q")] }),
      update({ sessionUpdate: "agent_message", messageId: "a-1", content: [text("r")] }),
      update({ sessionUpdate: "agent_thought", messageId: "t-1", content: [text("hmm")] })
    ])
    expect(next.messages.map((message) => message.kind)).toEqual(["user", "agent", "thought"])
  })
})

describe("v2 tool calls", () => {
  it("prototype-named tool calls start fresh and retain their chunks", () => {
    for (const id of ["constructor", "toString", "__proto__"]) {
      const next = State.reduceAll(v2(), [
        update({ sessionUpdate: "tool_call_update", toolCallId: id, title: "Run" }),
        update({ sessionUpdate: "tool_call_content_chunk", toolCallId: id, content: { type: "content", content: text("done") } })
      ])
      expect(Object.hasOwn(next.toolCalls, id)).toBe(true)
      expect(next.toolCalls[id]).toMatchObject({ toolCallId: id, title: "Run", content: [{ type: "content", content: text("done") }] })
      expect(Object.getPrototypeOf(next.toolCalls)).toBe(Object.prototype)
    }
  })

  it("omitted fields are unchanged, null clears, values replace", () => {
    const seeded = State.reduce(
      v2(),
      update({
        sessionUpdate: "tool_call_update",
        toolCallId: "t-1",
        title: "Edit",
        name: "edit_file",
        status: "pending",
        rawInput: { path: "a.ts" }
      })
    )
    const next = State.reduce(
      seeded,
      // `title` omitted, `name` explicitly cleared, `status` replaced.
      update({ sessionUpdate: "tool_call_update", toolCallId: "t-1", name: null, status: "completed" })
    )
    const call = next.toolCalls["t-1"]!
    expect(call.title).toBe("Edit")
    expect(call.name).toBeNull()
    expect(call.status).toBe("completed")
    expect(call.rawInput).toEqual({ path: "a.ts" })
  })

  it("content chunks append to the tool call", () => {
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "tool_call_update", toolCallId: "t-1", title: "Run" }),
      update({ sessionUpdate: "tool_call_content_chunk", toolCallId: "t-1", content: { type: "content", content: text("1") } }),
      update({ sessionUpdate: "tool_call_content_chunk", toolCallId: "t-1", content: { type: "content", content: text("2") } })
    ])
    expect(next.toolCalls["t-1"]!.content).toHaveLength(2)
  })

  it("a full content update replaces accumulated chunks", () => {
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "tool_call_content_chunk", toolCallId: "t-1", content: { type: "content", content: text("1") } }),
      update({ sessionUpdate: "tool_call_update", toolCallId: "t-1", content: [{ type: "content", content: text("final") }] })
    ])
    expect(next.toolCalls["t-1"]!.content).toEqual([{ type: "content", content: text("final") }])
  })
})

describe("v2 terminals", () => {
  const base64 = (value: string) => btoa(value)

  it("prototype-named terminals keep decoded output", () => {
    for (const id of ["constructor", "toString", "__proto__"]) {
      const next = State.reduceAll(v2(), [
        update({ sessionUpdate: "terminal_update", terminalId: id, output: { data: base64("A") } }),
        update({ sessionUpdate: "terminal_output_chunk", terminalId: id, data: base64("B") })
      ])
      expect(Object.hasOwn(next.terminals, id)).toBe(true)
      expect(next.terminals[id]!.outputBytes).toEqual([65, 66])
      expect(Object.getPrototypeOf(next.terminals)).toBe(Object.prototype)
    }
  })

  it("base64 keeps accepted unpadded and whitespace forms and ignores invalid chunks", () => {
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "terminal_update", terminalId: "term-1", output: { data: " Y Q = = " } }),
      update({ sessionUpdate: "terminal_output_chunk", terminalId: "term-1", data: "Yg" }),
      update({ sessionUpdate: "terminal_output_chunk", terminalId: "term-1", data: "invalid!" }),
      update({ sessionUpdate: "terminal_output_chunk", terminalId: "term-1", data: "/w==" })
    ])
    expect(next.terminals["term-1"]!.outputBytes).toEqual([97, 98, 255])
    expect(next.raw).toHaveLength(4)
  })

  // Spec: "Terminal snapshot and byte chunks".
  it("a replacement snapshot replaces output, then chunks append decoded bytes", () => {
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "terminal_output_chunk", terminalId: "term-1", data: base64("stale") }),
      update({ sessionUpdate: "terminal_update", terminalId: "term-1", command: "ls", output: { data: base64("A") } }),
      update({ sessionUpdate: "terminal_output_chunk", terminalId: "term-1", data: base64("B") }),
      update({ sessionUpdate: "terminal_output_chunk", terminalId: "term-1", data: base64("C") })
    ])
    const terminal = next.terminals["term-1"]!
    expect(String.fromCharCode(...terminal.outputBytes)).toBe("ABC")
    expect(terminal.command).toBe("ls")
  })

  it("each chunk is decoded independently, not as concatenated base64", () => {
    // "a" and "b" each encode to 4 base64 chars with padding; concatenating
    // the text before decoding would produce garbage rather than "ab".
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "terminal_output_chunk", terminalId: "term-1", data: base64("a") }),
      update({ sessionUpdate: "terminal_output_chunk", terminalId: "term-1", data: base64("b") })
    ])
    expect(String.fromCharCode(...next.terminals["term-1"]!.outputBytes)).toBe("ab")
  })

  it("exit status marks the terminal exited", () => {
    const next = State.reduce(
      v2(),
      update({ sessionUpdate: "terminal_update", terminalId: "term-1", exitStatus: { exitCode: 3, signal: null } })
    )
    expect(next.terminals["term-1"]!.exitCode).toBe(3)
  })

  // Spec: "Transcript exceeds retained-content budget".
  it("output beyond the byte budget is truncated and reported", () => {
    const limits = { ...defaultContentLimits, terminalBytes: 4 }
    const next = State.reduceAll(
      v2(),
      [
        update({ sessionUpdate: "terminal_output_chunk", terminalId: "term-1", data: base64("abcd") }),
        update({ sessionUpdate: "terminal_output_chunk", terminalId: "term-1", data: base64("ef") })
      ],
      limits
    )
    const terminal = next.terminals["term-1"]!
    expect(terminal.outputBytes).toHaveLength(4)
    expect(String.fromCharCode(...terminal.outputBytes)).toBe("cdef")
    expect(terminal.outputTruncated).toBe(true)
    expect(next.truncated.terminals).toEqual(["term-1"])
  })
})

describe("v2 plans, commands, config, usage, info", () => {
  it("a plan update replaces its entries by plan id", () => {
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "plan_update", plan: { type: "items", planId: "p-1", entries: [{ content: "one", priority: "high", status: "pending" }] } }),
      update({ sessionUpdate: "plan_update", plan: { type: "items", planId: "p-1", entries: [{ content: "two", priority: "low", status: "completed" }] } })
    ])
    expect(next.plans["p-1"]!.entries).toEqual([{ content: "two", priority: "low", status: "completed" }])
  })

  it("an unknown plan variant stays observable without inventing entries", () => {
    const next = State.reduce(
      v2(),
      update({ sessionUpdate: "plan_update", plan: { type: "_custom", planId: "p-9" } })
    )
    expect(next.plans["p-9"]!.entries).toBeNull()
    expect(next.plans["p-9"]!.variant).toEqual({ type: "_custom", planId: "p-9" })
  })

  it("available commands are replaced wholesale", () => {
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "available_commands_update", availableCommands: [{ name: "a", description: "" }] }),
      update({ sessionUpdate: "available_commands_update", availableCommands: [{ name: "b", description: "" }] })
    ])
    expect(next.commands.map((command) => command.name)).toEqual(["b"])
  })

  it("v2 config options key on configId and replace the whole set", () => {
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "config_option_update", configOptions: [{ configId: "model", name: "Model", type: "select", currentValue: "a", options: [{ value: "a", name: "A" }] }] }),
      update({ sessionUpdate: "config_option_update", configOptions: [{ configId: "depth", name: "Depth", type: "boolean", currentValue: false }] })
    ])
    expect(Object.keys(next.config)).toEqual(["depth"])
  })

  it("prototype-named config options are own entries", () => {
    for (const id of ["constructor", "toString", "__proto__"]) {
      const next = State.reduce(v2(), update({ sessionUpdate: "config_option_update", configOptions: [
        { configId: id, name: "Mode", type: "boolean", currentValue: false }
      ] }))
      expect(Object.keys(next.config)).toEqual([id])
      expect(next.config[id]!.key).toBe(id)
      expect(Object.getPrototypeOf(next.config)).toBe(Object.prototype)
    }
  })

  it("usage and session info are projected", () => {
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "usage_update", used: 10, size: 100, cost: { amount: 1, currency: "USD" } }),
      update({ sessionUpdate: "session_info_update", title: "Fix bug", updatedAt: "2026-01-01T00:00:00Z" })
    ])
    expect(next.usage).toEqual({ used: 10, size: 100, cost: { amount: 1, currency: "USD" } })
    expect(next.metadata.title).toBe("Fix bug")
  })

  it("session info null clears the title but omission keeps it", () => {
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "session_info_update", title: "Set" }),
      update({ sessionUpdate: "session_info_update", updatedAt: "later" }),
      update({ sessionUpdate: "session_info_update", title: null })
    ])
    expect(next.metadata.updatedAt).toBe("later")
    expect(next.metadata.title).toBeNull()
  })
})

describe("raw updates and unknown variants", () => {
  it("malformed known updates stay raw without creating projected records", () => {
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "terminal_update", command: "missing terminal id" }),
      update({ sessionUpdate: "tool_call_update", toolCallId: 12, title: "invalid id" }),
      update({ sessionUpdate: "agent_message_chunk", content: text("missing message id") }),
      update({ sessionUpdate: "usage_update", used: "invalid", size: 100 })
    ])
    expect(next.raw).toHaveLength(4)
    expect(next.terminals).toEqual({})
    expect(next.toolCalls).toEqual({})
    expect(next.messages).toEqual([])
    expect(next.usage).toBeNull()
  })

  it("every update is retained raw, including undecodable ones", () => {
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "agent_message_chunk", messageId: "m-1", content: text("a") }),
      update({ sessionUpdate: "_vendor/extension", anything: true }),
      update("not an object")
    ])
    expect(next.raw.map((record) => record.kind)).toEqual([
      "agent_message_chunk",
      "_vendor/extension",
      "undecodable"
    ])
    expect(next.raw.every((record) => record.version === 2)).toBe(true)
  })

  it("an unknown variant does not corrupt known state", () => {
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "agent_message", messageId: "m-1", content: [text("kept")] }),
      update({ sessionUpdate: "future_variant", messageId: "m-1", content: [] })
    ])
    expect(next.messages[0]!.content).toEqual([text("kept")])
  })
})

describe("v1 adaptation", () => {
  it("command input hints are adapted to the v2 text-input variant", () => {
    const next = State.reduce(v1(), update({
      sessionUpdate: "available_commands_update",
      availableCommands: [{ name: "search", description: "Search files", input: { hint: "pattern" } }]
    }))
    expect(next.commands).toEqual([{
      name: "search", description: "Search files", input: { type: "text", hint: "pattern" }
    }])
  })

  // Spec: "Missing message ID during replay".
  it("chunks without message ids get local identities, never agent ones", () => {
    const next = State.reduceAll(v1(), [
      update({ sessionUpdate: "agent_message_chunk", content: text("a") }),
      update({ sessionUpdate: "agent_message_chunk", content: text("b") })
    ])
    expect(next.messages).toHaveLength(1)
    expect(next.messages[0]!.provenance).toEqual({ _tag: "local" })
    expect(next.messages[0]!.content).toEqual([text("a"), text("b")])
  })

  it("a role change starts a new local message rather than merging by content", () => {
    const next = State.reduceAll(v1(), [
      update({ sessionUpdate: "agent_message_chunk", content: text("same") }),
      update({ sessionUpdate: "user_message_chunk", content: text("same") }),
      update({ sessionUpdate: "agent_message_chunk", content: text("same") })
    ])
    // Identical text across roles must not be folded into one message.
    expect(next.messages).toHaveLength(3)
    expect(next.messages.map((message) => message.kind)).toEqual(["agent", "user", "agent"])
    expect(new Set(next.messages.map((message) => message.id)).size).toBe(3)
  })

  it("tool_call introduces and tool_call_update merges, keeping agent tool ids", () => {
    const next = State.reduceAll(v1(), [
      update({ sessionUpdate: "tool_call", toolCallId: "t-1", title: "Edit", status: "pending" }),
      update({ sessionUpdate: "tool_call_update", toolCallId: "t-1", status: "completed" })
    ])
    const call = next.toolCalls["t-1"]!
    expect(call.title).toBe("Edit")
    expect(call.status).toBe("completed")
    expect(call.provenance).toEqual({ _tag: "agent", agentId: "t-1" })
  })

  it("v1 has a single implicit plan, marked local", () => {
    const next = State.reduceAll(v1(), [
      update({ sessionUpdate: "plan", entries: [{ content: "one", priority: "high", status: "pending" }] }),
      update({ sessionUpdate: "plan", entries: [{ content: "two", priority: "low", status: "completed" }] })
    ])
    expect(Object.keys(next.plans)).toEqual(["local-plan"])
    expect(next.plans["local-plan"]!.provenance).toEqual({ _tag: "local" })
    expect(next.plans["local-plan"]!.entries).toHaveLength(1)
  })

  it("v1 config options key on id and modes surface under a reserved key", () => {
    const next = State.reduceAll(v1(), [
      update({ sessionUpdate: "config_option_update", configOptions: [{ id: "model", name: "Model", type: "select", currentValue: "a", options: [{ value: "a", name: "A" }] }] }),
      update({ sessionUpdate: "current_mode_update", currentModeId: "architect" })
    ])
    expect(next.config["model"]).toBeDefined()
    expect(next.config["acp/currentMode"]!.option).toMatchObject({ currentModeId: "architect" })
  })

  it("raw records carry the v1 version", () => {
    const next = State.reduce(v1(), update({ sessionUpdate: "plan", entries: [] }))
    expect(next.raw[0]!.version).toBe(1)
  })
})

describe("submissions", () => {
  it("prototype-named submission IDs update only registered entries", () => {
    for (const id of ["constructor", "toString", "__proto__"]) {
      const missing = State.reduce(v2(), { _tag: "submissionDispatched", id, requestId: 7 })
      expect(Object.hasOwn(missing.submissions, id)).toBe(false)
      const next = State.reduceAll(v2(), [
        { _tag: "submissionRegistered", submission: submission(id) },
        { _tag: "submissionDispatched", id, requestId: 7 }
      ])
      expect(next.submissions[id]).toMatchObject({ id, status: { _tag: "dispatched" }, requestId: 7 })
      expect(Object.getPrototypeOf(next.submissions)).toBe(Object.prototype)
    }
  })
  it("v2 acceptance records the agent message id without completing foreground", () => {
    const next = State.reduceAll(v2(), [
      { _tag: "submissionRegistered", submission: submission("s-1") },
      { _tag: "submissionDispatched", id: "s-1", requestId: 7 },
      { _tag: "submissionAccepted", id: "s-1", agentMessageId: "m-1" }
    ])
    expect(next.submissions["s-1"]!.status).toEqual({ _tag: "accepted" })
    expect(next.submissions["s-1"]!.agentMessageId).toBe("m-1")
    // Acceptance is insertion only; foreground work is still outstanding.
    expect(next.activeSubmissionId).toBe("s-1")
  })

  // Spec: "User update precedes acknowledgement".
  it("a user update before the response does not duplicate on acceptance", () => {
    const next = State.reduceAll(v2(), [
      { _tag: "submissionRegistered", submission: submission("s-1") },
      { _tag: "submissionDispatched", id: "s-1", requestId: 1 },
      update({ sessionUpdate: "user_message", messageId: "m-1", content: [text("hi")] }),
      { _tag: "submissionAccepted", id: "s-1", agentMessageId: "m-1" }
    ])
    expect(next.messages.filter((message) => message.id === "m-1")).toHaveLength(1)
    expect(next.submissions["s-1"]!.agentMessageId).toBe("m-1")
    expect(next.activeSubmissionId).toBe("s-1")
  })

  it("v2 foreground ends on the idle state update, not on acceptance", () => {
    const next = State.reduceAll(v2(), [
      { _tag: "submissionRegistered", submission: submission("s-1") },
      { _tag: "submissionDispatched", id: "s-1", requestId: 1 },
      { _tag: "submissionAccepted", id: "s-1", agentMessageId: "m-1" },
      update({ sessionUpdate: "state_update", state: "idle", stopReason: "end_turn" })
    ])
    expect(next.activeSubmissionId).toBeNull()
    expect(next.foreground).toEqual({ state: "idle", stopReason: "end_turn" })
  })

  it("a new turn replaces inherited idle but keeps an idle update before dispatch", () => {
    const previousIdle = State.reduce(v2(), update({ sessionUpdate: "state_update", state: "idle", stopReason: "end_turn" }))
    const registered = State.reduce(previousIdle, { _tag: "submissionRegistered", submission: submission("s-2") })
    expect(registered.foreground).toEqual({ state: "running", provenance: "inferred" })

    const earlyIdle = State.reduce(registered, update({ sessionUpdate: "state_update", state: "idle", stopReason: "end_turn" }))
    const dispatched = State.reduce(earlyIdle, { _tag: "submissionDispatched", id: "s-2", requestId: 2 })
    expect(dispatched.foreground).toEqual({ state: "idle", stopReason: "end_turn" })
    expect(dispatched.activeSubmissionId).toBeNull()
  })

  it("dispatch preserves an agent-reported foreground state", () => {
    const next = State.reduceAll(v2(), [
      { _tag: "submissionRegistered", submission: submission("s-1") },
      update({ sessionUpdate: "state_update", state: "queued" }),
      { _tag: "submissionDispatched", id: "s-1", requestId: 1 }
    ])
    expect(next.foreground).toEqual({ state: "queued", provenance: "agent-reported" })
  })

  it("agent-reported running is distinguishable from inferred running", () => {
    // v1 has no `state_update` variant at all, so a running v1 session is
    // only ever locally inferred from an outstanding prompt.
    const inferred = State.reduceAll(v1(), [
      { _tag: "submissionRegistered", submission: submission("s-1", { acceptanceUnavailable: true }) },
      { _tag: "submissionDispatched", id: "s-1", requestId: 1 }
    ])
    expect(inferred.foreground).toEqual({ state: "running", provenance: "inferred" })

    const reported = State.reduceAll(v2(), [
      { _tag: "submissionRegistered", submission: submission("s-1") },
      { _tag: "submissionDispatched", id: "s-1", requestId: 1 },
      update({ sessionUpdate: "state_update", state: "running" })
    ])
    expect(reported.foreground).toEqual({ state: "running", provenance: "agent-reported" })
  })

  it("a v1 state_update is not interpreted, since v1 has no such variant", () => {
    const next = State.reduce(v1(), update({ sessionUpdate: "state_update", state: "idle" }))
    expect(next.foreground).toEqual({ state: "unknown" })
    // It stays visible as a raw record rather than being silently discarded.
    expect(next.raw[0]!.kind).toBe("state_update")
  })

  // Spec: "V1 prompt remains pending".
  it("v1 completion comes from the prompt response and reports its stop reason", () => {
    const next = State.reduceAll(v1(), [
      { _tag: "submissionRegistered", submission: submission("s-1", { acceptanceUnavailable: true }) },
      { _tag: "submissionDispatched", id: "s-1", requestId: 1 },
      { _tag: "submissionCompleted", id: "s-1", stopReason: "end_turn" }
    ])
    expect(next.submissions["s-1"]!.acceptanceUnavailable).toBe(true)
    expect(next.submissions["s-1"]!.agentMessageId).toBeNull()
    expect(next.foreground).toEqual({ state: "idle", stopReason: "end_turn" })
    expect(next.activeSubmissionId).toBeNull()
  })

  it("a failed submission releases the foreground and records why", () => {
    const next = State.reduceAll(v2(), [
      { _tag: "submissionRegistered", submission: submission("s-1") },
      { _tag: "submissionFailed", id: "s-1", failure: { _tag: "timeout" } }
    ])
    expect(next.submissions["s-1"]!.status).toEqual({ _tag: "failed", failure: { _tag: "timeout" } })
    expect(next.activeSubmissionId).toBeNull()
  })

  it("an unknown agent state is preserved verbatim as agent-reported", () => {
    const next = State.reduce(v2(), update({ sessionUpdate: "state_update", state: "_vendor_waiting" }))
    expect(next.foreground).toEqual({ state: "_vendor_waiting", provenance: "agent-reported" })
  })
})

describe("interactions", () => {
  const pending = {
    interactionId: "permission-0",
    kind: "permission" as const,
    version: 2 as const,
    status: "pending" as const,
    request: { sessionId: "s", title: "Edit file", options: [{ optionId: "allow", name: "Allow", kind: "allow_once" }] },
    outcome: null,
    createdAt: 1,
    resolvedAt: null
  }

  it("prototype-named interactions settle only after they are created", () => {
    for (const id of ["constructor", "toString", "__proto__"]) {
      const missing = State.reduce(v2(), { _tag: "interactionSettled", interactionId: id, status: "resolved", outcome: { outcome: { outcome: "cancelled" } } })
      expect(Object.hasOwn(missing.interactions, id)).toBe(false)
      const next = State.reduceAll(v2(), [
        { _tag: "interactionCreated", interaction: { ...pending, interactionId: id } },
        { _tag: "interactionSettled", interactionId: id, status: "resolved", outcome: { outcome: { outcome: "cancelled" } } }
      ])
      expect(Object.hasOwn(next.interactions, id)).toBe(true)
      expect(next.interactions[id]!.status).toBe("resolved")
      expect(Object.getPrototypeOf(next.interactions)).toBe(Object.prototype)
    }
  })

  it("settling records the outcome and resolution point", () => {
    const next = State.reduceAll(v2(), [
      { _tag: "interactionCreated", interaction: pending },
      { _tag: "interactionSettled", interactionId: "permission-0", status: "resolved", outcome: { outcome: { outcome: "selected", optionId: "allow" } } }
    ])
    const interaction = next.interactions["permission-0"]!
    expect(interaction.status).toBe("resolved")
    expect(interaction.outcome).toEqual({ outcome: { outcome: "selected", optionId: "allow" } })
    expect(interaction.resolvedAt).toBe(next.seq)
  })

  // Spec: "Duplicate interaction response" — the reducer half of it.
  it("a second settle does not overwrite the first", () => {
    const next = State.reduceAll(v2(), [
      { _tag: "interactionCreated", interaction: pending },
      { _tag: "interactionSettled", interactionId: "permission-0", status: "resolved", outcome: { outcome: { outcome: "selected", optionId: "allow" } } },
      { _tag: "interactionSettled", interactionId: "permission-0", status: "cancelled", outcome: { outcome: { outcome: "cancelled" } } }
    ])
    expect(next.interactions["permission-0"]!.outcome).toEqual({ outcome: { outcome: "selected", optionId: "allow" } })
    expect(next.interactions["permission-0"]!.status).toBe("resolved")
  })
})

describe("cancellation", () => {
  // Spec: "Updates after cancel".
  it("updates after a cancel request are applied and do not confirm cancellation", () => {
    const next = State.reduceAll(v2(), [
      { _tag: "submissionRegistered", submission: submission("s-1") },
      { _tag: "submissionDispatched", id: "s-1", requestId: 1 },
      { _tag: "cancelRequested" },
      update({ sessionUpdate: "tool_call_update", toolCallId: "t-1", status: "completed" })
    ])
    expect(next.toolCalls["t-1"]!.status).toBe("completed")
    // No completion signal yet, so foreground work is still outstanding.
    expect(next.activeSubmissionId).toBe("s-1")

    const confirmed = State.reduce(next, update({ sessionUpdate: "state_update", state: "idle", stopReason: "cancelled" }))
    expect(confirmed.activeSubmissionId).toBeNull()
  })
})

describe("retention bounds", () => {
  it("record caps retain a prototype-named newest key as data", () => {
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "tool_call_update", toolCallId: "old" }),
      update({ sessionUpdate: "tool_call_update", toolCallId: "__proto__" }),
      update({ sessionUpdate: "plan_update", plan: { type: "items", planId: "old", entries: [] } }),
      update({ sessionUpdate: "plan_update", plan: { type: "items", planId: "__proto__", entries: [] } })
    ], { ...defaultContentLimits, toolCalls: 1, plans: 1 })
    for (const record of [next.toolCalls, next.plans]) {
      expect(Object.keys(record)).toEqual(["__proto__"])
      expect(Object.getPrototypeOf(record)).toBe(Object.prototype)
    }
  })

  it("interaction cap retains a prototype-named pending key as data", () => {
    const interaction = (interactionId: string) => ({
      interactionId, kind: "permission" as const, version: 2 as const, status: "pending" as const,
      request: { sessionId: "s", title: "Edit", options: [] }, outcome: null, createdAt: 1, resolvedAt: null
    })
    const next = State.reduceAll(v2(), [
      { _tag: "interactionCreated", interaction: interaction("old") },
      { _tag: "interactionSettled", interactionId: "old", status: "resolved", outcome: { outcome: { outcome: "cancelled" } } },
      { _tag: "interactionCreated", interaction: interaction("__proto__") }
    ], { ...defaultContentLimits, interactions: 1 })
    expect(Object.keys(next.interactions)).toEqual(["__proto__"])
    expect(Object.getPrototypeOf(next.interactions)).toBe(Object.prototype)
  })
  it("messages beyond the budget are evicted oldest-first and reported", () => {
    const limits = { ...defaultContentLimits, messages: 3 }
    const next = State.reduceAll(
      v2(),
      Array.from({ length: 5 }, (_, index) =>
        update({ sessionUpdate: "agent_message", messageId: `m-${index}`, content: [text(String(index))] })),
      limits
    )
    expect(next.messages.map((message) => message.id)).toEqual(["m-2", "m-3", "m-4"])
    expect(next.truncated.history).toBe(true)
  })

  it("tool calls, plans, and raw records are each bounded independently", () => {
    const limits = { ...defaultContentLimits, toolCalls: 2, plans: 1, rawUpdates: 3 }
    const next = State.reduceAll(
      v2(),
      [
        ...Array.from({ length: 4 }, (_, index) => update({ sessionUpdate: "tool_call_update", toolCallId: `t-${index}` })),
        ...Array.from({ length: 2 }, (_, index) =>
          update({ sessionUpdate: "plan_update", plan: { type: "items", planId: `p-${index}`, entries: [] } }))
      ],
      limits
    )
    expect(Object.keys(next.toolCalls).sort()).toEqual(["t-2", "t-3"])
    expect(Object.keys(next.plans)).toEqual(["p-1"])
    expect(next.raw).toHaveLength(3)
    expect(next.truncated).toMatchObject({ toolCalls: true, plans: true, raw: true })
  })

  it("pending interactions are never evicted by the interaction budget", () => {
    const limits = { ...defaultContentLimits, interactions: 1 }
    const next = State.reduceAll(
      v2(),
      [
        {
          _tag: "interactionCreated",
          interaction: {
            interactionId: "settled",
            kind: "permission",
            version: 2,
            status: "pending",
            request: { sessionId: "s", title: "Permission", options: [{ optionId: "allow", name: "Allow", kind: "allow_once" }] },
            outcome: null,
            createdAt: 1,
            resolvedAt: null
          }
        },
        { _tag: "interactionSettled", interactionId: "settled", status: "resolved", outcome: { outcome: { outcome: "cancelled" } } },
        {
          _tag: "interactionCreated",
          interaction: {
            interactionId: "still-pending",
            kind: "permission",
            version: 2,
            status: "pending",
            request: { sessionId: "s", title: "Permission", options: [{ optionId: "allow", name: "Allow", kind: "allow_once" }] },
            outcome: null,
            createdAt: 3,
            resolvedAt: null
          }
        }
      ],
      limits
    )
    // Dropping a pending interaction would strand its waiting handler fiber.
    expect(Object.keys(next.interactions)).toEqual(["still-pending"])
    expect(next.truncated.interactions).toBe(true)
  })

  it("seq advances once per applied event", () => {
    const next = State.reduceAll(v2(), [
      update({ sessionUpdate: "state_update", state: "running" }),
      update({ sessionUpdate: "state_update", state: "idle" })
    ])
    expect(next.seq).toBe(2)
  })
})

describe("lifecycle events", () => {
  it("config options and v1 modes from a lifecycle response are projected", () => {
    const next = State.reduce(v1(), {
      _tag: "lifecycle",
      cwd: "/work",
      configOptions: [{ id: "model", name: "Model", type: "select", currentValue: "a", options: [{ value: "a", name: "A" }] }],
      modes: { currentModeId: "code", availableModes: [{ id: "code", name: "Code" }] }
    })
    expect(next.metadata.cwd).toBe("/work")
    expect(next.config["model"]).toBeDefined()
    expect(next.config["acp/modes"]!.option).toMatchObject({ currentModeId: "code" })
  })
})

it("omitted message content keeps prior content while null clears it", () => {
  const state = State.reduceAll(v2(), [
    update({ sessionUpdate: "agent_message_chunk", messageId: "m", content: text("keep") }),
    update({ sessionUpdate: "agent_message", messageId: "m" })
  ])
  expect(state.messages[0]!.content).toEqual([text("keep")])
  expect(State.reduce(state, update({ sessionUpdate: "agent_message", messageId: "m", content: null })).messages[0]!.content).toEqual([])
})

it("one huge transcript entry and unlimited chunks cannot bypass the byte budget", () => {
  let state = v2()
  const limits = { ...defaultContentLimits, transcriptBytes: 2048 }
  for (let i = 0; i < 30; i++) state = State.reduce(state, update({ sessionUpdate: "agent_message_chunk", messageId: "m", content: text("x".repeat(3000)) }), limits)
  expect(state.truncated.content).toBe(true)
  expect(new TextEncoder().encode(JSON.stringify(state)).byteLength).toBeLessThanOrEqual(2048)
})

it("v1 replay respects optional agent message IDs", () => {
  const next = State.reduceAll(v1(), [
    update({ sessionUpdate: "agent_message_chunk", messageId: "first", content: text("a") }),
    update({ sessionUpdate: "agent_message_chunk", messageId: "first", content: text("b") }),
    update({ sessionUpdate: "agent_message_chunk", messageId: "second", content: text("c") })
  ])
  expect(next.messages.map(message => ({ id: message.id, content: message.content }))).toEqual([
    { id: "first", content: [text("a"), text("b")] },
    { id: "second", content: [text("c")] }
  ])
})
