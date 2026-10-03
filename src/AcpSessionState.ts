import type { InteractionOutcome } from "./AcpApp.ts"
import * as V1 from "./protocol/v1/Schema.ts"
import * as Result from "effect/Result"
import * as Base64 from "effect/encoding/Base64"
import * as Schema from "effect/Schema"
import * as Json from "./internal/json.ts"
/**
 * Pure, version-aware session state reduction.
 *
 * **Details**
 *
 * Every function here is synchronous and total: given a snapshot and one
 * event it returns the next snapshot. The runtime in `AcpLocalClient` owns
 * ordering, publication, and effects; keeping interpretation pure is what
 * makes the v1/v2 differences testable without a transport.
 *
 * Patch semantics follow the negotiated protocol. For v2 an omitted field
 * means "no change", an explicit `null` clears, and a value replaces; chunks
 * append. v1 has no patch/clear distinction on most families and no message
 * identities, so its adapter synthesizes local identities and marks them as
 * such rather than pretending they are durable.
 */
import type {
  ConfigOptionSnapshot,
  ContentLimits,
  Foreground,
  ForegroundProvenance,
  InteractionSnapshot,
  MessageSnapshot,
  PlanSnapshot,
  Provenance,
  RawUpdateRecord,
  SessionId,
  SessionSnapshot,
  SessionVersion,
  SubmissionFailure,
  SubmissionSnapshot,
  TerminalSnapshot,
  ToolCallSnapshot
} from "./AcpApp.ts"
import { defaultContentLimits } from "./AcpApp.ts"
import type { RequestId } from "./AcpSchema.ts"
import * as V2 from "./protocol/v2/Schema.ts"

// -----------------------------------------------------------------------------
// Events
// -----------------------------------------------------------------------------

/**
 * Everything that can advance a session snapshot.
 *
 * **Details**
 *
 * Wire updates arrive as `update`; local lifecycle facts (a submission being
 * dispatched, an interaction resolving) arrive as their own events so the
 * reducer never has to guess at what the runtime did.
 *
 * @category models
 */
export type Event =
  /** Raw update payload; known variants are validated before projection. */
  | { readonly _tag: "update"; readonly update: unknown }
  /** Session metadata learned from a lifecycle response rather than an update. */
  | {
    readonly _tag: "lifecycle"
    readonly cwd?: string | null
    readonly configOptions?: ReadonlyArray<V1.SessionConfigOption | V2.SessionConfigOption> | null
    readonly modes?: V1.SessionModeState | null
  }
  | { readonly _tag: "submissionRegistered"; readonly submission: SubmissionSnapshot }
  | { readonly _tag: "submissionDispatched"; readonly id: string; readonly requestId: RequestId }
  /** v2: the prompt response acknowledged insertion with `agentMessageId`. */
  | { readonly _tag: "submissionAccepted"; readonly id: string; readonly agentMessageId: string }
  /** v1: the prompt response arrived, which is turn completion, not acceptance. */
  | { readonly _tag: "submissionCompleted"; readonly id: string; readonly stopReason: string | null }
  | { readonly _tag: "submissionFailed"; readonly id: string; readonly failure: SubmissionFailure }
  | { readonly _tag: "interactionCreated"; readonly interaction: InteractionSnapshot }
  | {
    readonly _tag: "interactionSettled"
    readonly interactionId: string
    readonly status: "resolved" | "cancelled" | "expired"
    readonly outcome: InteractionOutcome
  }
  /** Cancellation was requested locally; foreground stays until protocol evidence. */
  | { readonly _tag: "cancelRequested" }

// -----------------------------------------------------------------------------
// Construction
// -----------------------------------------------------------------------------

/**
 * Creates an empty session projection with sequence zero and unknown foreground state.
 *
 * **Details**
 *
 * The supplied working directory initializes metadata; other metadata and retained collections
 * start empty.
 *
 * @category constructors
 */
export const empty = (
  sessionId: SessionId,
  version: SessionVersion,
  cwd: string | null = null
): SessionSnapshot => ({
  sessionId,
  version,
  seq: 0,
  metadata: { title: null, updatedAt: null, cwd },
  foreground: { state: "unknown" },
  activeSubmissionId: null,
  submissions: {},
  messages: [],
  toolCalls: {},
  plans: {},
  terminals: {},
  commands: [],
  config: {},
  usage: null,
  interactions: {},
  raw: [],
  truncated: { history: false, toolCalls: false, plans: false, terminals: [], raw: false, interactions: false }
})

// -----------------------------------------------------------------------------
// Small helpers
// -----------------------------------------------------------------------------

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)

/** True when a patch field was supplied at all (`null` counts, omitted does not). */
const supplied = <S extends object>(source: S, key: keyof S): boolean => Object.hasOwn(source, key)

const own = <A>(record: Readonly<Record<string, A>>, key: string): A | undefined =>
  Object.hasOwn(record, key) ? record[key] : undefined

const setOwn = <A>(record: Record<string, A>, key: string, value: A): void => {
  Object.defineProperty(record, key, { value, writable: true, enumerable: true, configurable: true })
}

/**
 * Applies v2 patch semantics to one field: omitted keeps `previous`, `null`
 * clears to `null`, any other value replaces.
 */
const patch = <A, S extends object>(schema: Schema.Codec<A>, source: S, key: keyof S, previous: A | null): A | null => {
  if (!supplied(source, key)) return previous
  const value = source[key]
  if (value === null || value === undefined) return null
  return Schema.is(schema)(value) ? value : previous
}

const agentProvenance = (agentId: string | null): Provenance => ({ _tag: "agent", agentId })
const localProvenance: Provenance = { _tag: "local" }

/** Drops oldest entries of a keyed record beyond `limit`, oldest by `seq`. */
const capRecord = <A extends { readonly seq: number }>(
  entries: Readonly<Record<string, A>>,
  limit: number
): readonly [Readonly<Record<string, A>>, boolean] => {
  const keys = Object.keys(entries)
  if (keys.length <= limit) return [entries, false]
  const ordered = keys.sort((a, b) => entries[a]!.seq - entries[b]!.seq)
  const kept: Record<string, A> = {}
  for (const key of ordered.slice(keys.length - limit)) setOwn(kept, key, entries[key]!)
  return [kept, true]
}

const decodeBase64 = (data: string): ReadonlyArray<number> => {
  // atob accepted ASCII whitespace and omitted padding. Keep both forms when
  // switching to Effect's result-based decoder.
  const stripped = data.replace(/[ \t\n\f\r]/g, "")
  const padded = stripped.includes("=") ? stripped : stripped.padEnd(stripped.length + (4 - stripped.length % 4) % 4, "=")
  const decoded = Base64.decode(padded)
  // An undecodable chunk contributes no bytes; the raw record keeps it observable.
  return Result.isSuccess(decoded) ? Array.from(decoded.success) : []
}

// -----------------------------------------------------------------------------
// Message helpers
// -----------------------------------------------------------------------------

type MessageKind = MessageSnapshot["kind"]

/** Appends chunk content to a message, creating it when first seen. */
const appendChunk = (
  snapshot: SessionSnapshot,
  id: string,
  kind: MessageKind,
  provenance: Provenance,
  block: V2.ContentBlock,
  seq: number
): SessionSnapshot => {
  const index = snapshot.messages.findIndex((message) => message.id === id)
  if (index < 0) {
    return {
      ...snapshot,
      messages: [...snapshot.messages, { id, kind, provenance, content: [block], seq }]
    }
  }
  const previous = snapshot.messages[index]!
  const messages = snapshot.messages.slice()
  messages[index] = { ...previous, content: [...previous.content, block], seq }
  return { ...snapshot, messages }
}

/**
 * Replaces a whole message's content. A replacement resets accumulated
 * chunks: only chunks received after it are appended (spec: "Replace
 * accumulated chunks").
 */
const replaceMessage = (
  snapshot: SessionSnapshot,
  id: string,
  kind: MessageKind,
  provenance: Provenance,
  content: ReadonlyArray<V2.ContentBlock> | null,
  seq: number
): SessionSnapshot => {
  const index = snapshot.messages.findIndex((message) => message.id === id)
  const next: MessageSnapshot = { id, kind, provenance, content: content ?? [], seq }
  if (index < 0) return { ...snapshot, messages: [...snapshot.messages, next] }
  const messages = snapshot.messages.slice()
  messages[index] = next
  return { ...snapshot, messages }
}

// -----------------------------------------------------------------------------
// v2 update reduction
// -----------------------------------------------------------------------------

const reduceToolCall = (
  snapshot: SessionSnapshot,
  update: V1.ToolCallUpdate | V2.ToolCallUpdate,
  provenance: Provenance,
  seq: number
): SessionSnapshot => {
  const toolCallId = update.toolCallId
  const previous: ToolCallSnapshot = own(snapshot.toolCalls, toolCallId) ?? {
    toolCallId,
    provenance,
    title: null,
    name: null,
    kind: null,
    status: null,
    content: null,
    locations: null,
    rawInput: undefined,
    rawOutput: undefined,
    seq
  }
  const next: ToolCallSnapshot = {
    ...previous,
    title: patch(Schema.String, update, "title", previous.title),
    name: snapshot.version === 1 && update.name == null ? previous.name : patch(Schema.String, update, "name", previous.name),
    kind: patch(V2.ToolKind, update, "kind", previous.kind),
    status: patch(V2.ToolCallStatus, update, "status", previous.status),
    content: snapshot.version === 1
      ? patch(Schema.Array(V1.ToolCallContent), update, "content", previous.content)
      : patch(Schema.Array(V2.ToolCallContent), update, "content", previous.content),
    locations: patch(Schema.Array(V2.ToolCallLocation), update, "locations", previous.locations),
    rawInput: supplied(update, "rawInput") ? update["rawInput"] : previous.rawInput,
    rawOutput: supplied(update, "rawOutput") ? update["rawOutput"] : previous.rawOutput,
    seq
  }
  return { ...snapshot, toolCalls: { ...snapshot.toolCalls, [toolCallId]: next } }
}

const reduceToolCallContentChunk = (
  snapshot: SessionSnapshot,
  update: V2.ToolCallContentChunk,
  provenance: Provenance,
  seq: number
): SessionSnapshot => {
  const toolCallId = update.toolCallId
  const previous = own(snapshot.toolCalls, toolCallId)
  const content = update["content"]
  const next: ToolCallSnapshot = previous === undefined
    ? {
      toolCallId,
      provenance,
      title: null,
      name: null,
      kind: null,
      status: null,
      content: [content],
      locations: null,
      rawInput: undefined,
      rawOutput: undefined,
      seq
    }
    : { ...previous, content: [...(previous.content ?? []), content], seq }
  return { ...snapshot, toolCalls: { ...snapshot.toolCalls, [toolCallId]: next } }
}

const reduceTerminalUpdate = (
  snapshot: SessionSnapshot,
  update: V2.TerminalUpdate,
  seq: number,
  limits: ContentLimits
): SessionSnapshot => {
  const terminalId = update.terminalId
  const previous: TerminalSnapshot = own(snapshot.terminals, terminalId) ?? {
    terminalId,
    command: null,
    cwd: null,
    outputBytes: [],
    exited: false,
    exitCode: null,
    exitSignal: null,
    outputTruncated: false,
    seq
  }
  // An output snapshot is authoritative: it replaces retained bytes entirely.
  const replaced = supplied(update, "output")
  const output = update["output"]
  let bytes = previous.outputBytes
  if (replaced) bytes = output == null ? [] : decodeBase64(output.data)
  const exitStatus = update["exitStatus"]
  const exited = exitStatus != null
  const retainedExitCode = supplied(update, "exitStatus") ? null : previous.exitCode
  const retainedExitSignal = supplied(update, "exitStatus") ? null : previous.exitSignal
  const [capped, truncated] = capBytes(bytes, limits.terminalBytes)
  const next: TerminalSnapshot = {
    ...previous,
    command: patch(Schema.String, update, "command", previous.command),
    cwd: patch(Schema.String, update, "cwd", previous.cwd),
    outputBytes: capped,
    exited: supplied(update, "exitStatus") ? exited : previous.exited ?? false,
    exitCode: exited ? exitStatus.exitCode ?? null : retainedExitCode,
    exitSignal: exited ? exitStatus.signal ?? null : retainedExitSignal,
    outputTruncated: replaced ? truncated : previous.outputTruncated || truncated,
    seq
  }
  return withTerminal(snapshot, next, truncated)
}

/** Keeps the tail of `bytes` within `limit`; reports whether anything was dropped. */
const capBytes = (
  bytes: ReadonlyArray<number>,
  limit: number
): readonly [ReadonlyArray<number>, boolean] =>
  bytes.length <= limit ? [bytes, false] : [bytes.slice(bytes.length - limit), true]

const withTerminal = (
  snapshot: SessionSnapshot,
  terminal: TerminalSnapshot,
  truncated: boolean
): SessionSnapshot => ({
  ...snapshot,
  terminals: { ...snapshot.terminals, [terminal.terminalId]: terminal },
  truncated: truncated && !snapshot.truncated.terminals.includes(terminal.terminalId)
    ? { ...snapshot.truncated, terminals: [...snapshot.truncated.terminals, terminal.terminalId] }
    : snapshot.truncated
})

const reduceTerminalOutputChunk = (
  snapshot: SessionSnapshot,
  update: V2.TerminalOutputChunk,
  seq: number,
  limits: ContentLimits
): SessionSnapshot => {
  const terminalId = update.terminalId
  const previous = own(snapshot.terminals, terminalId)
  // Each chunk is encoded independently, so it is decoded on its own before
  // being appended: concatenating base64 text first would corrupt the bytes.
  const appended = decodeBase64(update.data)
  const combined = [...(previous?.outputBytes ?? []), ...appended]
  const [capped, truncated] = capBytes(combined, limits.terminalBytes)
  const next: TerminalSnapshot = {
    terminalId,
    command: previous?.command ?? null,
    cwd: previous?.cwd ?? null,
    outputBytes: capped,
    exited: previous?.exited ?? false,
    exitCode: previous?.exitCode ?? null,
    exitSignal: previous?.exitSignal ?? null,
    outputTruncated: (previous?.outputTruncated ?? false) || truncated,
    seq
  }
  return withTerminal(snapshot, next, truncated)
}

const reducePlan = (
  snapshot: SessionSnapshot,
  plan: V2.PlanUpdateContent,
  provenance: Provenance,
  seq: number
): SessionSnapshot => {
  const planId = plan.planId
  // Only the `items` variant carries interpretable entries; other variants
  // stay observable through `variant` without clobbering known entries.
  const entries = plan.type === "items" && Schema.is(V2.PlanItems)(plan) ? plan.entries : null
  const next: PlanSnapshot = { planId, provenance, entries, variant: plan, seq }
  return { ...snapshot, plans: { ...snapshot.plans, [planId]: next } }
}

const reduceConfigOptions = (
  snapshot: SessionSnapshot,
  options: ReadonlyArray<V1.SessionConfigOption | V2.SessionConfigOption>,
  version: SessionVersion,
  seq: number
): SessionSnapshot => {
  // The agent always reports the full set, so the projection is replaced.
  const config: Record<string, ConfigOptionSnapshot> = {}
  for (const option of options) {
    if (version === 2 && Schema.is(V2.SessionConfigOption)(option)) {
      const key = option.configId
      setOwn(config, key, { key, option, seq })
    } else if (version === 1 && Schema.is(V1.SessionConfigOption)(option)) {
      const key = option.id
      setOwn(config, key, { key, option, seq })
    }
  }
  return { ...snapshot, config }
}

const foregroundFromState = (state: V2.StateUpdate): Foreground => {
  const name = state.state
  if (name === "idle") {
    const stopReason = "stopReason" in state ? state.stopReason : undefined
    return { state: "idle", stopReason: typeof stopReason === "string" ? stopReason : null }
  }
  return { state: name, provenance: "agent-reported" }
}

/** Applies one decoded v2 `session/update` payload. */
const reduceV2Update = (
  snapshot: SessionSnapshot,
  update: Record<string, unknown>,
  seq: number,
  limits: ContentLimits
): SessionSnapshot => {
  const kind = textValue(update["sessionUpdate"])
  const messageId = typeof update["messageId"] === "string" ? update["messageId"] : null
  const provenance = agentProvenance(messageId)
  switch (kind) {
    case "user_message_chunk":
    case "agent_message_chunk":
    case "agent_thought_chunk": {
      if (!Schema.is(V2.ContentChunk)(update)) return snapshot
      const role: MessageKind = messageKind(kind)
      const block = update.content
      return appendChunk(
        snapshot,
        update.messageId,
        role,
        agentProvenance(update.messageId),
        block,
        seq
      )
    }
    case "user_message":
    case "agent_message":
    case "agent_thought": {
      if (!Schema.is(Schema.Union([V2.UserMessage, V2.AgentMessage, V2.AgentThought]))(update)) return snapshot
      const role: MessageKind = messageKind(kind)
      return replaceMessage(
        snapshot,
        update.messageId,
        role,
        agentProvenance(update.messageId),
        patch(Schema.Array(V2.ContentBlock), update, "content", snapshot.messages.find((message) => message.id === update.messageId)?.content ?? null),
        seq
      )
    }
    case "state_update": {
      if (!Schema.is(V2.StateUpdate)(update)) return snapshot
      const foreground = foregroundFromState(update)
      return {
        ...snapshot,
        foreground,
        // v2 reports the end of foreground work as an idle state, separately
        // from the prompt response that acknowledged insertion.
        activeSubmissionId: foreground.state === "idle" ? null : snapshot.activeSubmissionId
      }
    }
    case "tool_call_update":
      if (!Schema.is(V2.ToolCallUpdate)(update)) return snapshot
      return reduceToolCall(snapshot, update, provenance, seq)
    case "tool_call_content_chunk":
      if (!Schema.is(V2.ToolCallContentChunk)(update)) return snapshot
      return reduceToolCallContentChunk(snapshot, update, provenance, seq)
    case "terminal_update":
      if (!Schema.is(V2.TerminalUpdate)(update)) return snapshot
      return reduceTerminalUpdate(snapshot, update, seq, limits)
    case "terminal_output_chunk":
      if (!Schema.is(V2.TerminalOutputChunk)(update)) return snapshot
      return reduceTerminalOutputChunk(snapshot, update, seq, limits)
    case "plan_update":
      if (!Schema.is(V2.PlanUpdate)(update)) return snapshot
      return reducePlan(snapshot, update.plan, provenance, seq)
    case "available_commands_update":
      if (!Schema.is(V2.AvailableCommandsUpdate)(update)) return snapshot
      return { ...snapshot, commands: update.availableCommands }
    case "config_option_update":
      if (!Schema.is(V2.ConfigOptionUpdate)(update)) return snapshot
      return reduceConfigOptions(snapshot, update.configOptions, 2, seq)
    case "session_info_update":
      if (!Schema.is(V2.SessionInfoUpdate)(update)) return snapshot
      return {
        ...snapshot,
        metadata: {
          ...snapshot.metadata,
          title: patch(Schema.String, update, "title", snapshot.metadata.title),
          updatedAt: patch(Schema.String, update, "updatedAt", snapshot.metadata.updatedAt)
        }
      }
    case "usage_update":
      if (!Schema.is(V2.UsageUpdate)(update)) return snapshot
      return {
        ...snapshot,
        usage: {
          used: update.used,
          size: update.size,
          cost: update.cost ?? null
        }
      }
    default:
      // A permitted unknown/extension variant. It stays observable through
      // the raw record; known state is deliberately left untouched.
      return snapshot
  }
}

// -----------------------------------------------------------------------------
// v1 update reduction
// -----------------------------------------------------------------------------

/**
 * Applies one decoded v1 `session/update` payload.
 *
 * v1 chunks may omit `messageId`, so consecutive anonymous chunks of the same
 * role and local turn are folded into one message and marked `local`. That local
 * identity is never matched against submissions or agent content.
 */
const reduceV1Update = (
  snapshot: SessionSnapshot,
  update: Record<string, unknown>,
  seq: number,
  _limits: ContentLimits
): SessionSnapshot => {
  const kind = textValue(update["sessionUpdate"])
  switch (kind) {
    case "user_message_chunk":
    case "agent_message_chunk":
    case "agent_thought_chunk": {
      const role: MessageKind = messageKind(kind)
      const last = snapshot.messages[snapshot.messages.length - 1]
      // A new prompt is a message boundary even when the agent does not echo
      // user chunks. This also separates replay's final reply from a new turn.
      const prefix = `local/${JSON.stringify(snapshot.activeSubmissionId)}/`
      const open = last !== undefined && last.kind === role && last.provenance._tag === "local" && last.id.startsWith(prefix)
      const block = update["content"]
      if (!Schema.is(V2.ContentBlock)(block)) return snapshot
      const messageId = typeof update["messageId"] === "string" ? update["messageId"] : null
      return appendChunk(
        snapshot,
        messageId ?? (open ? last.id : `${prefix}${seq}`),
        role,
        messageId === null ? localProvenance : agentProvenance(messageId),
        block,
        seq
      )
    }
    case "tool_call":
    case "tool_call_update":
      // v1 has no omit/null/value distinction; `tool_call` introduces and
      // `tool_call_update` merges, both with agent-owned tool call ids.
      if (!Schema.is(kind === "tool_call" ? V1.ToolCall : V1.ToolCallUpdate)(update)) return snapshot
      return reduceToolCall(snapshot, update, agentProvenance(update.toolCallId), seq)
    case "plan":
      if (!Schema.is(V1.Plan)(update)) return snapshot
      // v1 has a single implicit open plan rather than plan identities.
      return reducePlan(
        snapshot,
        { type: "items", planId: "local-plan", entries: update.entries },
        localProvenance,
        seq
      )
    case "available_commands_update":
      if (!Schema.is(V1.AvailableCommandsUpdate)(update)) return snapshot
      return { ...snapshot, commands: update.availableCommands.map((command) => {
        const { input, ...rest } = command
        return {
          ...rest,
          ...(input === undefined ? {} : { input: input === null ? null : { ...input, type: "text" as const } })
        }
      }) }
    case "current_mode_update":
      if (!Schema.is(V1.CurrentModeUpdate)(update)) return snapshot
      return {
        ...snapshot,
        config: {
          ...snapshot.config,
          "acp/currentMode": { key: "acp/currentMode", option: update, seq }
        }
      }
    case "config_option_update":
      if (!Schema.is(V1.ConfigOptionUpdate)(update)) return snapshot
      return reduceConfigOptions(snapshot, update.configOptions, 1, seq)
    case "session_info_update":
      if (!Schema.is(V1.SessionInfoUpdate)(update)) return snapshot
      return {
        ...snapshot,
        metadata: {
          ...snapshot.metadata,
          title: patch(Schema.String, update, "title", snapshot.metadata.title),
          updatedAt: patch(Schema.String, update, "updatedAt", snapshot.metadata.updatedAt)
        }
      }
    case "usage_update":
      if (!Schema.is(V1.UsageUpdate)(update)) return snapshot
      return {
        ...snapshot,
        usage: {
          used: update.used,
          size: update.size,
          cost: update.cost ?? null
        }
      }
    default:
      return snapshot
  }
}

// -----------------------------------------------------------------------------
// Submissions
// -----------------------------------------------------------------------------

const withSubmission = (
  snapshot: SessionSnapshot,
  id: string,
  update: (submission: SubmissionSnapshot) => SubmissionSnapshot
): SessionSnapshot => {
  const previous = own(snapshot.submissions, id)
  if (previous === undefined) return snapshot
  return { ...snapshot, submissions: { ...snapshot.submissions, [id]: update(previous) } }
}

/** Foreground state inferred locally while a submission is outstanding. */
const inferredRunning: Foreground = { state: "running", provenance: "inferred" satisfies ForegroundProvenance }

// -----------------------------------------------------------------------------
// Retention
// -----------------------------------------------------------------------------

const applyLimits = (snapshot: SessionSnapshot, limits: ContentLimits): SessionSnapshot => {
  let next = snapshot
  let truncated = snapshot.truncated

  if (next.messages.length > limits.messages) {
    next = { ...next, messages: next.messages.slice(next.messages.length - limits.messages) }
    truncated = { ...truncated, history: true }
  }
  const [toolCalls, toolCallsDropped] = capRecord(next.toolCalls, limits.toolCalls)
  if (toolCallsDropped) truncated = { ...truncated, toolCalls: true }
  const [plans, plansDropped] = capRecord(next.plans, limits.plans)
  if (plansDropped) truncated = { ...truncated, plans: true }
  if (next.raw.length > limits.rawUpdates) {
    next = { ...next, raw: next.raw.slice(next.raw.length - limits.rawUpdates) }
    truncated = { ...truncated, raw: true }
  }
  // Pending interactions are never evicted: a dropped pending interaction
  // would strand its handler fiber with no way for a caller to resolve it.
  const interactionKeys = Object.keys(next.interactions)
  if (interactionKeys.length > limits.interactions) {
    const settled = interactionKeys
      .filter((key) => next.interactions[key]!.status !== "pending")
      .sort((a, b) => next.interactions[a]!.createdAt - next.interactions[b]!.createdAt)
    const drop = new Set(settled.slice(0, interactionKeys.length - limits.interactions))
    if (drop.size > 0) {
      const interactions: Record<string, InteractionSnapshot> = {}
      for (const key of interactionKeys) if (!drop.has(key)) setOwn(interactions, key, next.interactions[key]!)
      next = { ...next, interactions }
      truncated = { ...truncated, interactions: true }
    }
  }
    next = { ...next, toolCalls, plans, truncated }
  const submissions = { ...next.submissions }
  const completed = Object.keys(submissions).filter((id) => id !== next.activeSubmissionId)
  while (Object.keys(submissions).length > (limits.submissions ?? 128) && completed.length) delete submissions[completed.shift()!]
  const [terminals, droppedTerminals] = capRecord(next.terminals, limits.terminals ?? 32)
  next = { ...next, submissions, terminals }
  next = { ...next, truncated: { ...next.truncated, terminals: next.truncated.terminals.filter((id) => Object.hasOwn(terminals, id)) } }
  if (droppedTerminals) next = { ...next, truncated: { ...next.truncated, content: true } }
  // Bound payload size as well as item counts: one message can contain arbitrarily many chunks.
  const budget = limits.transcriptBytes ?? 4 * 1024 * 1024
  // Non-serializable retained data cannot fit a JSON byte budget; evict it.
  const bytes = () => Result.getOrElse(Json.byteLength(next), () => Infinity)
  if (bytes() > budget) {
    next = { ...next, truncated: { ...next.truncated, content: true, history: true, raw: true }, raw: [] }
    const messages = [...next.messages]
    next = { ...next, messages }
    while (messages.length && bytes() > budget) messages.shift()
    if (bytes() > budget) {
      next = { ...next, toolCalls: {}, plans: {}, terminals: {}, commands: [], config: {}, usage: null,
        truncated: { ...next.truncated, toolCalls: true, plans: true } }
    }
    if (bytes() > budget) {
      next = { ...next, submissions: Object.fromEntries(Object.entries(next.submissions).filter(([id]) => id === next.activeSubmissionId)
        .map(([id, submission]) => [id, { ...submission, prompt: [] }])),
        interactions: Object.fromEntries(Object.entries(next.interactions).filter(([, i]) => i.status === "pending")),
        metadata: { title: null, cwd: null, updatedAt: null },
        foreground: next.foreground.state.length > 256 ? { state: "unknown" } : next.foreground,
        truncated: { ...next.truncated, terminals: [] } }
    }
  }
  return next
}

// -----------------------------------------------------------------------------
// Reduce
// -----------------------------------------------------------------------------

/**
 * Applies one event, returning the next snapshot.
 *
 * **Details**
 *
 * `seq` advances on every applied event, so an observer can tell whether a
 * snapshot it holds predates one it is comparing against.
 *
 * @category transforming
 */
export const reduce = (
  snapshot: SessionSnapshot,
  event: Event,
  limits: ContentLimits = defaultContentLimits
): SessionSnapshot => {
  const seq = snapshot.seq + 1
  const next = apply({ ...snapshot, seq }, event, seq, limits)
  return applyLimits(next, limits)
}

const apply = (
  snapshot: SessionSnapshot,
  event: Event,
  seq: number,
  limits: ContentLimits
): SessionSnapshot => {
  switch (event._tag) {
    case "update": {
      const update = isRecord(event.update) ? event.update : {}
      const kind = typeof update["sessionUpdate"] === "string" ? update["sessionUpdate"] : "undecodable"
      const record: RawUpdateRecord = { seq, version: snapshot.version, kind, update: event.update }
      const withRaw = { ...snapshot, raw: [...snapshot.raw, record] }
      return snapshot.version === 2
        ? reduceV2Update(withRaw, update, seq, limits)
        : reduceV1Update(withRaw, update, seq, limits)
    }

    case "lifecycle": {
      let next = snapshot
      if (event.cwd !== undefined) next = { ...next, metadata: { ...next.metadata, cwd: event.cwd } }
      if (event.configOptions !== undefined && event.configOptions !== null) {
        next = reduceConfigOptions(next, event.configOptions, next.version, seq)
      }
      if (Schema.is(V1.SessionModeState)(event.modes)) {
        // v1 modes have no v2 equivalent; they are surfaced as configuration
        // under a reserved key rather than invented into the v2 shape.
        next = {
          ...next,
          config: { ...next.config, "acp/modes": { key: "acp/modes", option: event.modes, seq } }
        }
      }
      return next
    }

    case "submissionRegistered":
      return {
        ...snapshot,
        submissions: { ...snapshot.submissions, [event.submission.id]: event.submission },
        activeSubmissionId: event.submission.id,
        // The previous turn's idle is no longer evidence that this turn has
        // finished. Reset it before the wire write so a new idle update that
        // arrives before dispatch or acceptance is still observed.
        foreground: snapshot.foreground.state === "idle" ? inferredRunning : snapshot.foreground
      }

    case "submissionDispatched":
      return {
        ...withSubmission(snapshot, event.id, (submission) => ({
          ...submission,
          status: { _tag: "dispatched" },
          requestId: event.requestId
        })),
        // v1 completes on each prompt response and has no foreground updates.
        // A later turn must replace the previous turn's idle state, otherwise
        // any update can release cancellation waiters before this turn ends.
        foreground: snapshot.version === 1 || snapshot.foreground.state === "unknown" ? inferredRunning : snapshot.foreground
      }

    case "submissionAccepted":
      // v2 acceptance acknowledges insertion only. The message may already
      // exist from an update that arrived first; adopting the agent id here
      // associates the two without appending a duplicate.
      return withSubmission(snapshot, event.id, (submission) => ({
        ...submission,
        status: { _tag: "accepted" },
        agentMessageId: event.agentMessageId
      }))

    case "submissionCompleted":
      return {
        ...withSubmission(snapshot, event.id, (submission) => ({ ...submission, status: { _tag: "completed" } })),
        activeSubmissionId: snapshot.activeSubmissionId === event.id ? null : snapshot.activeSubmissionId,
        // v1 reports the turn's stop reason on the prompt response itself.
        foreground: snapshot.version === 1
          ? { state: "idle", stopReason: event.stopReason }
          : snapshot.foreground
      }

    case "submissionFailed":
      return {
        ...withSubmission(snapshot, event.id, (submission) => ({
          ...submission,
          status: { _tag: "failed", failure: event.failure }
        })),
        activeSubmissionId: snapshot.activeSubmissionId === event.id ? null : snapshot.activeSubmissionId,
        foreground: "provenance" in snapshot.foreground && snapshot.foreground.provenance === "inferred" ? { state: "unknown" } : snapshot.foreground
      }

    case "interactionCreated":
      return {
        ...snapshot,
        interactions: { ...snapshot.interactions, [event.interaction.interactionId]: event.interaction }
      }

    case "interactionSettled": {
      const previous = own(snapshot.interactions, event.interactionId)
      if (previous === undefined || previous.status !== "pending") return snapshot
      return {
        ...snapshot,
        interactions: {
          ...snapshot.interactions,
          [event.interactionId]: {
            ...previous,
            status: event.status,
            outcome: event.outcome,
            resolvedAt: seq
          }
        }
      }
    }

    case "cancelRequested":
      // Cancellation is not confirmed here: updates keep flowing and the
      // foreground only ends on the negotiated completion signal.
      return snapshot
  }
}

/**
 * Applies a sequence of session events in order with the same retention limits.
 *
 * **When to use**
 *
 * Use to replay recorded events or project a batch of updates without a live transport.
 *
 * **Details**
 *
 * Each event advances the snapshot sequence and applies retention limits. An empty iterable returns
 * the supplied snapshot.
 *
 * **Example** (Replacing accumulated message chunks)
 *
 * ```ts
 * import * as State from "effect-acp/AcpSessionState"
 *
 * const next = State.reduceAll(State.empty("session-1", 2), [
 *   { _tag: "update", update: {
 *     sessionUpdate: "agent_message_chunk", messageId: "message-1",
 *     content: { type: "text", text: "draft" }
 *   } },
 *   { _tag: "update", update: {
 *     sessionUpdate: "agent_message", messageId: "message-1",
 *     content: [{ type: "text", text: "final" }]
 *   } }
 * ])
 *
 * // The whole-message update replaces the accumulated draft.
 * const content = next.messages[0]?.content
 * // content: [{ type: "text", text: "final" }]
 * void content
 * ```
 *
 * @see {@link reduce} for applying a single event.
 * @category transforming
 */
export const reduceAll = (
  snapshot: SessionSnapshot,
  events: Iterable<Event>,
  limits: ContentLimits = defaultContentLimits
): SessionSnapshot => {
  let next = snapshot
  for (const event of events) next = reduce(next, event, limits)
  return next
}

const textValue = (value: unknown): string => typeof value === "string" ? value : ""
const messageKind = (kind: string): MessageKind => {
  if (kind.startsWith("user_")) return "user"
  if (kind.startsWith("agent_message")) return "agent"
  return "thought"
}
