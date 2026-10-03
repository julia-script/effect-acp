import type * as V1 from "../protocol/v1/Schema.ts"
import type { McpServer } from "../AcpApp.ts"
import type { ContentBlock } from "../agent/Content.ts"
/**
 * Normalizes v1 and v2 negotiated capabilities into one shape.
 *
 * The two protocols state the same facts differently: v2 signals support by
 * the presence of a (usually empty) capability object, while v1 mixes that
 * style with plain booleans, nests things under different keys, and names
 * auth method ids `id` rather than `methodId`. The normalized view is what
 * operations check before dispatch; the raw negotiated response stays
 * attached so nothing is lost by normalizing.
 */
import type {
  AuthCapabilities,
  Capabilities,
  McpCapabilities,
  SessionCapabilities,
  SessionVersion
} from "../AcpApp.ts"
import type * as AcpProtocol from "../AcpProtocol.ts"
import * as V2 from "../protocol/v2/Schema.ts"

/**
 * Whether a capability is advertised.
 *
 * The two protocols spell the same fact differently: v2 nests a (usually
 * empty) object whose presence is the signal, while v1 uses plain booleans
 * for its prompt and MCP capabilities. An explicit `false` must therefore
 * read as unsupported rather than as "a value is present".
 */
const present = (source: Readonly<Record<string, unknown>> | null | undefined, key: string): boolean => {
  if (source == null || !Object.hasOwn(source, key)) return false
  const value = source[key]
  if (value === null || value === undefined) return false
  return typeof value === "boolean" ? value : true
}

/**
 * The identifier of one advertised auth method.
 *
 * v1 names the field `id` and v2 names it `methodId`, even though both send
 * `methodId` back in the authenticate request.
 */
export const authMethodId = (method: V1.AuthMethod | V2.AuthMethod): string | undefined => {
  const id = "methodId" in method ? method.methodId : method.id
  return typeof id === "string" ? id : undefined
}

const authMethodIds = (response: V1.InitializeResponse | V2.InitializeResponse): ReadonlyArray<string> => {
  const methods = response.authMethods ?? []
  return methods.flatMap((method) => {
    const id = authMethodId(method)
    return id === undefined ? [] : [id]
  })
}

/**
 * MCP transports the agent accepts.
 *
 * v1 has no `stdio` key because stdio is its baseline; v2 moved every
 * transport behind an explicit capability object, so there stdio must be
 * advertised like the rest.
 */
const mcpServerTypes = (mcp: V1.McpCapabilities | NonNullable<NonNullable<V2.InitializeResponse["capabilities"]>["session"]>["mcp"], version: SessionVersion): ReadonlyArray<string> => {
  const types: Array<string> = []
  if (version === 1 || present(mcp, "stdio")) types.push("stdio")
  if (present(mcp, "http")) types.push("http")
  if (present(mcp, "sse")) types.push("sse")
  return types
}

export interface Installed {
  /** v1 filesystem handlers are installed. */
  readonly filesystem: boolean
  /** v1 terminal handlers are installed. */
  readonly terminal: boolean
}

const v2Capabilities = (
  response: V2.InitializeResponse,
  _installed: Installed
): Capabilities => {
  const session = response.capabilities?.session
  const mcp = session?.mcp
  const sessionCapabilities: SessionCapabilities = {
    // v2 always has new/prompt/resume; list/delete/close are advertised.
    list: session != null,
    delete: present(session, "delete"),
    resume: session != null,
    close: session != null,
    additionalDirectories: present(session, "additionalDirectories"),
    prompt: session != null,
    setConfigOption: session != null,
    // v1-only surfaces.
    loadSession: false,
    setMode: false
  }
  const auth: AuthCapabilities = {
    authenticate: (response.authMethods?.length ?? 0) > 0,
    logout: (response.authMethods?.length ?? 0) > 0,
    methods: authMethodIds(response)
  }
  const mcpCapabilities: McpCapabilities = {
    supported: true,
    serverTypes: mcpServerTypes(mcp, 2)
  }
  return {
    version: 2,
    protocolVersion: String(response.protocolVersion),
    agentInfo: response.info ?? null,
    auth,
    session: sessionCapabilities,
    mcp: mcpCapabilities,
    // v2 moved filesystem and terminal work to display-only updates: the
    // client is not asked to execute anything, so nothing is installed.
    filesystem: false,
    terminal: false,
    elicitation: true,
    negotiated: response
  }
}

const v1Capabilities = (
  response: V1.InitializeResponse,
  installed: Installed
): Capabilities => {
  const agent = response.agentCapabilities
  const session = agent?.sessionCapabilities
  const sessionCapabilities: SessionCapabilities = {
    list: present(session, "list"),
    delete: present(session, "delete"),
    resume: present(session, "resume"),
    close: present(session, "close"),
    additionalDirectories: present(session, "additionalDirectories"),
    prompt: true,
    setConfigOption: true,
    loadSession: agent?.loadSession === true,
    setMode: true
  }
  const auth: AuthCapabilities = {
    authenticate: (response.authMethods?.length ?? 0) > 0,
    logout: present(agent?.auth, "logout"),
    methods: authMethodIds(response)
  }
  return {
    version: 1,
    protocolVersion: String(response.protocolVersion),
    agentInfo: response.agentInfo ?? null,
    auth,
    session: sessionCapabilities,
    mcp: { supported: true, serverTypes: mcpServerTypes(agent?.mcpCapabilities, 1) },
    // Only advertise what the application actually installed handlers for.
    filesystem: installed.filesystem,
    terminal: installed.terminal,
    elicitation: true,
    negotiated: response
  }
}

export const normalize = (
  negotiated: AcpProtocol.Negotiated,
  installed: Installed
): Capabilities =>
  negotiated.version === 2
    ? v2Capabilities(negotiated.response, installed)
    : v1Capabilities(negotiated.response, installed)

/** Whether the actual client initialization advertised this elicitation mode. */
export const elicitationSupported = (negotiated: AcpProtocol.Negotiated, mode: string): boolean => {
  const capabilities = negotiated.advertised.version === 1
    ? negotiated.advertised.params.clientCapabilities
    : negotiated.advertised.params.capabilities
  return present(capabilities?.elicitation, mode)
}

/** Whether the negotiated peer accepts the given MCP server configuration. */
export const mcpServerSupported = (capabilities: Capabilities, server: McpServer): boolean => {
  // v1 stdio servers are identified structurally (command/args), v2 by `type`.
  const fallback = "command" in server ? "stdio" : undefined
  const type = "type" in server && typeof server.type === "string" ? server.type : fallback
  return type !== undefined && capabilities.mcp.serverTypes.includes(type)
}

/** Whether a prompt content block is accepted by the negotiated peer. */
export const contentSupported = (
  capabilities: Capabilities,
  block: ContentBlock
): boolean => {
  const type = block.type
  if (type === "text" || type === "resource_link") return true
  const prompt = promptCapabilities(capabilities)
  switch (type) {
    case "image":
      return present(prompt, "image")
    case "audio":
      return present(prompt, "audio")
    case "resource":
      return present(prompt, "embeddedContext")
    default:
      // Unknown/extension content: let the agent judge it rather than
      // refusing to send something this client does not model.
      return true
  }
}

const promptCapabilities = (capabilities: Capabilities) => capabilities.version === 2
  ? capabilities.negotiated.capabilities?.session?.prompt
  : capabilities.negotiated.agentCapabilities?.promptCapabilities

export type { SessionVersion }
