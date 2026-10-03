import { describe, expect, it } from "tstyche"
import type { NewSessionOptions, ResumeSessionOptions, ElicitationCallback } from "../src/AcpClient.ts"
import * as Effect from "effect/Effect"
import { AcpProtocolError } from "../src/AcpError.ts"
import type { RequestId } from "../src/AcpSchema.ts"

describe("client option compatibility", () => {
  it("continues to accept explicit undefined on public new and resume options", () => {
    const options = { cwd: "/work", additionalDirectories: undefined, mcpServers: undefined }
    expect(options).type.toBeAssignableTo<NewSessionOptions>()
    expect({ ...options, sessionId: "session-1" }).type.toBeAssignableTo<ResumeSessionOptions>()
  })

  it("preserves request correlation and accepts typed callback failures", () => {
    const callback: ElicitationCallback = (request, version) => {
      expect(request.requestId).type.toBe<RequestId>()
      expect(version).type.toBe<1 | 2>()
      return Effect.fail(new AcpProtocolError({ message: "application failure" }))
    }
    expect(callback).type.toBeAssignableTo<ElicitationCallback>()
  })
})
