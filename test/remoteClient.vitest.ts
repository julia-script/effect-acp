import { expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Schema from "effect/Schema"
import * as AcpGateway from "../src/AcpGateway.ts"
import * as GC from "../src/AcpGatewayClient.ts"
import * as Remote from "../src/AcpRemoteClient.ts"
import { singleFailureOf } from "./support/failure.ts"
import { sessionContract } from "./support/sessionContract.ts"
import { apiFor, hostedHarness } from "./support/host.ts"
sessionContract(1, hostedHarness, "remote", "live")
sessionContract(2, hostedHarness, "remote", "live")

it.live("a fresh remote rejects mismatched takeover metadata before revoking a live controller", () =>
  Effect.scoped(Effect.gen(function*() {
    const h = yield* hostedHarness(2)
    const original = yield* h.connection.newSession({ cwd: "/work" })
    const descriptor = h.remote.descriptor(original)!
    const gateway = yield* GC.fromApi(apiFor(h.host), { workspace: "work", storage: GC.memoryStorage() })
    const remote = yield* Remote.make(gateway, { profile: "test" })
    for (const stale of [{ ...descriptor, sessionId: "wrong" }, { ...descriptor, version: 1 as const }]) {
      const rejected = yield* Effect.exit(remote.attach(stale, true))
      expect(Exit.isFailure(rejected)).toBe(true)
      const error = singleFailureOf(rejected)
      expect(Schema.is(AcpGateway.GatewayError)(error) && error.code).toBe("Invalid")
      // A failed takeover must leave the original controller usable.
      yield* original.cancel
    }
    const replacement = yield* remote.attach(descriptor, true)
    yield* replacement.cancel
    const revoked = yield* Effect.exit(original.cancel)
    const error = singleFailureOf(revoked)
    expect(Schema.is(AcpGateway.GatewayError)(error) && error.code).toBe("StaleController")
    yield* replacement.release
    yield* original.release
  })).pipe(Effect.timeout("3 seconds"))
)
