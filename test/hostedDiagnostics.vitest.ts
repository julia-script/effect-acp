import { fileURLToPath } from "node:url"
import { expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"

it.live("default host and gateway loggers omit private failure payloads", () => Effect.sync(() => {
  // Run without a Logger layer or console interception to exercise the shipped default logger.
  const program = `
    import * as Effect from "effect/Effect"
    import * as Deferred from "effect/Deferred"
    import * as Layer from "effect/Layer"
    import * as HttpRouter from "effect/http/HttpRouter"
    import * as Host from "./src/AcpHost.ts"
    import * as Gateway from "./src/AcpGateway.ts"
    import * as GatewayHttp from "./src/server/GatewayHttp.ts"
    import { policy } from "./test/support/host.ts"
    const results = []
    for (const kind of ["typed", "defect"]) {
      results.push(await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
        const settled = yield* Deferred.make()
        const secret = kind === "typed" ? "private-typed-credential" : "private-defect-file-body"
        const host = yield* Host.make({ policy, authorize: () => Effect.void,
          open: () => kind === "typed" ? Effect.fail(new Error(secret)) : Effect.die(new Error(secret)),
          onLifecycle: (event) => event.type === "settled" ? Deferred.succeed(settled, undefined) : Effect.void })
        const identity = { principalId: "owner" }
        const window = yield* host.hello(identity, { version: 1, workspace: "work", clientId: "client" })
        yield* host.admit(identity, { window, operationId: kind, command: { _tag: "Open", profile: "demo", options: null } })
        yield* Deferred.await(settled)
        const operation = yield* host.operation(identity, { window, operationId: kind })
        return { kind, status: operation.status, error: operation.error }
      }))))
    }
    for (const kind of ["internal", "known"]) {
      const route = GatewayHttp.route({ allowOrigin: () => true,
        authenticate: () => kind === "internal"
          ? Effect.die(new Error("private-auth-token"))
          : Effect.fail(new Gateway.GatewayError({ code: "Unauthorized", message: "private-known-token" })) })
      const application = HttpRouter.addAll([route]).pipe(
        Layer.provideMerge(Host.layer({ policy, authorize: () => Effect.void, open: () => Effect.die("must not launch") })),
        Layer.provideMerge(HttpRouter.layer))
      const app = HttpRouter.toWebHandler(application, { disableLogger: true })
      try {
        const response = await app.handler(new Request("http://localhost/acp/gateway"))
        results.push({ kind, status: response.status, body: await response.text() })
      } finally { await app.dispose() }
    }
    console.log("SAFE-RESULTS " + JSON.stringify(results))
  `
  const child = Bun.spawnSync([process.execPath, "-e", program], {
    cwd: fileURLToPath(new URL("..", import.meta.url)), timeout: 10_000
  })
  const output = child.stdout.toString() + child.stderr.toString()
  expect(child.exitCode).toBe(0)
  for (const secret of ["private-typed-credential", "private-defect-file-body", "private-auth-token", "private-known-token"]) {
    expect(output).not.toContain(secret)
  }
  expect(output.match(/Hosted operation failed/g)).toHaveLength(2)
  expect(output.match(/Gateway authentication failed/g)).toHaveLength(1)
  const result = output.split("\n").find((line) => line.startsWith("SAFE-RESULTS "))
  expect(result).toBeDefined()
  expect(JSON.parse(result!.slice("SAFE-RESULTS ".length))).toEqual([
    { kind: "typed", status: "failed", error: { _tag: "AcpGatewayError", code: "AgentFailure", message: "AgentFailure" } },
    { kind: "defect", status: "failed", error: { _tag: "AcpGatewayError", code: "AgentFailure", message: "AgentFailure" } },
    { kind: "internal", status: 401, body: "" },
    { kind: "known", status: 401, body: "" }
  ])
}))
