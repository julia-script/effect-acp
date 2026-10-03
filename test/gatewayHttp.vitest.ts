import { expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Logger from "effect/Logger"
import * as HttpRouter from "effect/http/HttpRouter"
import * as Gateway from "../src/AcpGateway.ts"
import * as Host from "../src/AcpHost.ts"
import * as GatewayHttp from "../src/server/GatewayHttp.ts"
import { policy } from "./support/host.ts"

for (const denial of ["origin", "authentication"] as const) it.effect(`gateway rejects ${denial} before upgrading or launching`, () => Effect.gen(function*() {
  let launches = 0
  const route = GatewayHttp.route({ allowOrigin: () => denial !== "origin",
    authenticate: () => Effect.fail(Gateway.failure("Unauthorized")) })
  const application = HttpRouter.addAll([route]).pipe(Layer.provideMerge(Host.layer({ policy, authorize: () => Effect.void,
    open: () => Effect.sync(() => { launches++; throw new Error("must not launch") }) })), Layer.provideMerge(HttpRouter.layer))
  const app = HttpRouter.toWebHandler(application, { disableLogger: true })
  try {
    const response = (yield* Effect.promise(() => app.handler(new Request("http://localhost/acp/gateway"))))
    expect(response.status).toBe(denial === "origin" ? 403 : 401)
    expect(launches).toBe(0)
  } finally { (yield* Effect.promise(() => app.dispose()))}
}))

it.effect("gateway authentication defects have safe diagnostics and return only 401", () => Effect.gen(function*() {
  const defect = new Error("private auth secret")
  const logs: Array<Logger.Options<unknown>> = []
  const logger = Logger.make<unknown, void>((entry) => { logs.push(entry) })
  const route = GatewayHttp.route({ allowOrigin: () => true, authenticate: () => Effect.die(defect) })
  const application = HttpRouter.addAll([route]).pipe(Layer.provideMerge(Host.layer({ policy, authorize: () => Effect.void,
    open: () => Effect.die("must not launch") })), Layer.provideMerge(HttpRouter.layer), Layer.provideMerge(Logger.layer([logger])))
  const app = HttpRouter.toWebHandler(application, { disableLogger: true })
  try {
    const response = yield* Effect.promise(() => app.handler(new Request("http://localhost/acp/gateway")))
    expect(response.status).toBe(401)
    expect(yield* Effect.promise(() => response.text())).not.toContain("private auth secret")
    const diagnostic = logs.filter((entry) => Array.isArray(entry.message) && entry.message[0] === "Gateway authentication failed")
    expect(diagnostic).toHaveLength(1)
    expect(diagnostic[0]!.cause.reasons).toEqual([])
    expect(JSON.stringify(diagnostic[0]!.message)).not.toContain("private auth secret")
  } finally { yield* Effect.promise(() => app.dispose()) }
}))
