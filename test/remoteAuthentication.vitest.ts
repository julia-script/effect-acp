import { describe, expect, it } from "@effect/vitest"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Remote from "../src/AcpRemoteClient.ts"
import { connectOptions } from "./support/connectOptions.ts"
import { singleFailureOf } from "./support/failure.ts"
import { hostedHarness } from "./support/host.ts"

const initialize = (version: 1 | 2, terminal: boolean) => {
  const method = terminal
    ? {
        type: "terminal",
        name: "Terminal",
        args: ["--login"],
        ...(version === 1 ? { id: "login", env: {} } : { methodId: "login", env: [] }),
      }
    : {
        name: "Login",
        ...(version === 1 ? { id: "login" } : { type: "agent", methodId: "login" }),
      }
  return version === 1
    ? { protocolVersion: 1, agentCapabilities: {}, authMethods: [method] }
    : {
        protocolVersion: 2,
        info: { name: "agent", version: "1" },
        capabilities: { session: {} },
        authMethods: [method],
      }
}

for (const version of [1, 2] as const) {
  describe(`v${version} hosted authentication`, () => {
    for (const recreate of [false, true]) {
      it.live(
        `terminal success allows fresh initialization from ${recreate ? "a new" : "the same"} remote client`,
        () =>
          Effect.scoped(
            Effect.gen(function* () {
              let callbacks = 0
              const h = yield* hostedHarness(version, {
                agent: { version, initialize: initialize(version, true), uniqueSessions: true },
                connect: {
                  terminalAuth: () =>
                    Effect.sync(() => {
                      callbacks++
                    }),
                },
              })
              const oldSession = yield* h.connection.newSession({ cwd: "/work" })
              yield* h.connection.authenticate("login")
              expect(callbacks).toBe(1)
              expect((yield* h.connection.closed)._tag).toBe("AcpConnectionClosed")
              expect(yield* h.storage.load(`${h.gateway.prefix}:connection:test`)).toBeUndefined()
              expect(
                (yield* h.agent.received).filter((message) =>
                  ["authenticate", "auth/login"].includes(String(message.method)),
                ),
              ).toEqual([])
              expect(
                singleFailureOf(yield* Effect.exit(h.connection.newSession({ cwd: "/closed" }))),
              ).toMatchObject({ code: "Closed" })
              expect(singleFailureOf(yield* Effect.exit(oldSession.cancel))).toMatchObject({
                _tag: "AcpGatewayError",
              })
              const remote = recreate
                ? yield* Remote.make(h.gateway, { profile: "test" })
                : h.remote
              const fresh = yield* remote.connect(connectOptions(version))
              yield* fresh.newSession({ cwd: "/fresh" })
              expect(h.opens()).toBe(2)
              expect(fresh.negotiated.version).toBe(version)
              const recreated = yield* Remote.make(h.gateway, { profile: "test" })
              const retained = yield* recreated.connect(connectOptions(version))
              yield* retained.newSession({ cwd: "/retained" })
              expect(h.opens()).toBe(2)
            }),
          ).pipe(Effect.timeout("3 seconds")),
      )
    }

    it.live("agent login retains the initialized connection and descriptor", () =>
      Effect.scoped(
        Effect.gen(function* () {
          const h = yield* hostedHarness(version, {
            agent: { version, initialize: initialize(version, false), uniqueSessions: true },
          })
          const key = `${h.gateway.prefix}:connection:test`
          const saved = yield* h.storage.load(key)
          const authenticating = yield* h.connection.authenticate("login").pipe(Effect.forkChild)
          const method = version === 1 ? "authenticate" : "auth/login"
          expect(yield* h.agent.awaitRequest(method)).toEqual({ methodId: "login" })
          yield* h.agent.respond(method, {})
          yield* Fiber.join(authenticating)
          expect(yield* h.storage.load(key)).toEqual(saved)
          const retained = yield* h.remote.connect(connectOptions(version))
          yield* retained.newSession({ cwd: "/work" })
          expect(h.opens()).toBe(1)
        }),
      ).pipe(Effect.timeout("3 seconds")),
    )

    it.live("an old terminal callback cannot delete a newer retained descriptor", () =>
      Effect.scoped(
        Effect.gen(function* () {
          const started = yield* Deferred.make<void>()
          const finish = yield* Deferred.make<void>()
          const h = yield* hostedHarness(version, {
            agent: { version, initialize: initialize(version, true), uniqueSessions: true },
            connect: {
              terminalAuth: () =>
                Deferred.succeed(started, undefined).pipe(
                  Effect.andThen(Deferred.await(finish)),
                  Effect.asVoid,
                ),
            },
          })
          const authenticating = yield* h.connection.authenticate("login").pipe(Effect.forkChild)
          yield* Deferred.await(started)
          const key = `${h.gateway.prefix}:connection:test`
          yield* h.storage.remove(key)
          const fresh = yield* h.remote.connect(connectOptions(version))
          const newer = yield* h.storage.load(key)
          expect(h.opens()).toBe(2)
          yield* Deferred.succeed(finish, undefined)
          yield* Fiber.join(authenticating)
          expect(yield* h.storage.load(key)).toEqual(newer)
          yield* fresh.newSession({ cwd: "/fresh" })
          const retained = yield* h.remote.connect(connectOptions(version))
          yield* retained.newSession({ cwd: "/retained" })
          expect(h.opens()).toBe(2)
        }),
      ).pipe(Effect.timeout("3 seconds")),
    )
  })
}
