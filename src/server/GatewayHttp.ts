import * as Schema from "effect/Schema"
/**
 * Authenticated Effect RPC WebSocket gateway, mounted in an existing router.
 */
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import * as HttpRouter from "effect/http/HttpRouter"
import * as Request from "effect/http/HttpServerRequest"
import * as Response from "effect/http/HttpServerResponse"
import * as RpcServer from "effect/rpc/RpcServer"
import * as RpcSerialization from "effect/rpc/RpcSerialization"
import * as AcpGateway from "../AcpGateway.ts"
import { AcpHost } from "../AcpHost.ts"

/**
 * Mounted path, request authentication, and origin policy for the gateway WebSocket route.
 *
 * @category configuration
 */
export interface Options<R = never> {
  /**
   * GET path for WebSocket upgrades. Defaults to `/acp/gateway`.
   */
  readonly path?: `/${string}`
  /**
   * Resolves the authenticated principal before upgrading; failure returns HTTP 401.
   */
  readonly authenticate: (request: Request.HttpServerRequest) => Effect.Effect<AcpGateway.Identity, AcpGateway.GatewayError, R>
  /**
   * Decides whether the Origin header is allowed, including when it is absent.
   */
  readonly allowOrigin: (origin: string | undefined) => boolean
}
/**
 * Registers an authenticated WebSocket route for the hosted gateway RPC protocol.
 *
 * **Details**
 *
 * The path defaults to `/acp/gateway` . Origin denial returns HTTP 403; authentication failure
 * returns HTTP 401. A successful `Hello` is required before other RPC methods.
 *
 * **Gotchas**
 *
 * Supply the host service and an application HTTP server. The authenticated identity is captured at
 * upgrade time, rather than accepted from RPC payloads.
 *
 * @category running
 */
export const route = <R>(options: Options<R>) => HttpRouter.route("GET", options.path ?? "/acp/gateway", Effect.gen(function*() {
  const request = yield* Request.HttpServerRequest
  if (!options.allowOrigin(request.headers.origin)) return Response.empty({ status: 403 })
  const authenticated = yield* Effect.exit(options.authenticate(request))
  if (authenticated._tag === "Failure") {
    if (Cause.hasInterruptsOnly(authenticated.cause)) {
      return yield* Effect.failCause(Cause.fromReasons<never>(authenticated.cause.reasons.filter(Cause.isInterruptReason)))
    }
    const reasons = authenticated.cause.reasons
    if (reasons.length !== 1 || !Cause.isFailReason(reasons[0]!) || !Schema.is(AcpGateway.GatewayError)(reasons[0]!.error)) {
      yield* Effect.logError("Gateway authentication failed")
    }
    return Response.empty({ status: 401 })
  }
  const identity = authenticated.value
  const host = yield* AcpHost
  let compatible = false
  const ready = Effect.suspend(() => compatible ? Effect.void : Effect.fail(AcpGateway.failure("UnsupportedVersion")))
  const safeFailure = (cause: Cause.Cause<unknown>) => {
    const reason = cause.reasons.length === 1 ? cause.reasons[0] : undefined
    const error = reason && Cause.isFailReason(reason) ? reason.error : undefined
    return Schema.is(AcpGateway.GatewayError)(error) ? AcpGateway.failure(error.code) : AcpGateway.failure("AgentFailure")
  }
  const reportFailure = (cause: Cause.Cause<unknown>) =>
    cause.reasons.length === 1 && Cause.isFailReason(cause.reasons[0]!) && Schema.is(AcpGateway.GatewayError)(cause.reasons[0]!.error)
      ? Effect.void : Effect.logError("Gateway request failed")
  const protect = <A, E, R2>(effect: Effect.Effect<A, E, R2>) => effect.pipe(Effect.catchCause((cause) =>
    Cause.hasInterruptsOnly(cause)
      ? Effect.failCause(Cause.fromReasons<never>(cause.reasons.filter(Cause.isInterruptReason)))
      : reportFailure(cause).pipe(Effect.andThen(Effect.fail(safeFailure(cause))))))
  const handlers = AcpGateway.Gateway.toLayer({
    Hello: (input) => protect(host.hello(identity, input).pipe(Effect.tap(() => Effect.sync(() => { compatible = true })))),
    Admit: (input) => protect(ready.pipe(Effect.andThen(host.admit(identity, input)))),
    Operation: (input) => protect(ready.pipe(Effect.andThen(host.operation(identity, input)))),
    Closed: (input) => protect(ready.pipe(Effect.andThen(host.closed(identity, input)))),
    List: (input) => protect(ready.pipe(Effect.andThen(host.list(identity, input)))),
    Attach: (input) => Stream.unwrap(Effect.as(ready, host.attach(identity, input))).pipe(Stream.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause)
        ? Stream.failCause(Cause.fromReasons<never>(cause.reasons.filter(Cause.isInterruptReason)))
        : Stream.unwrap(Effect.as(reportFailure(cause), Stream.fail(safeFailure(cause))))))
  })
  // Public primitive underlying layerProtocolWebsocket. Construct per upgrade so
  // authenticated identity is captured by handlers, never trusted from RPC payloads.
  const { protocol, httpEffect } = yield* RpcServer.makeProtocolWithHttpEffectWebsocket.pipe(Effect.provide(RpcSerialization.layerNdjson))
  yield* RpcServer.make(AcpGateway.Gateway, { disableTracing: true, disableFatalDefects: true }).pipe(
    Effect.provideService(RpcServer.Protocol, protocol), Effect.provide(handlers), Effect.forkScoped)
  return yield* httpEffect
}))
