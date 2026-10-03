/**
 * ACP over a WebSocket using the custom `effect-acp-jsonrpc-v1` profile.
 *
 * **Details**
 *
 * Each WebSocket message carries one complete UTF-8 JSON frame; there is no
 * stdio newline delimiter. The profile versions framing only: it is negotiated
 * independently of the ACP protocol version (v1 or v2), so the same connection
 * carries either once `initialize` has selected it.
 *
 * The client dials through the injected `WebSocketConstructor` service, so this
 * module stays browser-safe; the server side adapts an already-upgraded
 * `Socket` (see `server/BridgeHttp`). Sockets are never reconnected implicitly
 * and frames are never resent after loss: a socket failure or scope release is
 * terminal for the transport.
 */
import * as Cause from "effect/Cause"
import type * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Queue from "effect/Queue"
import * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as Socket from "effect/socket/Socket"
import * as AcpConnector from "../AcpConnector.ts"
import { AcpTransportError } from "../AcpError.ts"
import { AcpTransport, type Transport } from "../AcpTransport.ts"

/**
 * The WebSocket subprotocol this adapter negotiates.
 *
 * @category constants
 */
export const profile = "effect-acp-jsonrpc-v1"

/**
 * Close code sent when a binary frame is received (Unsupported Data).
 *
 * @category constants
 */
export const unsupportedDataClose = 1003
/**
 * Close code sent when a text frame exceeds the size limit (Message Too Big).
 *
 * @category constants
 */
export const tooLargeClose = 1009

/**
 * Frame size, inbound buffering, and socket opening and pressure settings.
 *
 * @category configuration
 */
export interface Options {
  /**
   * Largest text frame accepted or sent, in bytes. Default 16 MiB.
   */
  readonly maxFrameBytes?: number | undefined
  /**
   * Frames buffered before reading pauses. Default 64.
   */
  readonly buffer?: number | undefined
  /**
   * How long to wait for the socket to open. Default 10 seconds.
   */
  readonly openTimeout?: Duration.Input | undefined
  /**
   * Buffered inbound bytes before a non-pausing socket fails. Default 16 MiB.
   * Pausable sockets pause at this limit; callback transports close on overflow.
   */
  readonly highWaterMark?: number | undefined
}

const encoder = new TextEncoder()

const frameBytes = (frame: string): number => encoder.encode(frame).byteLength

// Native sockets accept protocol close codes; browser clients only allow 1000
// or application codes. Closing must never replace the transport's typed error.
const closeSocket = (ws: Socket.WebSocketLike, code = 1000, reason?: string): void => {
  try {
    ws.close(code, reason)
  } catch {
    try {
      ws.close(1000)
    } catch {
      // A failed underlying close cannot be recovered by this adapter.
    }
  }
}

const closedError = new AcpTransportError({ reason: "Closed", message: "WebSocket transport is closed" })

const isClose = (error: Socket.SocketError): boolean => error.reason._tag === "SocketCloseError"

const describe = (error: Socket.SocketError): string =>
  error.reason._tag === "SocketCloseError" ? `Socket closed (${error.reason.code})` : error.message

/**
 * Parses a `Sec-WebSocket-Protocol` header and reports the profile amongst its offers.
 *
 * @category predicates
 */
export const requested = (headers: Readonly<Record<string, string | undefined>>): boolean => {
  const header = headers["sec-websocket-protocol"]
  if (header === undefined) return false
  return header.split(",").some((token) => token.trim() === profile)
}

/**
 * Adapts an Effect socket acquisition to a scoped ACP frame transport.
 *
 * **When to use**
 *
 * Use when the application already owns an upgraded socket or supplies a custom socket acquisition.
 *
 * **Details**
 *
 * Acquires the socket once and holds it for the transport lifetime. Socket acquisition failures
 * become transport Open errors; read and write failures become transport errors.
 *
 * **Gotchas**
 *
 * Binary messages are rejected with close code 1003. Oversized incoming text frames are rejected
 * with close code 1009. No reconnection or frame replay occurs.
 *
 * @see {@link make} for dialing a URL with the required subprotocol.
 * @category constructors
 */
export const fromSocket = <E, R>(
  socket: Effect.Effect<Socket.Socket, E, R>,
  options: Options = {}
): Effect.Effect<Transport, AcpTransportError, R | Scope.Scope> =>
  Effect.gen(function*() {
    const maxFrameBytes = options.maxFrameBytes ?? 16 * 1024 * 1024
    const scope = yield* Scope.Scope
    const socketScope = yield* Scope.fork(scope)
    let closed = false

    const open = (cause: unknown) =>
      new AcpTransportError({ reason: "Open", message: "WebSocket failed to open", cause })
    const sock = yield* Scope.provide(socket, socketScope).pipe(Effect.mapError(open))
    const reader = yield* Scope.provide(sock.reader, socketScope).pipe(Effect.mapError(open))
    const writer = yield* Scope.provide(sock.writer, socketScope)

    const closeWith = (code: number, reason: string) =>
      writer.write(new Socket.CloseEvent(code, reason)).pipe(Effect.ignore)

    const frames = yield* Queue.bounded<string, AcpTransportError | Cause.Done>(options.buffer ?? 64)
    yield* Scope.addFinalizer(socketScope, Effect.suspend(() => {
      closed = true
      return Queue.fail(frames, closedError)
    }))

    const pump = Effect.gen(function*() {
      while (true) {
        const batch = yield* reader.pull
        for (const item of batch) {
          if (typeof item !== "string") {
            yield* closeWith(unsupportedDataClose, "Binary frames are not supported")
            return yield* new AcpTransportError({
              reason: "InvalidFrame",
              message: "Binary WebSocket frames are not supported"
            })
          }
          if (frameBytes(item) > maxFrameBytes) {
            yield* closeWith(tooLargeClose, "Frame exceeds size limit")
            return yield* new AcpTransportError({
              reason: "FrameTooLarge",
              message: `Frame exceeds ${maxFrameBytes} bytes`
            })
          }
          yield* Queue.offer(frames, item)
        }
      }
    })

    yield* pump.pipe(
      Effect.ensuring(Effect.sync(() => {
        closed = true
      })),
      Effect.matchEffect({
        onSuccess: () => Queue.end(frames),
        onFailure: (error) => {
          if (!Socket.isSocketError(error)) return Queue.fail(frames, error)
          if (isClose(error)) return Queue.end(frames)
          return Queue.fail(frames, new AcpTransportError({ reason: "Read", message: describe(error), cause: error }))
        }
      }),
      Effect.forkIn(socketScope)
    )

    const transport: Transport = {
      incoming: Stream.fromQueue(frames),
      send: (frame) =>
        Effect.suspend(() => {
          if (closed) return Effect.fail(closedError)
          if (frameBytes(frame) > maxFrameBytes) {
            return Effect.fail(new AcpTransportError({
              reason: "FrameTooLarge",
              message: `Frame exceeds ${maxFrameBytes} bytes`
            }))
          }
          return writer.write(frame).pipe(
            Effect.mapError((error) => {
              closed = true
              return isClose(error)
                ? closedError
                : new AcpTransportError({ reason: "Write", message: describe(error), cause: error })
            }),
            Effect.tapError((error) => Queue.fail(frames, error))
          )
        })
    }
    return transport
  })

/**
 * Dials `url` with the required subprotocol and returns a scoped transport. The connection is
 * opened once; a missing/mismatched subprotocol, socket failure, or scope release is terminal.
 *
 * @category constructors
 */
export const make = <E = never, R = never>(
  url: string | Effect.Effect<string, E, R>,
  options: Options = {}
): Effect.Effect<Transport, AcpTransportError | E, Socket.WebSocketConstructor | Scope.Scope | R> =>
  Effect.gen(function*() {
    const constructor = yield* Socket.WebSocketConstructor
    const resolved = typeof url === "string" ? url : yield* url
    const maxFrameBytes = options.maxFrameBytes ?? 16 * 1024 * 1024
    const buffer = options.buffer ?? 64
    const highWaterMark = options.highWaterMark ?? 16 * 1024 * 1024
    if (![maxFrameBytes, buffer, highWaterMark].every((limit) => Number.isSafeInteger(limit) && limit > 0)) {
      return yield* new AcpTransportError({ reason: "Open", message: "WebSocket limits must be positive finite integers" })
    }
    const ws = yield* Effect.acquireRelease(
      dial(constructor, resolved).pipe(Effect.interruptible, Effect.timeoutOrElse({
        duration: options.openTimeout ?? "10 seconds", orElse: () => Effect.fail(openFailure("WebSocket open timeout"))
      }), Effect.mapError((cause) => new AcpTransportError({ reason: "Open", message: "WebSocket failed to open", cause }))),
      (ws) => Effect.sync(() => closeSocket(ws))
    )
    const frames = yield* Queue.bounded<string, AcpTransportError | Cause.Done>(buffer)
    let closed = false
    let bufferedBytes = 0
    let paused = false
    const pause = "pause" in ws && typeof ws.pause === "function" ? ws.pause.bind(ws) : undefined
    const resume = "resume" in ws && typeof ws.resume === "function" ? ws.resume.bind(ws) : undefined
    const pausable = pause !== undefined && resume !== undefined

    const fail = (error: AcpTransportError, code = 1011) => {
      if (closed) return
      closed = true
      Queue.failCauseUnsafe(frames, Cause.fail(error))
      // Close at the callback boundary: neither the queue nor a lower socket reader
      // can keep accumulating frames while the application stops reading.
      ws.removeEventListener("message", onMessage)
      closeSocket(ws, code)
    }
    const onMessage = (event: Socket.WebSocketEvent) => {
      if (closed) return
      if (typeof event.data !== "string") {
        fail(new AcpTransportError({ reason: "InvalidFrame", message: "Binary WebSocket frames are not supported" }), unsupportedDataClose)
        return
      }
      const bytes = frameBytes(event.data)
      if (bytes > maxFrameBytes) {
        fail(new AcpTransportError({ reason: "FrameTooLarge", message: `Frame exceeds ${maxFrameBytes} bytes` }), tooLargeClose)
        return
      }
      if (bufferedBytes + bytes > highWaterMark || !Queue.offerUnsafe(frames, event.data)) {
        fail(new AcpTransportError({ reason: "Read", message: "WebSocket inbound capacity exceeded" }), tooLargeClose)
        return
      }
      bufferedBytes += bytes
      if (pausable && !paused && (bufferedBytes >= highWaterMark || Queue.sizeUnsafe(frames) >= buffer)) {
        paused = true
        pause()
      }
    }
    const onError = () => fail(new AcpTransportError({ reason: "Read", message: "WebSocket read failed" }))
    const onClose = () => {
      if (closed) return
      closed = true
      Queue.endUnsafe(frames)
    }
    ws.addEventListener("message", onMessage)
    ws.addEventListener("error", onError)
    ws.addEventListener("close", onClose)
    yield* Effect.addFinalizer(() => Effect.sync(() => {
      ws.removeEventListener("message", onMessage)
      ws.removeEventListener("error", onError)
      ws.removeEventListener("close", onClose)
      closed = true
      Queue.failCauseUnsafe(frames, Cause.fail(closedError))
    }))
    return {
      incoming: Stream.fromQueue(frames).pipe(Stream.tap((frame) => Effect.sync(() => {
        bufferedBytes -= frameBytes(frame)
        if (paused && !closed && bufferedBytes < highWaterMark && Queue.sizeUnsafe(frames) < buffer) {
          paused = false
          resume?.()
        }
      }))),
      send: (frame: string) => Effect.suspend(() => {
        if (closed) return Effect.fail(closedError)
        if (frameBytes(frame) > maxFrameBytes) {
          return Effect.fail(new AcpTransportError({ reason: "FrameTooLarge", message: `Frame exceeds ${maxFrameBytes} bytes` }))
        }
        return Effect.try({
          try: () => ws.send(frame),
          catch: (cause) => new AcpTransportError({ reason: "Write", message: "WebSocket write failed", cause })
        }).pipe(Effect.tapError((error) => Effect.sync(() => fail(error))))
      })
    } satisfies Transport
  })

/** Dials and waits for open, rejecting a peer that did not select the profile. */
const dial = (
  constructor: (url: string, options?: Socket.WebSocketConstructorOptions) => Socket.WebSocketLike,
  url: string
): Effect.Effect<Socket.WebSocketLike, Socket.SocketError, Scope.Scope> =>
  Effect.acquireRelease(
    Effect.interruptible(Effect.callback<Socket.WebSocketLike, Socket.SocketError>((resume) => {
      let ws: Socket.WebSocketLike
      try {
        ws = constructor(url, [profile])
      } catch (cause) {
        resume(Effect.fail(openFailure(cause)))
        return
      }
      let settled = false
      const remove = () => {
        ws.removeEventListener("open", onOpen)
        ws.removeEventListener("error", onError)
        ws.removeEventListener("close", onClose)
      }
      const finish = (effect: Effect.Effect<Socket.WebSocketLike, Socket.SocketError>) => {
        if (settled) return
        settled = true
        remove()
        resume(effect)
      }
      const onOpen = () => finish(selected(ws).pipe(Effect.tapError(() => Effect.sync(() => closeSocket(ws, 1002, "Subprotocol mismatch")))))
      const onError = (event: Socket.WebSocketEvent) => finish(Effect.fail(openFailure(event)))
      const onClose = (event: Socket.WebSocketEvent) =>
        finish(Effect.fail(new Socket.SocketError({
          reason: new Socket.SocketCloseError({
            code: typeof event.code === "number" ? event.code : 1006,
            ...(event.reason === undefined ? {} : { closeReason: event.reason })
          })
        })))
      ws.addEventListener("open", onOpen, { once: true })
      ws.addEventListener("error", onError, { once: true })
      ws.addEventListener("close", onClose, { once: true })
      if (ws.readyState === 1) onOpen()
      return Effect.sync(() => {
        if (!settled) {
          settled = true
          remove()
          closeSocket(ws)
        }
      })
    })),
    (ws) => Effect.sync(() => closeSocket(ws))
  )

const openFailure = (cause: unknown): Socket.SocketError =>
  new Socket.SocketError({ reason: new Socket.SocketOpenError({ kind: "Unknown", cause }) })

const selected = (ws: Socket.WebSocketLike): Effect.Effect<Socket.WebSocketLike, Socket.SocketError> => {
  const negotiated = "protocol" in ws ? ws.protocol : undefined
  if (negotiated === profile) return Effect.succeed(ws)
  let actual = "an invalid subprotocol value"
  if (negotiated === undefined || negotiated === "") actual = "no subprotocol"
  else if (typeof negotiated === "string") actual = `subprotocol "${negotiated}"`
  return Effect.fail(openFailure(
    new Error(`Peer selected ${actual}; expected "${profile}"`)
  ))
}

/**
 * Provides an `AcpConnector` that dials a fresh socket for every connection.
 *
 * **Gotchas**
 *
 * An effectful `url` is resolved once, when the layer builds. For a per-connection URL, use
 * `AcpConnector.layer(WebSocket.make(url))`.
 *
 * @category layers
 */
export const layer = <E = never, R = never>(
  url: string | Effect.Effect<string, E, R>,
  options?: Options
): Layer.Layer<AcpConnector.AcpConnector, E, Socket.WebSocketConstructor | Exclude<R, Scope.Scope>> =>
  Layer.effect(
    AcpConnector.AcpConnector,
    Effect.flatMap(typeof url === "string" ? Effect.succeed(url) : url, (resolved) => AcpConnector.make(make(resolved, options)))
  )

/**
 * Uses an injected accepted socket, e.g. after an HTTP upgrade.
 *
 * @category layers
 */
export const layerSocket = (options?: Options): Layer.Layer<AcpTransport, AcpTransportError, Socket.Socket> =>
  Layer.effect(AcpTransport, fromSocket(Socket.Socket, options))
