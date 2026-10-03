# effect-acp

## 0.1.1

### Patch Changes

- ebafc6d: Fix ACP v1/v2 negotiation, request-scoped elicitation, terminal authentication lifecycle, version-aware session projection, and agent prompt acceptance and cancellation. Preserve accepted v2 message IDs after retention failures or request cancellation, report failed turns as errors, and drain cancellation before session closure.

  Bound browser WebSocket and MessagePort receive queues, keep notification dispatch alive after handler interruption, enforce integer JSON-RPC error codes, sanitize hosted failure diagnostics, and validate attachment descriptors before controller takeover.

## 0.1.0

### Minor Changes

- 31c7521: Publish the initial effect-acp release on stable Effect 4 with Effect-native ACP clients, scoped transports, agent authoring, browser bridges, hosted sessions, and generated protocol schemas.
