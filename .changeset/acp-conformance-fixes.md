---
"effect-acp": patch
---

Fix ACP v1/v2 negotiation, request-scoped elicitation, terminal authentication lifecycle, version-aware session projection, and agent prompt acceptance and cancellation. Preserve accepted v2 message IDs after retention failures or request cancellation, report failed turns as errors, and drain cancellation before session closure.

Bound browser WebSocket and MessagePort receive queues, keep notification dispatch alive after handler interruption, enforce integer JSON-RPC error codes, sanitize hosted failure diagnostics, and validate attachment descriptors before controller takeover.
