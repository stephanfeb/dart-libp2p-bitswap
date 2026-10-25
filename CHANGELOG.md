## 2.0.0

Breaking changes:

- `IBlockStore` is renamed to `BlockStore`.
- `Bitswap` no longer takes `peerEventsStream`; it subscribes to `host.eventBus` itself.
- Construction has no side effects. Call `await bitswap.start()` before use; `wantBlocks`/`getBlock` throw `StateError` if not started or after `stop()`.
- Unsolicited blocks (not on the local wantlist) are discarded instead of stored.
- `getBlock` takes an optional `timeout` (default `BitswapOptions.getBlockTimeout`, 30s). It returns `null` only on timeout or cancellation; other errors propagate.
- `stop()` fails pending `getBlock` calls with `StateError`.
- The library logs via `package:logging` and no longer prints.

New:

- `BitswapOptions`, `Bitswap.cancelWants`.

Fixes:

- CIDv0 blocks from peers (including go-bitswap) are now accepted.
- Sends to a peer are serialised over one stream, preserving order; a failed write on a reused stream is retried once.
- Inbound messages over 4 MiB are rejected; responses are split to stay under 4 MiB.
- Peers sending `WANT_HAVE` get a `HAVE` presence instead of the block.
- Concurrent `getBlock` calls share one want; one caller timing out no longer abandons it for others, and an abandoned want is cancelled with peers.

## 1.0.0

- Initial version.
