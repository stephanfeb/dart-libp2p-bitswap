## 2.0.0

Breaking changes:

- `IBlockStore` is renamed to `BlockStore`.
- `Bitswap` no longer takes `peerEventsStream`; it subscribes to `host.eventBus` itself.
- Construction has no side effects. Call `await bitswap.start()` before use; `wantBlocks`/`getBlock` throw `StateError` if not started or after `stop()`.
- Unsolicited blocks (not on the local wantlist) are discarded instead of stored.
- `getBlock` takes an optional `timeout` (default `BitswapOptions.getBlockTimeout`, 30s). It returns `null` only on timeout or cancellation; other errors propagate.
- `stop()` fails pending `getBlock` calls with `StateError`, and stops rebroadcasts and queued replies before its first await, so nothing more is sent while it runs.
- The library logs via `package:logging` and no longer prints.

New:

- `BitswapOptions` (`getBlockTimeout`, `rebroadcastInterval`), `Bitswap.cancelWants`.
- Blocks hashed with sha3-224/256/384/512, keccak-256/512, blake2b-256/512, blake3 (1–128-byte digests) or identity (up to 128 bytes) are verified and accepted, in addition to sha2-256/512.
- A runnable two-node example (`example/dart_libp2p_bitswap_example.dart`).
- Optional `SizedBlockStore` (`getSize`), which lets Bitswap size blocks without reading them. With one, responses carry `pendingBytes` (bytes still queued for the peer, counted as boxo does). Received `pendingBytes` is ignored, as in boxo.
- `Bitswap.notifyNewBlock` for blocks the application stores itself: serves peers waiting for it and completes local wants.
- Bitswap 1.2.0 want strategy: `WANT_BLOCK` to one peer and `WANT_HAVE` to the rest, with `sendDontHave`; on `DONT_HAVE` or disconnect, `WANT_BLOCK` moves to a peer that said `HAVE`.
- Outstanding wants are rebroadcast periodically, reaching peers that connect after the want was made.
- Later wants to a peer are sent as deltas; only the first message carries `full`.

Fixes:

- CIDv0 blocks from peers (including go-bitswap) are now accepted.
- Sends to a peer are serialised over one stream, preserving order; a failed write on a reused stream is retried once.
- An outgoing stream the remote closes or resets is dropped as soon as that is seen, so the next send opens a fresh stream instead of first failing a write.
- Inbound messages over 4 MiB are rejected; responses are split to stay under 4 MiB.
- Peers sending `WANT_HAVE` get a `HAVE` presence instead of the block, or the block itself when it is at most `BitswapOptions.wantHaveReplaceSize` bytes (default 1024, as in boxo).
- The wantlist is sent on connect only to peers that speak Bitswap, rather than to every connected peer.
- Serving is bounded and fair: at most `maxWantsPerPeer` wants per peer (default 1024); peers take turns of up to `serveBytesPerTurn` (default 1 MiB), highest priority first; serving no longer blocks the peer's read loop; a CANCEL received while a block is being read stops it being sent.
- Concurrent `getBlock` calls share one want; one caller timing out no longer abandons it for others, and an abandoned want is cancelled with peers.

## 1.0.0

- Initial version.
