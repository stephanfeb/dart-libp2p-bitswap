# Dart Libp2p Bitswap

[![Pub Version](https://img.shields.io/pub/v/dart_libp2p_bitswap)](https://pub.dev/packages/dart_libp2p_bitswap)

A Dart implementation of the [Bitswap protocol](https://github.com/ipfs/specs/blob/main/src/bitswap-protocol.md) for [dart_libp2p](https://pub.dev/packages/dart_libp2p). It lets Dart applications exchange content-addressed blocks with other libp2p peers. Protocol behaviour (want strategy, limits, defaults) follows [boxo](https://github.com/ipfs/boxo), the Go implementation used by kubo; interoperability with Go nodes has not yet been tested.

This implementation targets **Bitswap 1.2.0** (`/ipfs/bitswap/1.2.0`).

## Features

*   **Fetching:** `getBlock` / `wantBlocks` with the 1.2.0 strategy — `WANT_BLOCK` to one peer, `WANT_HAVE` to the rest, with `DONT_HAVE` and disconnects moving the request to a peer that has the block. Wants are rebroadcast periodically and cancelled with every peer asked once a block arrives.
*   **Serving:** answers peers' wantlists from your `BlockStore`, pushes blocks to peers waiting for them as they arrive, and sends tiny blocks directly in reply to `WANT_HAVE`.
*   **Bounded and fair:** per-peer wantlist limits, round-robin serving by priority, 4 MiB message limits in both directions, and serving kept off the network read path.
*   **Verification:** every received block is re-hashed against its CID; unsolicited blocks are discarded. Supports CIDv0/v1 with sha2, sha3, keccak, blake2b, blake3 and identity multihashes.
*   **Lifecycle:** explicit `start()` / `stop()`, logging via `package:logging`, no side effects at construction.

## Getting Started

### Requirements

*   Dart SDK 3.5 or later.
*   [`dart_libp2p`](https://pub.dev/packages/dart_libp2p) 4.12 or later (within 4.x), with a started `Host`. Bitswap uses the host for streams, its event bus for peer connect/disconnect events, and its peer store to find peers that speak Bitswap.
*   [`dcid`](https://pub.dev/packages/dcid) for CIDs.

### Installation

```sh
dart pub add dart_libp2p_bitswap
```

or add it to your `pubspec.yaml`:

```yaml
dependencies:
  dart_libp2p_bitswap: ^2.0.0
  dart_libp2p: ^4.12.0
  dcid: ^1.0.0
```

Upgrading from 1.x? See the [changelog](CHANGELOG.md) for the breaking changes in 2.0.0.

## Usage

### 1. Implement `BlockStore`

Bitswap reads blocks it serves from, and writes blocks it fetches to, a `BlockStore` you provide.

```dart
import 'dart:typed_data';

import 'package:dart_libp2p_bitswap/dart_libp2p_bitswap.dart';
import 'package:dcid/dcid.dart' as dart_cid;

// A simple in-memory block store for example purposes
class MyBlockStore implements BlockStore {
  final Map<String, Uint8List> _storage = {};

  @override
  Future<Uint8List?> get(dart_cid.CID cid) async => _storage[cid.toString()];

  @override
  Future<bool> has(dart_cid.CID cid) async => _storage.containsKey(cid.toString());

  @override
  Future<void> put(dart_cid.CID cid, Uint8List data) async => _storage[cid.toString()] = data;
}
```

If your store can report a block's size without reading it, implement `SizedBlockStore` (adds `Future<int?> getSize(CID)`) instead. Bitswap then avoids reading large blocks when deciding how to answer a `WANT_HAVE` (see below), and can tell peers how many bytes it still has queued for them (`pendingBytes`, as boxo does).

### 2. Create and start Bitswap

Construct `Bitswap` with your libp2p `Host` and block store, then call `start()`. Construction has no side effects; `start()` registers the `/ipfs/bitswap/1.2.0` protocol handler and subscribes to the host's event bus for peer connect/disconnect events.

```dart
// 'host' is your initialized and started libp2p.Host; see the example
// below for a complete host setup
final bitswap = Bitswap(
  host: host,
  blockStore: MyBlockStore(),
  options: const BitswapOptions(getBlockTimeout: Duration(seconds: 30)), // optional
);
await bitswap.start();
```

### 3. Fetching blocks

`getBlock` returns a block from the local store if present, otherwise wants it from connected Bitswap peers and waits for it. It returns `null` if the block does not arrive within the timeout or its want is cancelled.

```dart
final cid = dart_cid.CID.fromString('bafkreibm6jg3ux5qumhcn2b3flc3tyu6dmlb4xa7u5bf44yegnrjhc4yeq');

final data = await bitswap.getBlock(cid, timeout: const Duration(seconds: 10));
if (data == null) print('Block not found in time');
```

To prefetch without waiting, use `wantBlocks`; received blocks are written to your `BlockStore`. Only blocks on the wantlist are accepted — unsolicited blocks are discarded. To stop wanting blocks, call `cancelWants`, which also sends `CANCEL` to the peers that were asked:

```dart
await bitswap.wantBlocks([cid1, cid2]);
// ...
bitswap.cancelWants([cid2]);
```

Wants follow the Bitswap 1.2.0 pattern: each block is requested with `WANT_BLOCK` from one peer and `WANT_HAVE` from every other Bitswap peer, all with `sendDontHave`. If the `WANT_BLOCK` peer replies `DONT_HAVE` or disconnects, the `WANT_BLOCK` moves to a peer that said `HAVE`. Outstanding wants are rebroadcast every `BitswapOptions.rebroadcastInterval` (default 10s), so peers that connect or finish identify later are asked too. Newly connected peers get the wantlist straight away if their identify info lists `/ipfs/bitswap/1.2.0`.

### 4. Providing Blocks

When peers request blocks present in your `BlockStore` (checked via `has` and `get`), Bitswap sends them automatically, split into messages of at most 4 MiB. Peers that send `WANT_HAVE` receive a `HAVE` presence rather than the block, unless the block is at most `BitswapOptions.wantHaveReplaceSize` bytes (default 1024, as in boxo; 0 disables): then it is cheaper to send the block itself.

Serving runs off the network read path. Peers take turns, each served up to `BitswapOptions.serveBytesPerTurn` (default 1 MiB) of blocks per turn, highest-priority wants first. Each peer may have at most `BitswapOptions.maxWantsPerPeer` (default 1024) outstanding wants; entries beyond that are ignored. A want cancelled while its block is being read is not sent.

Peers whose wants arrive before you have the block are remembered. When your application adds a block to the `BlockStore` itself, call `notifyNewBlock` so those peers are served, and so any pending `getBlock` for it completes:

```dart
await blockStore.put(cid, data);
bitswap.notifyNewBlock(cid, data);
```

### 5. Stopping Bitswap

Call `stop()` when done. It unregisters the protocol handler, closes all streams, and fails any pending `getBlock` calls with a `StateError`. A stopped instance cannot be restarted.

```dart
await bitswap.stop();
print('Bitswap service stopped.');
```

### 6. Logging

The library logs through [`package:logging`](https://pub.dev/packages/logging) and prints nothing by default. Loggers are `bitswap`, `bitswap.network` and `bitswap.wantmanager`; problems are logged at `WARNING`, protocol activity at `FINE`, and per-chunk stream I/O at `FINEST`.

```dart
import 'package:logging/logging.dart';

hierarchicalLoggingEnabled = true;
Logger('bitswap').level = Level.FINE;
Logger('bitswap').onRecord.listen((r) => print('${r.level.name} ${r.loggerName}: ${r.message}'));
```

### Example

`example/dart_libp2p_bitswap_example.dart` starts two in-process hosts (UDX transport, Noise, Yamux), stores a block on one and fetches it from the other:

```bash
dart run example/dart_libp2p_bitswap_example.dart
```

It also shows a minimal in-memory `BlockStore`. Note that dart_libp2p's default address factory drops loopback addresses; the example passes `Libp2p.addrsFactory((addrs) => addrs)` so the two local hosts can dial each other.

## Key Components

*   **`Bitswap`**: The main public class for interacting with the Bitswap service.
*   **`BlockStore`**: An interface you must implement to provide block storage capabilities to the Bitswap service.
*   **`BitswapOptions`**: Tunables: the default `getBlock` timeout, the wantlist rebroadcast interval, and serving limits (`maxWantsPerPeer`, `serveBytesPerTurn`) and `wantHaveReplaceSize`.
*   **`BitswapClient`** (`lib/src/client/`): (Internal) Fetching — our `LocalWantlist`, broadcasting wants, accepting blocks and presences.
*   **`BitswapServer`** (`lib/src/server/`): (Internal) Serving — the per-peer `PeerLedger`, answering wantlists, size-limited response packing.
*   **`BitswapNetworkService`** (`lib/src/network/`): (Internal) Stream handling, varint framing, and the ordered per-peer send queue.
*   **Protobuf Messages**: The package exports the generated Dart classes for Bitswap messages (e.g., `Message`, `Wantlist`, `Block`) from `src/proto/bitswap_message.pb.dart`.

## Protocol Details

This library implements version **1.2.0** of the Bitswap protocol, identified by the libp2p protocol ID `/ipfs/bitswap/1.2.0`. Key features of 1.2.0 supported include:
*   `Block` and `Have` want types.
*   `sendDontHave` requests.
*   `HAVE` / `DONT_HAVE` block presence messages.
*   CIDv0 and CIDv1 blocks via `prefix` and `data` in block payloads. Received blocks are re-hashed and verified with: sha2-256/512, sha3-224/256/384/512, keccak-256/512, blake2b-256/512, blake3 (digests of 1–128 bytes), and identity (up to 128 bytes). Blocks using other hash functions are rejected.
*   4 MiB message size limit, matching go-bitswap / boxo.

For full specification details, please refer to the [official Bitswap spec](https://github.com/ipfs/specs/blob/main/src/bitswap-protocol.md).
You can also find a copy of the spec used during development in `bitswap_spec.txt` within this repository.

## Additional Information

### Contributing and issues

Contributions are welcome. Please open an issue or pull request on [GitHub](https://github.com/stephanfeb/dart-libp2p-bitswap); bugs and feature requests go to the [issue tracker](https://github.com/stephanfeb/dart-libp2p-bitswap/issues).

### License

MIT; see [LICENSE](LICENSE).
