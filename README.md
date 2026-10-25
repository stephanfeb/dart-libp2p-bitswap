# Dart Libp2p Bitswap

[![Pub Version](https://img.shields.io/pub/v/dart_libp2p_bitswap)](https://pub.dev/packages/dart_libp2p_bitswap) <!-- Replace with actual badge if published -->
[![Dart CI](https://github.com/your_org/dart-libp2p-bitswap/actions/workflows/dart.yml/badge.svg)](https://github.com/your_org/dart-libp2p-bitswap/actions/workflows/dart.yml) <!-- Replace with your CI badge -->

A Dart implementation of the [Bitswap protocol](https://github.com/ipfs/specs/blob/master/bitswap/README.md) for the [libp2p](https://libp2p.io/) networking stack. This package enables Dart applications to exchange content-addressed blocks of data with other peers in the libp2p network using Bitswap.

This implementation targets **Bitswap Protocol Version 1.2.0**.

## Features

*   Implements Bitswap version 1.2.0 (`/ipfs/bitswap/1.2.0`).
*   Requesting blocks from peers using wantlists.
*   Responding to peer requests with blocks from a local block store.
*   Sending and receiving `HAVE` / `DONT_HAVE` messages for blocks.
*   Sending `CANCEL` messages for blocks no longer needed.
*   Management of local and per-peer wantlists.
*   Automatic handling of varint-prefixed message framing.
*   Reuse of outgoing streams for efficient communication.
*   Basic peer disconnection handling to clean up resources.

## Getting Started

### Prerequisites

*   A working Dart environment.
*   An initialized and running `dart_libp2p` `Host` instance. This package relies on an existing libp2p host for network communication, peer discovery, and stream management.
*   Familiarity with CIDs (Content Identifiers) and block-based data storage.

### Installation

If this package is published to pub.dev, add it to your `pubspec.yaml`:

```yaml
dependencies:
  dart_libp2p_bitswap: ^2.0.0
  dart_libp2p: ^1.0.0
  dcid: ^1.0.0     # For CID objects
  # other necessary dependencies
```

If you are using it as a local path dependency:

```yaml
dependencies:
  dart_libp2p_bitswap:
    path: ../path/to/dart_libp2p_bitswap # Adjust path as needed
  # ... other dependencies
```

Then run `dart pub get` or `flutter pub get`.

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

### 2. Create and start Bitswap

Construct `Bitswap` with your libp2p `Host` and block store, then call `start()`. Construction has no side effects; `start()` registers the `/ipfs/bitswap/1.2.0` protocol handler and subscribes to the host's event bus for peer connect/disconnect events.

```dart
// 'host' is your initialized and started libp2p.Host
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

### 4. Providing Blocks

When peers request blocks present in your `BlockStore` (checked via `has` and `get`), Bitswap sends them automatically, split into messages of at most 4 MiB. Peers that send `WANT_HAVE` receive a `HAVE` presence rather than the block.

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

`example/dart_libp2p_bitswap_example.dart` shows a `BlockStore` implementation; host setup there is still a placeholder.

## Key Components

*   **`Bitswap`**: The main public class for interacting with the Bitswap service.
*   **`BlockStore`**: An interface you must implement to provide block storage capabilities to the Bitswap service.
*   **`BitswapOptions`**: Tunables such as the default `getBlock` timeout.
*   **`BitswapClient`** (`lib/src/client/`): (Internal) Fetching — our `LocalWantlist`, broadcasting wants, accepting blocks and presences.
*   **`BitswapServer`** (`lib/src/server/`): (Internal) Serving — the per-peer `PeerLedger`, answering wantlists, size-limited response packing.
*   **`BitswapNetworkService`** (`lib/src/network/`): (Internal) Stream handling, varint framing, and the ordered per-peer send queue.
*   **Protobuf Messages**: The package exports the generated Dart classes for Bitswap messages (e.g., `Message`, `Wantlist`, `Block`) from `src/proto/bitswap_message.pb.dart`.

## Protocol Details

This library implements version **1.2.0** of the Bitswap protocol, identified by the libp2p protocol ID `/ipfs/bitswap/1.2.0`. Key features of 1.2.0 supported include:
*   `Block` and `Have` want types.
*   `sendDontHave` requests.
*   `HAVE` / `DONT_HAVE` block presence messages.
*   CIDv0 and CIDv1 blocks via `prefix` and `data` in block payloads (sha2-256 and sha2-512).
*   4 MiB message size limit, matching go-bitswap / boxo.

For full specification details, please refer to the [official Bitswap spec](https://github.com/ipfs/specs/blob/master/bitswap/README.md).
You can also find a copy of the spec used during development in `bitswap_spec.txt` within this repository.

## Additional Information

### Contributing
Contributions are welcome! Please feel free to open an issue or submit a pull request on the [GitHub repository](https://github.com/your_org/dart-libp2p-bitswap). <!-- Replace with actual repo URL -->

### Filing Issues
If you encounter any bugs or have feature requests, please file an issue on the [GitHub issue tracker](https://github.com/your_org/dart-libp2p-bitswap/issues). <!-- Replace with actual repo URL -->

### License
This package is licensed under the [Your License] license. <!-- TODO: Specify License, e.g., MIT, Apache-2.0 -->
