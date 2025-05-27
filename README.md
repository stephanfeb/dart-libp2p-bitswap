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
  dart_libp2p_bitswap: ^latest_version # Replace with the actual version
  dart_libp2p: any # Your dart_libp2p version
  dart_cid: any    # For CID objects
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

### 1. Implement `IBlockStore`

The Bitswap service requires a block store to manage blocks. You need to provide an implementation of the `IBlockStore` interface. This interface defines how Bitswap interacts with your application's data storage.

```dart
import 'package:dart_libp2p_bitswap/dart_libp2p_bitswap.dart';
import 'package:dart_cid/dart_cid.dart' as dart_cid;
import 'dart:typed_data';

// A simple in-memory block store for example purposes
class MyBlockStore implements IBlockStore {
  final Map<String, Uint8List> _storage = {};

  @override
  Future<Uint8List?> get(dart_cid.CID cid) async {
    print('[MyBlockStore] GET ${cid.toString()}');
    return _storage[cid.toString()];
  }

  @override
  Future<bool> has(dart_cid.CID cid) async {
    final present = _storage.containsKey(cid.toString());
    print('[MyBlockStore] HAS ${cid.toString()}: $present');
    return present;
  }

  @override
  Future<void> put(dart_cid.CID cid, Uint8List data) async {
    print('[MyBlockStore] PUT ${cid.toString()} (data length: ${data.length})');
    _storage[cid.toString()] = data;
    // In a real application, you might notify Bitswap that new blocks are available,
    // or Bitswap might re-query peers based on updated wantlists.
  }
}
```

### 2. Initialize Bitswap

Instantiate the `Bitswap` class with your configured libp2p `Host` and your `IBlockStore` implementation.

```dart
import 'package:dart_libp2p_bitswap/dart_libp2p_bitswap.dart';
import 'package:dart_libp2p/dart_libp2p.dart' as libp2p; // Assuming this is your libp2p import

// Assuming 'host' is your initialized and started libp2p.Host instance
// libp2p.Host host = ...; 
// await host.start();

final myBlockStore = MyBlockStore();
final bitswap = Bitswap(host: host, blockStore: myBlockStore);

print('Bitswap service initialized.');
```

### 3. Requesting Blocks (Wanting Blocks)

To request blocks from the network, use the `wantBlocks` method.

```dart
import 'package:dart_cid/dart_cid.dart' as dart_cid;

// Example CIDs you want to fetch
final cid1 = dart_cid.CID.parse('bafkreibm6jg3ux5qumhcn2b3flc3tyu6dmlb4xa7u5bf44yegnrjhc4yeq'); // Replace with actual CIDs
// final cid2 = dart_cid.CID.parse('...');

await bitswap.wantBlocks([cid1 /*, cid2 */]);
print('Requested CIDs: ${cid1.toString()}');

// Bitswap will now attempt to find peers with these blocks and retrieve them.
// Received blocks will be put into your IBlockStore via the `put` method.
```

### 4. Providing Blocks

When other peers request blocks that are present in your `IBlockStore` (checked via the `has` and `get` methods), the Bitswap service will automatically send them. Ensure your `IBlockStore.put` method correctly stores blocks that your node wishes to share.

### 5. Stopping Bitswap

When your application is shutting down, or you no longer need the Bitswap service, call the `stop` method to clean up resources, such as unregistering protocol handlers and closing active streams.

```dart
await bitswap.stop();
print('Bitswap service stopped.');
```

### Example

A more detailed, runnable example can be found in the `/example` directory of this package. It includes a placeholder for libp2p host setup.

```dart
// (Simplified snippet from example/dart_libp2p_bitswap_example.dart)

// ... (Host and BlockStore setup as above) ...

// Bitswap bitswap = Bitswap(host: host, blockStore: blockStore);

// final exampleData = Uint8List.fromList('Hello Bitswap!'.codeUnits);
// final exampleCid = await computeCid(exampleData); // You'd need a CID computation helper
// await myBlockStore.put(exampleCid, exampleData);

// final cidToWant = dart_cid.CID.parse('bafkreibm6jg3ux5qumhcn2b3flc3tyu6dmlb4xa7u5bf44yegnrjhc4yeq');
// await bitswap.wantBlocks([cidToWant]);

// ... (await bitswap.stop()) ...
```

## Key Components

*   **`Bitswap`**: The main public class for interacting with the Bitswap service.
*   **`IBlockStore`**: An interface you must implement to provide block storage capabilities to the Bitswap service.
*   **`BitswapNetworkService`**: (Internal) Handles the libp2p stream and message framing logic.
*   **`WantManager`**: (Internal) Manages local and peer wantlists.
*   **Protobuf Messages**: The package exports the generated Dart classes for Bitswap messages (e.g., `Message`, `Wantlist`, `Block`) from `src/proto/bitswap_message.pb.dart`.

## Protocol Details

This library implements version **1.2.0** of the Bitswap protocol, identified by the libp2p protocol ID `/ipfs/bitswap/1.2.0`. Key features of 1.2.0 supported include:
*   `Block` and `Have` want types.
*   `sendDontHave` requests.
*   `HAVE` / `DONT_HAVE` block presence messages.
*   CIDv1 support via `prefix` and `data` in block payloads.

For full specification details, please refer to the [official Bitswap spec](https://github.com/ipfs/specs/blob/master/bitswap/README.md).
You can also find a copy of the spec used during development in `bitswap_spec.txt` within this repository.

## Additional Information

### Contributing
Contributions are welcome! Please feel free to open an issue or submit a pull request on the [GitHub repository](https://github.com/your_org/dart-libp2p-bitswap). <!-- Replace with actual repo URL -->

### Filing Issues
If you encounter any bugs or have feature requests, please file an issue on the [GitHub issue tracker](https://github.com/your_org/dart-libp2p-bitswap/issues). <!-- Replace with actual repo URL -->

### License
This package is licensed under the [Your License] license. <!-- TODO: Specify License, e.g., MIT, Apache-2.0 -->
