/// A Dart implementation of the Bitswap protocol for libp2p.
///
/// This library provides the main `Bitswap` class for interacting with the Bitswap network,
/// the `BlockStore` interface that clients need to implement for block storage,
/// and the underlying protobuf message definitions.
library;

export 'src/bitswap.dart'; // Exports Bitswap, BitswapOptions and the BlockStore interface
export 'src/proto/bitswap_message.pb.dart'; // Exports protobuf message types

