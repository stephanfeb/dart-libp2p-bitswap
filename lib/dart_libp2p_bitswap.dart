/// A Dart implementation of the Bitswap protocol for libp2p.
///
/// This library provides the main `Bitswap` class for interacting with the Bitswap network,
/// the `IBlockStore` interface that clients need to implement for block storage,
/// and the underlying protobuf message definitions.
library dart_libp2p_bitswap;

export 'src/bitswap.dart'; // Exports Bitswap class and IBlockStore interface
export 'src/proto/bitswap_message.pb.dart'; // Exports protobuf message types

// The 'src/dart_libp2p_bitswap_base.dart' was a placeholder and is not exported.
