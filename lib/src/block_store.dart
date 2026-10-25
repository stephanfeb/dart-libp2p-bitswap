import 'dart:typed_data';

import 'package:dcid/dcid.dart' as dart_cid;

/// Storage for blocks that Bitswap serves to peers and fills from the network.
abstract class BlockStore {
  Future<bool> has(dart_cid.CID cid);
  Future<Uint8List?> get(dart_cid.CID cid);
  Future<void> put(dart_cid.CID cid, Uint8List data);
}

/// Tunables for a Bitswap instance.
class BitswapOptions {
  /// How long `Bitswap.getBlock` waits for a block unless told otherwise.
  final Duration getBlockTimeout;

  const BitswapOptions({this.getBlockTimeout = const Duration(seconds: 30)});
}
