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

  /// How often outstanding wants are re-sent to peers not yet asked, and
  /// re-targeted when no peer holds an outstanding WANT_BLOCK.
  final Duration rebroadcastInterval;

  /// Most wants remembered per remote peer. Wantlist entries for new CIDs
  /// beyond this are ignored, bounding the memory one peer can claim.
  final int maxWantsPerPeer;

  /// Block bytes served to one peer before moving on to the next, so a peer
  /// with a large wantlist cannot starve the others.
  final int serveBytesPerTurn;

  const BitswapOptions({
    this.getBlockTimeout = const Duration(seconds: 30),
    this.rebroadcastInterval = const Duration(seconds: 10),
    this.maxWantsPerPeer = 1024,
    this.serveBytesPerTurn = 1024 * 1024,
  });
}
