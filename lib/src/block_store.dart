import 'dart:typed_data';

import 'package:dcid/dcid.dart' as dart_cid;

/// Storage for blocks that Bitswap serves to peers and fills from the network.
abstract class BlockStore {
  Future<bool> has(dart_cid.CID cid);
  Future<Uint8List?> get(dart_cid.CID cid);
  Future<void> put(dart_cid.CID cid, Uint8List data);
}

/// A [BlockStore] that can report a block's size without reading it.
///
/// Optional: Bitswap uses [getSize] to check cheaply whether a block is
/// small enough to send in reply to a WANT_HAVE (see
/// [BitswapOptions.wantHaveReplaceSize]). With a plain [BlockStore] it reads
/// the block instead.
abstract class SizedBlockStore implements BlockStore {
  /// The size in bytes of [cid]'s block, or null if it is not stored.
  Future<int?> getSize(dart_cid.CID cid);
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

  /// Blocks up to this many bytes are sent in reply to a WANT_HAVE instead
  /// of a HAVE, saving the peer a round trip (as boxo does). 0 disables it.
  final int wantHaveReplaceSize;

  const BitswapOptions({
    this.getBlockTimeout = const Duration(seconds: 30),
    this.rebroadcastInterval = const Duration(seconds: 10),
    this.maxWantsPerPeer = 1024,
    this.serveBytesPerTurn = 1024 * 1024,
    this.wantHaveReplaceSize = 1024,
  });
}
