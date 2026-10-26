import 'package:dart_libp2p/dart_libp2p.dart' as libp2p;
import 'package:dcid/dcid.dart';

import '../proto/bitswap_message.pb.dart' as pb;

/// A block a remote peer has asked us for.
class PeerWant {
  final CID cid;
  final int priority;
  final pb.Message_Wantlist_WantType wantType;
  final bool sendDontHave;

  const PeerWant({
    required this.cid,
    this.priority = 1,
    this.wantType = pb.Message_Wantlist_WantType.Block,
    this.sendDontHave = false,
  });

  @override
  String toString() {
    return 'PeerWant(cid: $cid, priority: $priority, type: $wantType, sendDontHave: $sendDontHave)';
  }
}

/// What each remote peer currently wants from us.
class PeerLedger {
  /// Most wants kept per peer; further new CIDs are ignored.
  final int maxWantsPerPeer;

  // Key: PeerId, Value: Map<String (CID), PeerWant>
  final Map<libp2p.PeerId, Map<String, PeerWant>> _wants = {};

  PeerLedger({this.maxWantsPerPeer = 1024});

  /// Adds or replaces [peerId]'s want for [cid]. Returns false, recording
  /// nothing, if [cid] is new and [peerId] already has [maxWantsPerPeer].
  bool addWant(
    libp2p.PeerId peerId,
    CID cid, {
    int priority = 1,
    pb.Message_Wantlist_WantType wantType = pb.Message_Wantlist_WantType.Block,
    bool sendDontHave = false,
  }) {
    final peerWants = _wants.putIfAbsent(peerId, () => {});
    final key = cid.toString();
    if (peerWants.length >= maxWantsPerPeer && !peerWants.containsKey(key)) {
      return false;
    }
    peerWants[key] = PeerWant(
      cid: cid,
      priority: priority,
      wantType: wantType,
      sendDontHave: sendDontHave,
    );
    return true;
  }

  /// [peerId]'s current want for [cid], if any.
  PeerWant? want(libp2p.PeerId peerId, CID cid) =>
      _wants[peerId]?[cid.toString()];

  /// Removes [peerId]'s want for [cid] (cancelled, or fulfilled by us).
  PeerWant? removeWant(libp2p.PeerId peerId, CID cid) {
    final peerWants = _wants[peerId];
    if (peerWants == null) return null;
    final removed = peerWants.remove(cid.toString());
    if (peerWants.isEmpty) _wants.remove(peerId);
    return removed;
  }

  /// Returns peerId -> want for every peer wanting [cid].
  Map<libp2p.PeerId, PeerWant> peersWanting(CID cid) {
    final cidStr = cid.toString();
    return {
      for (final entry in _wants.entries)
        if (entry.value[cidStr] case final want?) entry.key: want,
    };
  }

  Map<String, PeerWant> wantsFor(libp2p.PeerId peerId) =>
      Map.unmodifiable(_wants[peerId] ?? const {});

  /// Forgets everything [peerId] wants (disconnect, or a full wantlist).
  void clearPeer(libp2p.PeerId peerId) => _wants.remove(peerId);
}
