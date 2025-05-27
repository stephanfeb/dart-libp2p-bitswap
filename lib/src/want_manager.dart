import './proto/bitswap_message.pb.dart' as pb;
import 'package:dart_cid/dart_cid.dart'; // Reverting to standard package import

// Represents an entry in a wantlist
class WantlistEntry {
  final CID cid; // Changed Cid to CID
  final int priority;
  final pb.Message_Wantlist_WantType wantType;
  final bool sendDontHave;
  final Set<String> peersSentTo; // Peers to whom this want has been sent

  WantlistEntry({
    required this.cid, // Changed Cid to CID
    this.priority = 1,
    this.wantType = pb.Message_Wantlist_WantType.Block,
    this.sendDontHave = false,
  }) : peersSentTo = {};

  @override
  String toString() {
    return 'WantlistEntry(cid: $cid, priority: $priority, type: $wantType, sendDontHave: $sendDontHave, peersSentTo: ${peersSentTo.length})';
  }
}

class WantManager {
  // Blocks our local node wants from the network
  // Key: CID as String
  final Map<String, WantlistEntry> _localWantlist = {};

  // Blocks remote peers want from our local node
  // Key: PeerId (String), Value: Map<String (CID), WantlistEntry>
  final Map<String, Map<String, WantlistEntry>> _peerWantlists = {};

  WantManager() {
    print('WantManager instance created.');
  }

  // --- Local Wantlist Management ---

  // Add a block to the local wantlist
  // Returns the entry, or null if it already existed with the same wantType
  WantlistEntry? addLocalWant(CID cid, { // Changed Cid to CID
    int priority = 1,
    pb.Message_Wantlist_WantType wantType = pb.Message_Wantlist_WantType.Block,
    bool sendDontHave = false,
  }) {
    final cidStr = cid.toString();
    if (_localWantlist.containsKey(cidStr) && _localWantlist[cidStr]!.wantType == wantType) {
      // Already wanting with same type, perhaps update priority or peersSentTo if needed
      return null; 
    }
    final entry = WantlistEntry(
      cid: cid, // Changed Cid to CID
      priority: priority,
      wantType: wantType,
      sendDontHave: sendDontHave,
    );
    _localWantlist[cidStr] = entry;
    print('Added to local wantlist: $cidStr');
    return entry;
  }

  // Call this when a local want is sent to a specific peer
  void recordWantSentToPeer(CID cid, String peerId) { // Changed Cid to CID
    final cidStr = cid.toString();
    _localWantlist[cidStr]?.peersSentTo.add(peerId);
  }

  Set<String> getPeersSentWantForCid(CID cid) { // Changed Cid to CID
    final cidStr = cid.toString();
    return Set.unmodifiable(_localWantlist[cidStr]?.peersSentTo ?? {});
  }

  // Remove a block from the local wantlist (e.g., block received or cancelled by local client)
  WantlistEntry? removeLocalWant(CID cid) {
    final removed = _localWantlist.remove(cid.toString());
    if (removed != null) {
      print('Removed from local wantlist: ${cid.toString()}');
    }
    return removed;
  }

  bool isWantingLocally(CID cid) {
    return _localWantlist.containsKey(cid.toString());
  }

  List<WantlistEntry> getLocalWants() {
    return _localWantlist.values.toList();
  }

  // --- Peer Wantlist Management ---

  // Add/update a want from a remote peer
  void addPeerWant(String peerId, CID cid, {
    int priority = 1,
    pb.Message_Wantlist_WantType wantType = pb.Message_Wantlist_WantType.Block,
    bool sendDontHave = false,
  }) {
    _peerWantlists.putIfAbsent(peerId, () => {});
    final cidStr = cid.toString();
    _peerWantlists[peerId]![cidStr] = WantlistEntry(
      cid: cid,
      priority: priority,
      wantType: wantType,
      sendDontHave: sendDontHave,
    );
    print('Peer $peerId added/updated want for $cidStr');
  }

  // Remove a peer's want (e.g., peer sent a cancel, or we sent the block)
  WantlistEntry? removePeerWant(String peerId, CID cid) {
    final peerWants = _peerWantlists[peerId];
    if (peerWants != null) {
      final removed = peerWants.remove(cid.toString());
      if (removed != null) {
        print('Peer $peerId removed want for ${cid.toString()}');
      }
      if (peerWants.isEmpty) {
        _peerWantlists.remove(peerId);
      }
      return removed;
    }
    return null;
  }

  Map<String, WantlistEntry> getWantsForPeer(String peerId) {
    return Map.unmodifiable(_peerWantlists[peerId] ?? {});
  }

  // Clear all wants for a specific peer
  void clearPeerWantlist(String peerId) {
    if (_peerWantlists.containsKey(peerId)) {
      _peerWantlists.remove(peerId);
      print('Cleared all wants for peer $peerId.');
    }
  }

  // --- Notifications ---

  // Called when a block is received that we were wanting
  void blockReceived(CID cid, List<int> data) {
    if (isWantingLocally(cid)) {
      print('Block received that was in local wantlist: $cid');
      removeLocalWant(cid);
      // TODO: Notify other parts of the system (e.g., the original requester)
      // TODO: Send CANCEL messages to peers we asked for this block
    }
  }

  // Called when we receive a HAVE from a peer for a block we want
  void receivedHave(String peerId, CID cid) {
    if (isWantingLocally(cid)) {
      print('Peer $peerId HAS block we want: $cid');
      // TODO: Potentially prioritize fetching from this peer
    }
  }

  // Called when we receive a DONT_HAVE from a peer for a block we want
  void receivedDontHave(String peerId, CID cid) {
    if (isWantingLocally(cid)) {
      print('Peer $peerId DOES NOT HAVE block we want: $cid');
      // TODO: Deprioritize or stop asking this peer for this block
    }
  }
}
