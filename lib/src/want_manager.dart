import 'dart:async'; // For Completer
import 'dart:typed_data'; // For Uint8List

import './proto/bitswap_message.pb.dart' as pb;
import 'package:dart_cid/dart_cid.dart'; 

// Represents an entry in a wantlist
class WantlistEntry {
  final CID cid; 
  final int priority;
  final pb.Message_Wantlist_WantType wantType;
  final bool sendDontHave;
  final Set<String> peersSentTo; // Peers to whom this want has been sent
  final Completer<Uint8List> completer; // For notifying when block is received

  WantlistEntry({
    required this.cid, 
    this.priority = 1,
    this.wantType = pb.Message_Wantlist_WantType.Block,
    this.sendDontHave = false,
  }) : peersSentTo = {}, completer = Completer<Uint8List>();

  @override
  String toString() {
    return 'WantlistEntry(cid: $cid, priority: $priority, type: $wantType, sendDontHave: $sendDontHave, peersSentTo: ${peersSentTo.length}, hasCompleter: ${completer != null})';
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
  // Returns the entry. If it already existed with the same wantType, returns the existing entry.
  WantlistEntry addLocalWant(CID cid, { 
    int priority = 1,
    pb.Message_Wantlist_WantType wantType = pb.Message_Wantlist_WantType.Block,
    bool sendDontHave = false,
  }) {
    final cidStr = cid.toString();
    if (_localWantlist.containsKey(cidStr) && _localWantlist[cidStr]!.wantType == wantType) {
      // Already wanting with same type, return existing entry
      print('Local want for $cidStr already exists with same type. Returning existing entry.');
      return _localWantlist[cidStr]!;
    }
    // If it exists but with a different wantType, or doesn't exist, create/overwrite.
    final entry = WantlistEntry(
      cid: cid, 
      priority: priority,
      wantType: wantType,
      sendDontHave: sendDontHave,
    );
    _localWantlist[cidStr] = entry;
    print('Added/Updated local wantlist for: $cidStr');
    return entry;
  }

  Future<Uint8List> whenBlockReceived(CID cid) {
    final cidStr = cid.toString();
    final entry = _localWantlist[cidStr];
    if (entry != null) {
      return entry.completer.future;
    }
    // If not currently wanting, return a future that will complete with an error.
    // This indicates a programming error if getBlock is called without wantBlocks ensuring the want exists.
    return Future.error(StateError('Block $cidStr was not in the local wantlist when whenBlockReceived was called.'));
  }

  // Call this when a local want is sent to a specific peer
  void recordWantSentToPeer(CID cid, String peerId) { 
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
    final cidStr = cid.toString();
    final entry = _localWantlist[cidStr];
    if (entry != null) {
      print('Block received that was in local wantlist: $cid');
      if (!entry.completer.isCompleted) {
        entry.completer.complete(Uint8List.fromList(data));
      }
      // It's important to remove the want *after* completing the future,
      // to avoid a race condition where whenBlockReceived might be called after removal but before completion.
      removeLocalWant(cid); 
      // TODO: Send CANCEL messages to peers we asked for this block (Bitswap class should handle this after block is stored)
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
