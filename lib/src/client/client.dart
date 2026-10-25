import 'dart:async';
import 'dart:typed_data';

import 'package:convert/convert.dart';
import 'package:dart_libp2p/dart_libp2p.dart' as libp2p;
import 'package:dcid/dcid.dart' as dart_cid;
import 'package:logging/logging.dart';

import '../block_store.dart';
import '../cid_prefix.dart';
import '../network/network_service.dart';
import '../proto/bitswap_message.pb.dart' as pb;
import './local_wantlist.dart';

final _log = Logger('bitswap.client');

/// Called after a wanted block has been verified and stored.
typedef BlockStoredCallback = void Function(dart_cid.CID cid, Uint8List data);

/// Completes a cancelled want's waiters; [BitswapClient.getBlock] maps it to null.
class _WantCancelled implements Exception {
  const _WantCancelled();
}

/// The fetching half of Bitswap: maintains our wantlist, broadcasts it to
/// peers, and accepts the blocks and presences they send back.
class BitswapClient {
  final libp2p.Host _host;
  final BlockStore _blockStore;
  final BitswapNetworkService _network;
  final BitswapOptions _options;
  final BlockStoredCallback _onBlockStored;
  final LocalWantlist _wantlist = LocalWantlist();

  // Peers known to speak Bitswap because they sent us Bitswap messages
  final Set<libp2p.PeerId> _activeBitswapPeers = {};
  // Peers we've sent a full wantlist to (first message should have full=true)
  final Set<libp2p.PeerId> _peersWithFullWantlistSent = {};
  Timer? _rebroadcastTimer;

  BitswapClient(this._host, this._blockStore, this._network, this._options, this._onBlockStored);

  /// Starts periodic rebroadcast of outstanding wants.
  void start() {
    _rebroadcastTimer ??= Timer.periodic(_options.rebroadcastInterval, (_) => rebroadcast());
  }

  /// Adds [cids] to the wantlist and broadcasts them.
  Future<void> wantBlocks(List<dart_cid.CID> cids) async {
    _log.fine(() => 'wantBlocks called with ${cids.length} CIDs: ${cids.join(', ')}');
    await _broadcastWants(_addWants(cids));
  }

  /// Removes [cids] from the wantlist and sends CANCEL to the peers that were
  /// asked. Pending [getBlock] calls for them complete with null.
  void cancelWants(List<dart_cid.CID> cids) {
    for (final cid in cids) {
      final removed = _wantlist.cancel(cid, const _WantCancelled());
      if (removed != null) _sendCancels(cid, removed.peersSentTo);
    }
  }

  /// See `Bitswap.getBlock`.
  Future<Uint8List?> getBlock(dart_cid.CID cid, {Duration? timeout}) async {
    if (await _blockStore.has(cid)) return _blockStore.get(cid);

    // Register the want and our waiter before any await, so a block that
    // arrives while the wantlist is being broadcast is not missed.
    final newWants = _addWants([cid]);
    final received = _wantlist.addWaiter(cid);
    try {
      await _broadcastWants(newWants);
      return await received.timeout(timeout ?? _options.getBlockTimeout);
    } on TimeoutException {
      _log.fine('Timeout waiting for block $cid');
      return null;
    } on _WantCancelled {
      return null;
    } finally {
      final abandoned = _wantlist.releaseWaiter(cid);
      if (abandoned != null) _sendCancels(cid, abandoned.peersSentTo);
    }
  }

  /// Notes that [peerId] speaks Bitswap, so future wants are sent to it.
  void peerActive(libp2p.PeerId peerId) => _activeBitswapPeers.add(peerId);

  /// Sends our wantlist to a newly connected peer if it speaks Bitswap.
  /// Peers whose protocols are not known yet (identify still running) are
  /// picked up by the next [rebroadcast].
  Future<void> peerConnected(libp2p.PeerId peerId) async {
    if (_wantlist.isEmpty) return;
    final peerInfo = await _host.peerStore.getPeer(peerId);
    if (peerInfo == null || !peerInfo.protocols.contains(_network.protocolId)) return;
    _sendPlanned(_wantlist.all, [peerId]);
  }

  /// Forgets [peerId] and moves any WANT_BLOCK it held to a peer that said HAVE.
  void peerDisconnected(libp2p.PeerId peerId) {
    _activeBitswapPeers.remove(peerId);
    _peersWithFullWantlistSent.remove(peerId);
    for (final want in _wantlist.forgetPeer(peerId)) {
      _requestBlockFromHaveCandidate(want);
    }
  }

  /// Re-sends outstanding wants: to peers not asked yet, and as WANT_BLOCK to
  /// a new peer for any want with no outstanding WANT_BLOCK.
  Future<void> rebroadcast() async {
    if (_wantlist.isEmpty) return;
    _sendPlanned(_wantlist.all, await _bitswapPeers());
  }

  /// Fails all pending wants with [error] and forgets peer state.
  void stop(Object error) {
    _rebroadcastTimer?.cancel();
    _rebroadcastTimer = null;
    _wantlist.failAll(error);
    _activeBitswapPeers.clear();
    _peersWithFullWantlistSent.clear();
  }

  /// Verifies and stores a block from [peerId] if we want it.
  Future<void> handleBlock(libp2p.PeerId peerId, pb.Message_Block blockProto) async {
    final prefixBytes = Uint8List.fromList(blockProto.prefix);
    final dataBytes = Uint8List.fromList(blockProto.data);

    final dart_cid.CID cid;
    try {
      cid = cidFromPrefixAndData(prefixBytes, dataBytes);
    } on FormatException catch (e) {
      _log.warning(() => 'Rejecting block from $peerId: ${e.message}. Prefix: ${hex.encode(prefixBytes)}');
      return;
    }

    // Only accept blocks we asked for (as boxo does); storing unsolicited
    // blocks would let any peer fill our blockstore.
    if (!_wantlist.contains(cid)) {
      _log.fine('Discarding unwanted block $cid from $peerId.');
      return;
    }

    try {
      await _blockStore.put(cid, dataBytes);
      _log.fine('Stored wanted block $cid from $peerId.');

      _completeWant(cid, dataBytes);
      _onBlockStored(cid, dataBytes);
    } catch (e, s) {
      _log.warning('Error storing block $cid from $peerId', e, s);
    }
  }

  /// Handles a block the application stored itself: completes our want for
  /// it, if any, and passes it on to be served to peers.
  void blockAdded(dart_cid.CID cid, Uint8List data) {
    _completeWant(cid, data);
    _onBlockStored(cid, data);
  }

  /// Wakes [cid]'s waiters and sends CANCEL to the peers we asked.
  void _completeWant(dart_cid.CID cid, Uint8List data) {
    final peersAsked = _wantlist.peersSentTo(cid);
    _wantlist.blockReceived(cid, data);
    _sendCancels(cid, peersAsked);
  }

  /// Handles a HAVE / DONT_HAVE from [peerId].
  void handlePresence(libp2p.PeerId peerId, pb.Message_BlockPresence presence) {
    final dart_cid.CID cid;
    try {
      cid = dart_cid.CID.fromBytes(Uint8List.fromList(presence.cid));
    } catch (e) {
      _log.warning('Error parsing CID from block presence from $peerId: $e');
      return;
    }
    _log.fine('${presence.type} from $peerId for $cid');
    _activeBitswapPeers.add(peerId);

    final has = presence.type == pb.Message_BlockPresenceType.Have;
    final want = _wantlist.recordPresence(cid, peerId, has: has);
    if (want == null || want.blockPeer != null) return;
    if (has) {
      _sendEntries(peerId, [_entryFor(want, block: true)]);
      _wantlist.recordSentTo(cid, peerId, block: true);
    } else {
      // Our WANT_BLOCK peer doesn't have it; try one that said HAVE
      _requestBlockFromHaveCandidate(want);
    }
  }

  void _requestBlockFromHaveCandidate(LocalWant want) {
    final candidate = want.haveCandidates.firstOrNull;
    if (candidate == null) return; // Wait for a HAVE or the next rebroadcast
    _sendEntries(candidate, [_entryFor(want, block: true)]);
    _wantlist.recordSentTo(want.cid, candidate, block: true);
  }

  /// Adds [cids] to the wantlist (synchronously) and returns the wants that
  /// are new or changed and therefore need broadcasting.
  List<LocalWant> _addWants(List<dart_cid.CID> cids) => [
        for (final cid in cids)
          if (_wantlist.add(cid) case final want?) want,
      ];

  Future<void> _broadcastWants(List<LocalWant> wants) async {
    if (wants.isEmpty) return;
    final peers = await _bitswapPeers();
    if (peers.isEmpty) _log.fine('No known Bitswap peers; wants will go out on rebroadcast.');
    _sendPlanned(wants, peers);
  }

  /// Sends [wants] to [peers] following the Bitswap 1.2.0 pattern: each want
  /// keeps one outstanding WANT_BLOCK (preferring a peer that said HAVE) and
  /// goes as WANT_HAVE to every other peer. Peers we have not messaged before
  /// get our full wantlist with `full` set.
  void _sendPlanned(List<LocalWant> wants, List<libp2p.PeerId> peers) {
    final newPeers = [for (final p in peers) if (!_peersWithFullWantlistSent.contains(p)) p];
    final knownPeers = [for (final p in peers) if (_peersWithFullWantlistSent.contains(p)) p];

    final outgoing = <libp2p.PeerId, List<pb.Message_Wantlist_Entry>>{};
    void plan(List<LocalWant> wants, List<libp2p.PeerId> peers) {
      for (final want in wants) {
        if (!identical(_wantlist[want.cid], want)) continue; // Fulfilled or cancelled meanwhile
        if (want.blockPeer == null) {
          final target = want.haveCandidates.firstOrNull ?? peers.where((p) => !want.peers.containsKey(p)).firstOrNull;
          if (target != null) {
            (outgoing[target] ??= []).add(_entryFor(want, block: true));
            _wantlist.recordSentTo(want.cid, target, block: true);
          }
        }
        for (final peer in peers) {
          if (want.peers.containsKey(peer)) continue;
          (outgoing[peer] ??= []).add(_entryFor(want, block: false));
          _wantlist.recordSentTo(want.cid, peer);
        }
      }
    }

    plan(wants, knownPeers);
    plan(_wantlist.all, newPeers); // First contact: the full wantlist

    for (final MapEntry(key: peer, value: entries) in outgoing.entries) {
      final full = _peersWithFullWantlistSent.add(peer);
      _sendEntries(peer, entries, full: full);
    }
  }

  void _sendEntries(libp2p.PeerId peerId, List<pb.Message_Wantlist_Entry> entries, {bool full = false}) {
    _log.fine('Sending ${full ? 'full ' : ''}wantlist (${entries.length} entries) to $peerId');
    _network.sendMessage(peerId, pb.Message()
      ..wantlist = (pb.Message_Wantlist()
        ..entries.addAll(entries)
        ..full = full));
  }

  void _sendCancels(dart_cid.CID cid, Set<libp2p.PeerId> peers) {
    if (peers.isEmpty) return;
    final cancel = pb.Message()
      ..wantlist = (pb.Message_Wantlist()
        ..entries.add(pb.Message_Wantlist_Entry()
          ..block = cid.toBytes()
          ..cancel = true));
    _log.fine('Sending CANCEL for $cid to $peers');
    for (final peerId in peers) {
      _network.sendMessage(peerId, cancel);
    }
  }

  /// Peers to send wants to: those that have spoken Bitswap to us, plus
  /// connected peers whose identify info lists the Bitswap protocol.
  Future<List<libp2p.PeerId>> _bitswapPeers() async {
    final peers = {..._activeBitswapPeers};
    try {
      for (final conn in _host.network.conns) {
        final remotePeerId = conn.remotePeer;
        final peerInfo = await _host.peerStore.getPeer(remotePeerId);
        if (peerInfo != null && peerInfo.protocols.contains(_network.protocolId)) {
          peers.add(remotePeerId);
        }
      }
    } catch (e) {
      _log.warning('Error querying connected peers for Bitswap support: $e. Falling back to known active peers.');
    }
    if (peers.isEmpty) _log.warning('Found no Bitswap-supporting peers.');
    return peers.toList();
  }

  static pb.Message_Wantlist_Entry _entryFor(LocalWant want, {required bool block}) => pb.Message_Wantlist_Entry()
    ..block = want.cid.toBytes()
    ..wantType = block ? pb.Message_Wantlist_WantType.Block : pb.Message_Wantlist_WantType.Have
    ..priority = want.priority
    ..sendDontHave = true;
}
