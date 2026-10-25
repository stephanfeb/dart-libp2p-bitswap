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

  BitswapClient(this._host, this._blockStore, this._network, this._options, this._onBlockStored);

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

  /// Sends our full wantlist to a newly connected peer.
  void peerConnected(libp2p.PeerId peerId) {
    final wants = _wantlist.all;
    if (wants.isNotEmpty) _sendFullWantlist(peerId, wants);
  }

  void peerDisconnected(libp2p.PeerId peerId) {
    _activeBitswapPeers.remove(peerId);
    _peersWithFullWantlistSent.remove(peerId);
  }

  /// Fails all pending wants with [error] and forgets peer state.
  void stop(Object error) {
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

      final peersAsked = _wantlist.peersSentTo(cid);
      _wantlist.blockReceived(cid, dataBytes);
      _sendCancels(cid, peersAsked);
      _onBlockStored(cid, dataBytes);
    } catch (e, s) {
      _log.warning('Error storing block $cid from $peerId', e, s);
    }
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

    if (presence.type != pb.Message_BlockPresenceType.Have) return;
    // If we still want this block and haven't asked this peer, ask it directly
    final want = _wantlist[cid];
    if (want == null || want.peersSentTo.contains(peerId)) return;
    final message = pb.Message()
      ..wantlist = (pb.Message_Wantlist()
        ..entries.add(pb.Message_Wantlist_Entry()
          ..block = cid.toBytes()
          ..wantType = pb.Message_Wantlist_WantType.Block
          ..priority = want.priority
          ..sendDontHave = true));
    _network.sendMessage(peerId, message);
    _wantlist.recordSentTo(cid, peerId);
  }

  /// Adds [cids] to the wantlist (synchronously) and returns the wants that
  /// are new or changed and therefore need broadcasting.
  List<LocalWant> _addWants(List<dart_cid.CID> cids) => [
        for (final cid in cids)
          if (_wantlist.add(cid) case final want?) want,
      ];

  Future<void> _broadcastWants(List<LocalWant> wants) async {
    if (wants.isEmpty) return;
    final delta = pb.Message()..wantlist = (pb.Message_Wantlist()..entries.addAll(wants.map(_entryFor)));

    final peers = await _bitswapPeers();
    if (peers.isEmpty) _log.fine('No known Bitswap peers to send wantlist to.');

    for (final peerId in peers) {
      if (!_peersWithFullWantlistSent.contains(peerId)) {
        // First message to this peer: send full wantlist with all local wants
        _sendFullWantlist(peerId, _wantlist.all);
      } else {
        _network.sendMessage(peerId, delta);
        for (final want in wants) {
          _wantlist.recordSentTo(want.cid, peerId);
        }
      }
    }
  }

  void _sendFullWantlist(libp2p.PeerId peerId, List<LocalWant> wants) {
    final message = pb.Message()
      ..wantlist = (pb.Message_Wantlist()
        ..entries.addAll(wants.map(_entryFor))
        ..full = true);
    _log.fine('Sending full wantlist (${wants.length} entries) to $peerId');
    _network.sendMessage(peerId, message);
    _peersWithFullWantlistSent.add(peerId);
    for (final want in wants) {
      _wantlist.recordSentTo(want.cid, peerId);
    }
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

  static pb.Message_Wantlist_Entry _entryFor(LocalWant want) => pb.Message_Wantlist_Entry()
    ..block = want.cid.toBytes()
    ..wantType = want.wantType
    ..priority = want.priority
    ..sendDontHave = want.sendDontHave;
}
