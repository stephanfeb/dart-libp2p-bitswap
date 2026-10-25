import 'dart:async';
import 'dart:collection';
import 'dart:typed_data';

import 'package:dart_libp2p/dart_libp2p.dart' as libp2p;
import 'package:dcid/dcid.dart' as dart_cid;
import 'package:logging/logging.dart';

import '../block_store.dart';
import '../cid_prefix.dart';
import '../network/network_service.dart';
import '../proto/bitswap_message.pb.dart' as pb;
import './message_packing.dart';
import './peer_ledger.dart';

final _log = Logger('bitswap.server');

/// The serving half of Bitswap: answers peers' wantlists from the
/// [BlockStore] and pushes blocks to peers waiting for them.
///
/// Incoming wants are only recorded and queued; a single serve loop answers
/// them off the network read path. Peers take turns, each served up to
/// [BitswapOptions.serveBytesPerTurn] of blocks per turn, highest-priority
/// wants first.
class BitswapServer {
  final BlockStore _blockStore;
  final BitswapNetworkService _network;
  final int _serveBytesPerTurn;
  final PeerLedger _ledger;

  // Per peer: wants awaiting a reply, by CID string
  final Map<libp2p.PeerId, Map<String, _Reply>> _pending = {};
  // Peers with pending wants, in serving order
  final LinkedHashSet<libp2p.PeerId> _turnOrder = LinkedHashSet();
  bool _serving = false;
  bool _stopped = false;

  BitswapServer(this._blockStore, this._network, BitswapOptions options)
      : _serveBytesPerTurn = options.serveBytesPerTurn,
        _ledger = PeerLedger(maxWantsPerPeer: options.maxWantsPerPeer);

  /// Records [peerId]'s wants and cancels, and queues replies to the wants.
  /// Wants we cannot answer yet stay in the ledger for [blockAvailable].
  void handleWantlist(libp2p.PeerId peerId, pb.Message_Wantlist wantlist) {
    if (_stopped) return;
    _log.fine('Wantlist from $peerId. Entries: ${wantlist.entries.length}, Full: ${wantlist.full}');

    // Queued replies to wants dropped here are skipped when their turn comes
    if (wantlist.full) _ledger.clearPeer(peerId);

    var ignored = 0;
    for (final entry in wantlist.entries) {
      dart_cid.CID cid;
      try {
        cid = dart_cid.CID.fromBytes(Uint8List.fromList(entry.block));
      } catch (e) {
        _log.warning('Error parsing CID from wantlist entry from $peerId: $e');
        continue;
      }

      if (entry.cancel) {
        _log.fine('Peer $peerId cancelled want for CID $cid');
        _ledger.removeWant(peerId, cid);
        continue;
      }

      final added = _ledger.addWant(
        peerId,
        cid,
        priority: entry.priority,
        wantType: entry.wantType,
        sendDontHave: entry.sendDontHave,
      );
      if (added) {
        _enqueue(peerId, cid);
      } else {
        ignored++;
      }
    }
    if (ignored > 0) {
      _log.fine('Ignored $ignored wants from $peerId: over the limit of ${_ledger.maxWantsPerPeer}');
    }
  }

  /// Push-on-store: queues [cid] for every peer waiting for it. Peers that
  /// sent WANT_HAVE get a HAVE presence; peers that sent WANT_BLOCK get the
  /// block.
  void blockAvailable(dart_cid.CID cid, Uint8List data) {
    if (_stopped) return;
    for (final peerId in _ledger.peersWanting(cid).keys) {
      _enqueue(peerId, cid, data);
    }
  }

  void peerDisconnected(libp2p.PeerId peerId) {
    _ledger.clearPeer(peerId);
    _pending.remove(peerId);
    _turnOrder.remove(peerId);
  }

  /// Drops all queued work; an in-progress turn sends nothing further.
  void stop() {
    _stopped = true;
    _pending.clear();
    _turnOrder.clear();
  }

  void _enqueue(libp2p.PeerId peerId, dart_cid.CID cid, [Uint8List? data]) {
    final replies = _pending[peerId] ??= {};
    final key = cid.toString();
    replies[key] = _Reply(cid, data ?? replies[key]?.data);
    _turnOrder.add(peerId);
    if (!_serving) {
      _serving = true;
      // Start after the caller has queued the rest of its wantlist, so the
      // first turn sees every want and can order them by priority
      scheduleMicrotask(_serve);
    }
  }

  Future<void> _serve() async {
    try {
      while (!_stopped && _turnOrder.isNotEmpty) {
        final peerId = _turnOrder.first;
        _turnOrder.remove(peerId);
        await _serveTurn(peerId);
        if (_pending[peerId]?.isNotEmpty ?? false) {
          _turnOrder.add(peerId); // More to send; back of the line
        } else if (!_turnOrder.contains(peerId)) {
          _pending.remove(peerId);
        }
      }
    } finally {
      _serving = false;
    }
  }

  /// Answers [peerId]'s pending wants, highest priority first, until
  /// [_serveBytesPerTurn] bytes of blocks are collected.
  Future<void> _serveTurn(libp2p.PeerId peerId) async {
    final pending = _pending[peerId];
    if (pending == null) return;
    pending.removeWhere((_, reply) => _ledger.want(peerId, reply.cid) == null);
    final replies = pending.values.toList()
      ..sort((a, b) => _ledger.want(peerId, b.cid)!.priority.compareTo(_ledger.want(peerId, a.cid)!.priority));

    final presences = <pb.Message_BlockPresence>[];
    final blocks = <pb.Message_Block>[];
    var blockBytes = 0;
    for (final reply in replies) {
      if (blockBytes >= _serveBytesPerTurn) break;
      pending.remove(reply.cid.toString());
      try {
        blockBytes += await _answer(peerId, reply, presences, blocks);
      } catch (e, s) {
        _log.warning('Error serving ${reply.cid} to $peerId', e, s);
      }
      if (_stopped) return;
    }

    final responses = packResponse(presences, blocks);
    if (responses.isNotEmpty) {
      _log.fine('Sending response to $peerId (${blocks.length} blocks, ${presences.length} presences) in ${responses.length} message(s)');
    }
    for (final message in responses) {
      _network.sendMessage(peerId, message);
    }
  }

  /// Adds [reply] to [peerId]'s want to the response and returns the block
  /// bytes added. The ledger is re-read after each blockstore call, so a want
  /// cancelled meanwhile is not answered.
  Future<int> _answer(
    libp2p.PeerId peerId,
    _Reply reply,
    List<pb.Message_BlockPresence> presences,
    List<pb.Message_Block> blocks,
  ) async {
    final cid = reply.cid;
    final hasBlock = reply.data != null || await _blockStore.has(cid);
    final want = _ledger.want(peerId, cid);
    if (want == null) return 0;

    if (!hasBlock) {
      // The want stays in the ledger so blockAvailable can serve it later
      if (want.sendDontHave) presences.add(_presence(cid, pb.Message_BlockPresenceType.DontHave));
      return 0;
    }

    if (want.wantType == pb.Message_Wantlist_WantType.Have) {
      presences.add(_presence(cid, pb.Message_BlockPresenceType.Have));
      _ledger.removeWant(peerId, cid);
      return 0;
    }

    final blockData = reply.data ?? await _blockStore.get(cid);
    if (blockData == null || _ledger.want(peerId, cid) == null) return 0;
    blocks.add(pb.Message_Block()
      ..prefix = encodeCidPrefix(cid)
      ..data = blockData);
    _ledger.removeWant(peerId, cid);
    return blockData.length;
  }

  pb.Message_BlockPresence _presence(dart_cid.CID cid, pb.Message_BlockPresenceType type) =>
      pb.Message_BlockPresence()
        ..cid = cid.toBytes()
        ..type = type;
}

/// A queued reply to one want; [data] is set when the block is already in
/// hand (push-on-store), saving a blockstore read.
class _Reply {
  final dart_cid.CID cid;
  final Uint8List? data;

  const _Reply(this.cid, this.data);
}
