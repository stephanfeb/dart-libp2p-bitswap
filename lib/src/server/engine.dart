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
class BitswapServer {
  final BlockStore _blockStore;
  final BitswapNetworkService _network;
  final PeerLedger _ledger = PeerLedger();

  BitswapServer(this._blockStore, this._network);

  /// Records [peerId]'s wants and replies with the blocks and presences we
  /// can provide right away; the rest are served by [blockAvailable].
  Future<void> handleWantlist(libp2p.PeerId peerId, pb.Message_Wantlist wantlist) async {
    _log.fine('Wantlist from $peerId. Entries: ${wantlist.entries.length}, Full: ${wantlist.full}');

    if (wantlist.full) _ledger.clearPeer(peerId);

    // Collect all responses, then pack them into size-limited messages
    final responseBlocks = <pb.Message_Block>[];
    final responsePresences = <pb.Message_BlockPresence>[];

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

      _ledger.addWant(
        peerId,
        cid,
        priority: entry.priority,
        wantType: entry.wantType,
        sendDontHave: entry.sendDontHave,
      );

      final hasBlock = await _blockStore.has(cid);
      if (!hasBlock) {
        // The want stays in the ledger so blockAvailable can serve it later
        if (entry.sendDontHave) responsePresences.add(_presence(cid, pb.Message_BlockPresenceType.DontHave));
        continue;
      }

      if (entry.wantType == pb.Message_Wantlist_WantType.Have) {
        responsePresences.add(_presence(cid, pb.Message_BlockPresenceType.Have));
        _ledger.removeWant(peerId, cid);
        continue;
      }

      final blockData = await _blockStore.get(cid);
      if (blockData != null) {
        responseBlocks.add(pb.Message_Block()
          ..prefix = encodeCidPrefix(cid)
          ..data = blockData);
        _ledger.removeWant(peerId, cid);
      }
    }

    final responses = packResponse(responsePresences, responseBlocks);
    if (responses.isNotEmpty) {
      _log.fine('Sending response to $peerId (${responseBlocks.length} blocks, ${responsePresences.length} presences) in ${responses.length} message(s)');
    }
    for (final message in responses) {
      _network.sendMessage(peerId, message);
    }
  }

  /// Push-on-store: serves [cid] to every peer waiting for it. Peers that sent
  /// WANT_HAVE get a HAVE presence; peers that sent WANT_BLOCK get the block.
  void blockAvailable(dart_cid.CID cid, Uint8List data) {
    final peersWanting = _ledger.peersWanting(cid);
    if (peersWanting.isEmpty) return;
    _log.fine('${peersWanting.length} peers want block $cid. Sending.');

    final blockPayload = pb.Message_Block()
      ..prefix = encodeCidPrefix(cid)
      ..data = data;

    for (final MapEntry(key: peerId, value: want) in peersWanting.entries) {
      final message = pb.Message();
      if (want.wantType == pb.Message_Wantlist_WantType.Have) {
        message.blockPresences.add(_presence(cid, pb.Message_BlockPresenceType.Have));
      } else {
        message.payload.add(blockPayload);
      }
      _network.sendMessage(peerId, message);
      _ledger.removeWant(peerId, cid);
    }
  }

  void peerDisconnected(libp2p.PeerId peerId) => _ledger.clearPeer(peerId);

  pb.Message_BlockPresence _presence(dart_cid.CID cid, pb.Message_BlockPresenceType type) =>
      pb.Message_BlockPresence()
        ..cid = cid.toBytes()
        ..type = type;
}
