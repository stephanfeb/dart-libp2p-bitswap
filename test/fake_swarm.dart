import 'dart:async';
import 'dart:typed_data';

import 'package:dart_libp2p/dart_libp2p.dart' as libp2p;
import 'package:dart_libp2p_bitswap/src/proto/bitswap_message.pb.dart' as pb;
import 'package:dcid/dcid.dart' as dart_cid;
import 'package:mockito/mockito.dart';

import 'bitswap_test.mocks.dart';
import 'test_helpers.dart';

const bitswapProtocolId = '/ipfs/bitswap/1.2.0';

/// A fake swarm: peers we are connected to, which ones speak Bitswap, and
/// every message Bitswap sends to each of them.
class FakeSwarm {
  final host = MockHost();
  final network = MockNetwork();
  final peerStore = MockPeerstore();
  final events = StreamController<libp2p.EvtPeerConnectednessChanged>.broadcast();
  final List<libp2p.PeerId> connected = [];
  final Set<libp2p.PeerId> speaksBitswap = {};
  final Map<libp2p.PeerId, List<pb.Message>> sent = {};
  // Every message sent, across peers, in send order
  final List<(libp2p.PeerId, pb.Message)> log = [];

  FakeSwarm() {
    final hostId = MockPeerId();
    when(hostId.toBase58()).thenReturn('QmHost');
    when(host.id).thenReturn(hostId);
    when(host.network).thenReturn(network);
    when(host.peerStore).thenReturn(peerStore);
    final bus = MockEventBus();
    when(host.eventBus).thenReturn(bus);
    stubPeerEvents(bus, events.stream);

    when(network.conns).thenAnswer((_) => [
          for (final peer in connected) _conn(peer),
        ]);
    when(peerStore.getPeer(any)).thenAnswer((inv) async {
      final peer = inv.positionalArguments[0] as libp2p.PeerId;
      final info = MockPeerInfo();
      when(info.protocols).thenReturn(speaksBitswap.contains(peer) ? {bitswapProtocolId} : <String>{});
      return info;
    });
    when(host.newStream(any, any, any)).thenAnswer((inv) async {
      final peer = inv.positionalArguments[0] as libp2p.PeerId;
      final stream = MockP2PStream();
      when(stream.id()).thenReturn('to-$peer');
      when(stream.close()).thenAnswer((_) async {});
      when(stream.write(any)).thenAnswer((w) async {
        final frame = w.positionalArguments[0] as Uint8List;
        final prefix = dart_cid.decodeVarint(frame);
        final message = pb.Message.fromBuffer(frame.sublist(prefix.value));
        (sent[peer] ??= []).add(message);
        log.add((peer, message));
      });
      return stream;
    });
  }

  libp2p.Conn _conn(libp2p.PeerId peer) {
    final conn = MockConn();
    when(conn.remotePeer).thenReturn(peer);
    return conn;
  }

  void addBitswapPeer(libp2p.PeerId peer) {
    connected.add(peer);
    speaksBitswap.add(peer);
  }

  /// Wantlist entries sent to [peer] for [cid], oldest first.
  List<pb.Message_Wantlist_Entry> entriesTo(libp2p.PeerId peer, dart_cid.CID cid) => [
        for (final m in sent[peer] ?? const <pb.Message>[])
          for (final e in m.wantlist.entries)
            if (_sameBytes(e.block, cid.toBytes())) e,
      ];

  bool sentWantBlock(libp2p.PeerId peer, dart_cid.CID cid) =>
      entriesTo(peer, cid).any((e) => !e.cancel && e.wantType == pb.Message_Wantlist_WantType.Block);

  static bool _sameBytes(List<int> a, List<int> b) =>
      a.length == b.length && Iterable<int>.generate(a.length).every((i) => a[i] == b[i]);
}
