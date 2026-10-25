import 'dart:async';
import 'dart:typed_data';

import 'package:dart_libp2p/dart_libp2p.dart' as libp2p;
import 'package:dart_libp2p_bitswap/src/bitswap.dart';
import 'package:dart_libp2p_bitswap/src/proto/bitswap_message.pb.dart' as pb;
import 'package:dart_libp2p_bitswap/src/server/peer_ledger.dart';
import 'package:dcid/dcid.dart' as dart_cid;
import 'package:mockito/mockito.dart';
import 'package:test/test.dart';

import 'bitswap_test.mocks.dart';
import 'fake_swarm.dart';
import 'test_helpers.dart';

void main() {
  final peerA = testPeer(1), peerB = testPeer(2);
  final blocks = {
    for (var i = 0; i < 4; i++) dart_cid.CID.fromData(dart_cid.CID.V1, 'raw', Uint8List.fromList([i])): Uint8List.fromList([i]),
  };
  final cids = blocks.keys.toList();

  late FakeSwarm swarm;
  late MockBlockStore store;
  late Bitswap bitswap;
  var started = false;

  Future<void> startWith(BitswapOptions options) async {
    bitswap = Bitswap(host: swarm.host, blockStore: store, options: options);
    await bitswap.start();
    started = true;
  }

  setUp(() {
    started = false;
    swarm = FakeSwarm();
    store = MockBlockStore();
    when(store.has(any)).thenAnswer((inv) async => blocks.containsKey(inv.positionalArguments[0]));
    when(store.get(any)).thenAnswer((inv) async => blocks[inv.positionalArguments[0]]);
  });

  tearDown(() async {
    if (started) await bitswap.stop();
    await swarm.events.close();
  });

  pb.Message wantBlocks(List<dart_cid.CID> cids, {List<int>? priorities, bool full = false}) => pb.Message()
    ..wantlist = (pb.Message_Wantlist()
      ..full = full
      ..entries.addAll([
        for (var i = 0; i < cids.length; i++)
          pb.Message_Wantlist_Entry()
            ..block = cids[i].toBytes()
            ..priority = priorities?[i] ?? 1
            ..wantType = pb.Message_Wantlist_WantType.Block
            ..sendDontHave = true,
      ]));

  pb.Message cancel(dart_cid.CID cid) => pb.Message()
    ..wantlist = (pb.Message_Wantlist()
      ..entries.add(pb.Message_Wantlist_Entry()
        ..block = cid.toBytes()
        ..cancel = true));

  /// Block bytes in send order, as (peer, first data byte).
  List<(libp2p.PeerId, int)> blocksSent() => [
        for (final (peer, message) in swarm.log)
          for (final block in message.payload) (peer, block.data.first),
      ];

  group('PeerLedger limit', () {
    test('rejects new CIDs past maxWantsPerPeer but allows updating existing ones', () {
      final ledger = PeerLedger(maxWantsPerPeer: 2);
      expect(ledger.addWant(peerA, cids[0]), isTrue);
      expect(ledger.addWant(peerA, cids[1]), isTrue);
      expect(ledger.addWant(peerA, cids[2]), isFalse);
      expect(ledger.addWant(peerA, cids[0], priority: 9), isTrue);
      expect(ledger.want(peerA, cids[0])!.priority, 9);
      expect(ledger.wantsFor(peerA), hasLength(2));
      expect(ledger.addWant(peerB, cids[2]), isTrue, reason: 'the limit is per peer');
    });
  });

  test('wants past maxWantsPerPeer are ignored', () async {
    await startWith(const BitswapOptions(maxWantsPerPeer: 2));
    final missing = [
      for (var i = 10; i < 13; i++) dart_cid.CID.fromData(dart_cid.CID.V1, 'raw', Uint8List.fromList([i])),
    ];

    await bitswap.handleIncomingMessage(peerA, wantBlocks(missing));
    await pumpEventQueue();

    final dontHaves = [for (final m in swarm.sent[peerA]!) ...m.blockPresences];
    expect(dontHaves, hasLength(2));
    verifyNever(store.has(missing[2]));
  });

  test('serves higher-priority wants first', () async {
    await startWith(const BitswapOptions(serveBytesPerTurn: 1));

    await bitswap.handleIncomingMessage(peerA, wantBlocks(cids.sublist(0, 3), priorities: [1, 5, 3]));
    await pumpEventQueue();

    expect(blocksSent(), [(peerA, 1), (peerA, 2), (peerA, 0)]);
  });

  test('peers take turns, so a long wantlist does not starve others', () async {
    await startWith(const BitswapOptions(serveBytesPerTurn: 1));

    await bitswap.handleIncomingMessage(peerA, wantBlocks(cids.sublist(0, 3)));
    await bitswap.handleIncomingMessage(peerB, wantBlocks([cids[3]]));
    await pumpEventQueue();

    final order = blocksSent().map((s) => s.$1).toList();
    expect(order, hasLength(4));
    expect(order.indexOf(peerB), lessThan(2), reason: 'B is served on the second turn, not after all of A');
  });

  test('handling a wantlist does not wait on the blockstore', () async {
    final gate = Completer<bool>();
    when(store.has(any)).thenAnswer((_) => gate.future);
    await startWith(const BitswapOptions());

    await bitswap
        .handleIncomingMessage(peerA, wantBlocks([cids[0]]))
        .timeout(const Duration(seconds: 1));

    gate.complete(true);
    await pumpEventQueue();
    expect(blocksSent(), [(peerA, 0)]);
  });

  test('a CANCEL that arrives while the block is being read stops it being sent', () async {
    final gate = Completer<Uint8List?>();
    when(store.get(any)).thenAnswer((_) => gate.future);
    await startWith(const BitswapOptions());

    await bitswap.handleIncomingMessage(peerA, wantBlocks([cids[0]]));
    await pumpEventQueue();
    await bitswap.handleIncomingMessage(peerA, cancel(cids[0]));
    gate.complete(blocks[cids[0]]);
    await pumpEventQueue();

    expect(blocksSent(), isEmpty);
  });

  test('a full wantlist drops queued replies to wants it no longer lists', () async {
    final gate = Completer<bool>();
    var first = true;
    when(store.has(any)).thenAnswer((inv) {
      if (first) {
        first = false;
        return gate.future; // Hold the serve loop on the first want
      }
      return Future.value(blocks.containsKey(inv.positionalArguments[0]));
    });
    await startWith(const BitswapOptions(serveBytesPerTurn: 1));

    await bitswap.handleIncomingMessage(peerA, wantBlocks([cids[0]], priorities: [9]));
    await bitswap.handleIncomingMessage(peerB, wantBlocks([cids[1]]));
    await pumpEventQueue();
    await bitswap.handleIncomingMessage(peerB, wantBlocks([cids[2]], full: true));
    gate.complete(true);
    await pumpEventQueue();

    expect(blocksSent(), [(peerA, 0), (peerB, 2)]);
  });

  test('a disconnected peer is not served', () async {
    final gate = Completer<bool>();
    when(store.has(any)).thenAnswer((_) => gate.future);
    await startWith(const BitswapOptions(serveBytesPerTurn: 1));

    await bitswap.handleIncomingMessage(peerA, wantBlocks([cids[0]]));
    await bitswap.handleIncomingMessage(peerB, wantBlocks([cids[1]]));
    await pumpEventQueue();
    bitswap.onPeerDisconnected(peerB);
    gate.complete(true);
    await pumpEventQueue();

    expect(blocksSent(), [(peerA, 0)]);
  });
}
