import 'dart:async';
import 'dart:typed_data';

import 'package:dart_libp2p/dart_libp2p.dart' as libp2p;
import 'package:dart_libp2p_bitswap/src/bitswap.dart';
import 'package:dart_libp2p_bitswap/src/proto/bitswap_message.pb.dart' as pb;
import 'package:dcid/dcid.dart' as dart_cid;
import 'package:mockito/mockito.dart';
import 'package:test/test.dart';

import 'bitswap_test.mocks.dart';
import 'fake_swarm.dart';
import 'test_helpers.dart';

void main() {
  final blockData = Uint8List.fromList([5, 6, 7]);
  final cid = dart_cid.CID.fromData(dart_cid.CID.V1, 'raw', blockData);
  final peerA = testPeer(1), peerB = testPeer(2), peerC = testPeer(3);

  late FakeSwarm swarm;
  late MockBlockStore store;
  late Bitswap bitswap;

  pb.Message presence(pb.Message_BlockPresenceType type) => pb.Message()
    ..blockPresences.add(pb.Message_BlockPresence()
      ..cid = cid.toBytes()
      ..type = type);
  final have = presence(pb.Message_BlockPresenceType.Have);
  final dontHave = presence(pb.Message_BlockPresenceType.DontHave);

  libp2p.PeerId blockPeer() => [peerA, peerB, peerC].singleWhere((p) => swarm.sentWantBlock(p, cid));

  setUp(() async {
    swarm = FakeSwarm();
    store = MockBlockStore();
    when(store.has(any)).thenAnswer((_) async => false);
    when(store.put(any, any)).thenAnswer((_) async {});
    bitswap = Bitswap(
      host: swarm.host,
      blockStore: store,
      options: const BitswapOptions(rebroadcastInterval: Duration(milliseconds: 20)),
    );
    await bitswap.start();
  });

  tearDown(() async {
    await bitswap.stop();
    await swarm.events.close();
  });

  test('sends WANT_BLOCK to one peer and WANT_HAVE to the rest, all with sendDontHave', () async {
    [peerA, peerB, peerC].forEach(swarm.addBitswapPeer);

    await bitswap.wantBlocks([cid]);
    await pumpEventQueue();

    final types = {
      for (final p in [peerA, peerB, peerC]) p: swarm.entriesTo(p, cid).single.wantType,
    };
    expect(types.values.where((t) => t == pb.Message_Wantlist_WantType.Block), hasLength(1));
    expect(types.values.where((t) => t == pb.Message_Wantlist_WantType.Have), hasLength(2));
    for (final p in [peerA, peerB, peerC]) {
      expect(swarm.entriesTo(p, cid).single.sendDontHave, isTrue);
      expect(swarm.sent[p]!.first.wantlist.full, isTrue, reason: 'first contact sends the full wantlist');
    }
  });

  test('a HAVE while a WANT_BLOCK is outstanding does not trigger another WANT_BLOCK', () async {
    [peerA, peerB, peerC].forEach(swarm.addBitswapPeer);
    await bitswap.wantBlocks([cid]);
    await pumpEventQueue();
    final holder = blockPeer();
    final other = [peerA, peerB, peerC].firstWhere((p) => p != holder);

    await bitswap.handleIncomingMessage(other, have);
    await pumpEventQueue();

    expect(swarm.sentWantBlock(other, cid), isFalse);
  });

  test('DONT_HAVE from the WANT_BLOCK peer moves WANT_BLOCK to a peer that said HAVE', () async {
    [peerA, peerB, peerC].forEach(swarm.addBitswapPeer);
    await bitswap.wantBlocks([cid]);
    await pumpEventQueue();
    final holder = blockPeer();
    final haver = [peerA, peerB, peerC].firstWhere((p) => p != holder);

    await bitswap.handleIncomingMessage(haver, have);
    await bitswap.handleIncomingMessage(holder, dontHave);
    await pumpEventQueue();

    expect(swarm.sentWantBlock(haver, cid), isTrue);
  });

  test('HAVE after the WANT_BLOCK peer said DONT_HAVE gets WANT_BLOCK immediately', () async {
    [peerA, peerB].forEach(swarm.addBitswapPeer);
    await bitswap.wantBlocks([cid]);
    await pumpEventQueue();
    final holder = blockPeer();
    final other = holder == peerA ? peerB : peerA;

    await bitswap.handleIncomingMessage(holder, dontHave);
    await bitswap.handleIncomingMessage(other, have);
    await pumpEventQueue();

    expect(swarm.sentWantBlock(other, cid), isTrue);
  });

  test('disconnect of the WANT_BLOCK peer moves WANT_BLOCK to a peer that said HAVE', () async {
    [peerA, peerB].forEach(swarm.addBitswapPeer);
    await bitswap.wantBlocks([cid]);
    await pumpEventQueue();
    final holder = blockPeer();
    final other = holder == peerA ? peerB : peerA;
    await bitswap.handleIncomingMessage(other, have);

    swarm.connected.remove(holder);
    swarm.events.add(libp2p.EvtPeerConnectednessChanged(peer: holder, connectedness: libp2p.Connectedness.notConnected));
    await pumpEventQueue();

    expect(swarm.sentWantBlock(other, cid), isTrue);
  });

  test('wants made with no peers go out on rebroadcast once a peer appears', () async {
    await bitswap.wantBlocks([cid]);
    swarm.addBitswapPeer(peerA);

    await Future.delayed(const Duration(milliseconds: 60));

    expect(swarm.sentWantBlock(peerA, cid), isTrue);
    expect(swarm.sent[peerA]!.first.wantlist.full, isTrue);
  });

  test('rebroadcast re-targets WANT_BLOCK when every asked peer said DONT_HAVE', () async {
    swarm.addBitswapPeer(peerA);
    await bitswap.wantBlocks([cid]);
    await pumpEventQueue();
    await bitswap.handleIncomingMessage(peerA, dontHave);

    swarm.addBitswapPeer(peerB);
    await Future.delayed(const Duration(milliseconds: 60));

    expect(swarm.sentWantBlock(peerB, cid), isTrue);
    expect(swarm.entriesTo(peerA, cid), hasLength(1), reason: 'peers already asked are not re-sent');
  });

  test('newly connected peer gets the wantlist only if it speaks Bitswap', () async {
    await bitswap.wantBlocks([cid]); // No peers yet

    swarm.connected.addAll([peerA, peerB]);
    swarm.speaksBitswap.add(peerB);
    for (final p in [peerA, peerB]) {
      swarm.events.add(libp2p.EvtPeerConnectednessChanged(peer: p, connectedness: libp2p.Connectedness.connected));
    }
    await pumpEventQueue();

    expect(swarm.sent[peerA], isNull);
    expect(swarm.sentWantBlock(peerB, cid), isTrue);
  });

  test('later wants to an already-contacted peer are sent as a delta', () async {
    swarm.addBitswapPeer(peerA);
    await bitswap.wantBlocks([cid]);
    final cid2 = dart_cid.CID.fromData(dart_cid.CID.V1, 'raw', Uint8List.fromList([8]));
    await bitswap.wantBlocks([cid2]);
    await pumpEventQueue();

    expect(swarm.sent[peerA]!.map((m) => m.wantlist.full), [true, false]);
    expect(swarm.sent[peerA]!.last.wantlist.entries.single.block, cid2.toBytes());
  });

  test('block arrival CANCELs every peer that was asked', () async {
    [peerA, peerB, peerC].forEach(swarm.addBitswapPeer);
    final pending = bitswap.getBlock(cid);
    await pumpEventQueue();

    await bitswap.handleIncomingMessage(blockPeer(), pb.Message()
      ..payload.add(pb.Message_Block()
        ..prefix = [0x01, 0x55, 0x12, 0x20]
        ..data = blockData));
    expect(await pending, blockData);
    await pumpEventQueue();

    for (final p in [peerA, peerB, peerC]) {
      expect(swarm.entriesTo(p, cid).last.cancel, isTrue, reason: '$p should get CANCEL');
    }
  });

  group('notifyNewBlock', () {
    pb.Message wantFrom(pb.Message_Wantlist_WantType type) => pb.Message()
      ..wantlist = (pb.Message_Wantlist()
        ..entries.add(pb.Message_Wantlist_Entry()
          ..block = cid.toBytes()
          ..wantType = type
          ..sendDontHave = true));

    test('serves the block to a peer whose WANT_BLOCK we could not answer', () async {
      swarm.addBitswapPeer(peerA);
      await bitswap.handleIncomingMessage(peerA, wantFrom(pb.Message_Wantlist_WantType.Block));
      await pumpEventQueue();
      expect(swarm.sent[peerA]!.single.blockPresences.single.type, pb.Message_BlockPresenceType.DontHave);

      bitswap.notifyNewBlock(cid, blockData);
      await pumpEventQueue();

      expect(swarm.sent[peerA]!.last.payload.single.data, blockData);
    });

    test('sends HAVE to a peer that sent WANT_HAVE', () async {
      swarm.addBitswapPeer(peerA);
      await bitswap.handleIncomingMessage(peerA, wantFrom(pb.Message_Wantlist_WantType.Have));

      bitswap.notifyNewBlock(cid, blockData);
      await pumpEventQueue();

      final last = swarm.sent[peerA]!.last;
      expect(last.payload, isEmpty);
      expect(last.blockPresences.single.type, pb.Message_BlockPresenceType.Have);
    });

    test('completes our own pending getBlock and CANCELs the peers asked', () async {
      [peerA, peerB].forEach(swarm.addBitswapPeer);
      final pending = bitswap.getBlock(cid);
      await pumpEventQueue();

      bitswap.notifyNewBlock(cid, blockData);
      expect(await pending, blockData);
      await pumpEventQueue();

      for (final p in [peerA, peerB]) {
        expect(swarm.entriesTo(p, cid).last.cancel, isTrue, reason: '$p should get CANCEL');
      }
    });

    test('throws once stopped', () async {
      await bitswap.stop();
      expect(() => bitswap.notifyNewBlock(cid, blockData), throwsStateError);
    });
  });
}
