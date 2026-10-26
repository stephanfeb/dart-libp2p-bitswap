import 'dart:typed_data';

import 'package:dart_libp2p_bitswap/src/bitswap.dart';
import 'package:dart_libp2p_bitswap/src/cid_prefix.dart';
import 'package:dart_libp2p_bitswap/src/proto/bitswap_message.pb.dart' as pb;
import 'package:dcid/dcid.dart' as dart_cid;
import 'package:mockito/mockito.dart';
import 'package:test/test.dart';

import 'bitswap_test.mocks.dart';
import 'fake_swarm.dart';
import 'test_helpers.dart';

/// Malformed or hostile input from peers, and failing blockstores. None of
/// it may crash Bitswap, corrupt its state, or get unverified data stored.
void main() {
  final blockData = Uint8List.fromList([5, 6, 7]);
  final cid = dart_cid.CID.fromData(dart_cid.CID.V1, 'raw', blockData);
  final rawPrefix = [0x01, 0x55, 0x12, 0x20];
  final garbage = [0xff, 0x00, 0x13];
  final peerA = testPeer(1), peerB = testPeer(2);

  late FakeSwarm swarm;
  late MockBlockStore store;
  late Bitswap bitswap;

  setUp(() async {
    swarm = FakeSwarm();
    store = MockBlockStore();
    when(store.has(any)).thenAnswer((_) async => false);
    when(store.put(any, any)).thenAnswer((_) async {});
    bitswap = Bitswap(
      host: swarm.host,
      blockStore: store,
      options: const BitswapOptions(rebroadcastInterval: Duration(hours: 1)),
    );
    await bitswap.start();
  });

  tearDown(() async {
    await bitswap.stop();
    await swarm.events.close();
  });

  pb.Message_Wantlist_Entry wantEntry(List<int> cidBytes, {bool cancel = false}) => pb.Message_Wantlist_Entry()
    ..block = cidBytes
    ..wantType = pb.Message_Wantlist_WantType.Block
    ..sendDontHave = true
    ..cancel = cancel;

  pb.Message wantlist(List<pb.Message_Wantlist_Entry> entries) =>
      pb.Message()..wantlist = (pb.Message_Wantlist()..entries.addAll(entries));

  pb.Message blockMsg(List<int> prefix, List<int> data) => pb.Message()
    ..payload.add(pb.Message_Block()
      ..prefix = prefix
      ..data = data);

  pb.Message presence(List<int> cidBytes, pb.Message_BlockPresenceType type) => pb.Message()
    ..blockPresences.add(pb.Message_BlockPresence()
      ..cid = cidBytes
      ..type = type);

  void serveBlock() {
    when(store.has(cid)).thenAnswer((_) async => true);
    when(store.get(cid)).thenAnswer((_) async => blockData);
  }

  List<pb.Message_Block> blocksTo(peer) => [for (final m in swarm.sent[peer] ?? <pb.Message>[]) ...m.payload];

  group('wantlists', () {
    test('an entry with a garbage CID is skipped and the rest are answered', () async {
      serveBlock();
      await bitswap.handleIncomingMessage(peerA, wantlist([wantEntry(garbage), wantEntry(cid.toBytes())]));
      await pumpEventQueue();

      expect(blocksTo(peerA).single.data, blockData);
    });

    test('the same CID listed twice is answered once', () async {
      serveBlock();
      await bitswap.handleIncomingMessage(peerA, wantlist([wantEntry(cid.toBytes()), wantEntry(cid.toBytes())]));
      await pumpEventQueue();

      expect(blocksTo(peerA), hasLength(1));
    });

    test('a CANCEL for something never wanted is a silent no-op', () async {
      await bitswap.handleIncomingMessage(peerA, wantlist([wantEntry(cid.toBytes(), cancel: true)]));
      await pumpEventQueue();

      expect(swarm.sent[peerA], isNull);
      verifyNever(store.has(any));
    });

    test('a want and its CANCEL in one message leave nothing to serve', () async {
      serveBlock();
      await bitswap.handleIncomingMessage(peerA, wantlist([wantEntry(cid.toBytes()), wantEntry(cid.toBytes(), cancel: true)]));
      await pumpEventQueue();

      expect(swarm.sent[peerA], isNull);
    });
  });

  group('blocks', () {
    test('a block whose data does not match the CID we want is not stored', () async {
      final pending = bitswap.getBlock(cid, timeout: const Duration(hours: 1));
      await pumpEventQueue();

      // Same prefix, different bytes: hashes to another CID, which we never wanted
      await bitswap.handleIncomingMessage(peerA, blockMsg(rawPrefix, [9, 9, 9]));
      verifyNever(store.put(any, any));

      // The genuine block is still accepted afterwards
      await bitswap.handleIncomingMessage(peerB, blockMsg(rawPrefix, blockData));
      expect(await pending, blockData);
    });

    test('a block with a garbage prefix is rejected without disturbing the want', () async {
      final pending = bitswap.getBlock(cid, timeout: const Duration(hours: 1));
      await pumpEventQueue();

      await bitswap.handleIncomingMessage(peerA, blockMsg(garbage, blockData));
      await bitswap.handleIncomingMessage(peerA, blockMsg([], blockData));
      verifyNever(store.put(any, any));

      await bitswap.handleIncomingMessage(peerA, blockMsg(rawPrefix, blockData));
      expect(await pending, blockData);
    });

    test('a block hashed with a non-sha2 multihash (blake2b-256) completes getBlock', () async {
      final prefix = [0x01, 0x55, 0xa0, 0xe4, 0x02, 0x20]; // raw, blake2b-256 (0xb220), 32 bytes
      final blakeCid = cidFromPrefixAndData(Uint8List.fromList(prefix), blockData);
      final pending = bitswap.getBlock(blakeCid, timeout: const Duration(hours: 1));
      await pumpEventQueue();

      await bitswap.handleIncomingMessage(peerA, blockMsg(prefix, blockData));

      expect(await pending, blockData);
    });

    test('the same block delivered twice is stored once', () async {
      final pending = bitswap.getBlock(cid, timeout: const Duration(hours: 1));
      await pumpEventQueue();

      final both = pb.Message()..payload.addAll([...blockMsg(rawPrefix, blockData).payload, ...blockMsg(rawPrefix, blockData).payload]);
      await bitswap.handleIncomingMessage(peerA, both);

      expect(await pending, blockData);
      verify(store.put(any, any)).called(1);
    });
  });

  group('presences', () {
    test('a presence with a garbage CID is ignored', () async {
      swarm.addBitswapPeer(peerA);
      await bitswap.wantBlocks([cid]);
      await pumpEventQueue();
      final before = swarm.log.length;

      await bitswap.handleIncomingMessage(peerA, presence(garbage, pb.Message_BlockPresenceType.Have));
      await pumpEventQueue();

      expect(swarm.log.length, before);
    });

    test('a HAVE for a CID we do not want triggers nothing', () async {
      await bitswap.handleIncomingMessage(peerA, presence(cid.toBytes(), pb.Message_BlockPresenceType.Have));
      await bitswap.handleIncomingMessage(peerA, presence(cid.toBytes(), pb.Message_BlockPresenceType.DontHave));
      await pumpEventQueue();

      expect(swarm.sent, isEmpty);
    });
  });

  group('failing blockstore', () {
    test('a put failure leaves the want open for another delivery', () async {
      var calls = 0;
      when(store.put(any, any)).thenAnswer((_) async {
        if (calls++ == 0) throw StateError('disk full');
      });
      final pending = bitswap.getBlock(cid, timeout: const Duration(hours: 1));
      await pumpEventQueue();

      await bitswap.handleIncomingMessage(peerA, blockMsg(rawPrefix, blockData));
      await bitswap.handleIncomingMessage(peerB, blockMsg(rawPrefix, blockData));

      expect(await pending, blockData);
    });

    test('a read failure for one want does not stop the others being served', () async {
      final other = dart_cid.CID.fromData(dart_cid.CID.V1, 'raw', Uint8List.fromList([1]));
      when(store.has(other)).thenThrow(StateError('io error'));
      serveBlock();

      await bitswap.handleIncomingMessage(peerA, wantlist([wantEntry(other.toBytes()), wantEntry(cid.toBytes())]));
      await pumpEventQueue();

      expect(blocksTo(peerA).single.data, blockData);
    });
  });

  test('stop() while serving and fetching sends nothing further and does not throw', () async {
    // We serve [served] to B while fetching [cid] from A
    final served = dart_cid.CID.fromData(dart_cid.CID.V1, 'raw', Uint8List.fromList([1]));
    when(store.has(served)).thenAnswer((_) async => true);
    when(store.get(served)).thenAnswer((_) async => Uint8List.fromList([1]));
    swarm.addBitswapPeer(peerA);
    final pending = bitswap.getBlock(cid, timeout: const Duration(hours: 1));
    await bitswap.handleIncomingMessage(peerB, wantlist([wantEntry(served.toBytes())]));
    final expectation = expectLater(pending, throwsStateError);

    await bitswap.stop();
    final sentAtStop = swarm.log.length;
    await pumpEventQueue();

    await expectation;
    expect(swarm.log.length, sentAtStop);
    expect(blocksTo(peerB), isEmpty, reason: 'the queued reply was dropped');
  });
}
