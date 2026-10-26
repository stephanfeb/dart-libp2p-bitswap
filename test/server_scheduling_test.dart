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

  group('WANT_HAVE replaced by a tiny block', () {
    final tiny = Uint8List.fromList(List.filled(1024, 1)); // Exactly the default limit
    final large = Uint8List.fromList(List.filled(1025, 2));
    final tinyCid = dart_cid.CID.fromData(dart_cid.CID.V1, 'raw', tiny);
    final largeCid = dart_cid.CID.fromData(dart_cid.CID.V1, 'raw', large);

    pb.Message wantHaves(List<dart_cid.CID> cids) => pb.Message()
      ..wantlist = (pb.Message_Wantlist()
        ..entries.addAll([
          for (final cid in cids)
            pb.Message_Wantlist_Entry()
              ..block = cid.toBytes()
              ..wantType = pb.Message_Wantlist_WantType.Have
              ..sendDontHave = true,
        ]));

    ({List<int> blockSizes, int haves}) repliesTo(peer) {
      final messages = swarm.sent[peer] ?? const <pb.Message>[];
      return (
        blockSizes: [for (final m in messages) for (final b in m.payload) b.data.length],
        haves: [for (final m in messages) ...m.blockPresences]
            .where((p) => p.type == pb.Message_BlockPresenceType.Have)
            .length,
      );
    }

    void storeHas(Map<dart_cid.CID, Uint8List> stored) {
      when(store.has(any)).thenAnswer((inv) async => stored.containsKey(inv.positionalArguments[0]));
      when(store.get(any)).thenAnswer((inv) async => stored[inv.positionalArguments[0]]);
    }

    test('blocks up to wantHaveReplaceSize are sent; larger ones get HAVE', () async {
      storeHas({tinyCid: tiny, largeCid: large});
      await startWith(const BitswapOptions());

      await bitswap.handleIncomingMessage(peerA, wantHaves([tinyCid, largeCid]));
      await pumpEventQueue();

      expect(repliesTo(peerA).blockSizes, [1024]);
      expect(repliesTo(peerA).haves, 1);
    });

    test('wantHaveReplaceSize 0 always answers HAVE', () async {
      storeHas({tinyCid: tiny});
      await startWith(const BitswapOptions(wantHaveReplaceSize: 0));

      await bitswap.handleIncomingMessage(peerA, wantHaves([tinyCid]));
      await pumpEventQueue();

      expect(repliesTo(peerA).blockSizes, []);
      expect(repliesTo(peerA).haves, 1);
      verifyNever(store.get(any));
    });

    test('a SizedBlockStore is asked for the size, so large blocks are not read', () async {
      final sized = _SizedStore({tinyCid: tiny, largeCid: large});
      bitswap = Bitswap(host: swarm.host, blockStore: sized);
      await bitswap.start();
      started = true;

      await bitswap.handleIncomingMessage(peerA, wantHaves([tinyCid, largeCid]));
      await pumpEventQueue();

      expect(repliesTo(peerA).blockSizes, [1024]);
      expect(repliesTo(peerA).haves, 1);
      expect(sized.gets, [tinyCid.toString()], reason: 'only the tiny block is read');
    });
  });

  group('pendingBytes', () {
    dart_cid.CID cidOf(Uint8List data) => dart_cid.CID.fromData(dart_cid.CID.V1, 'raw', data);
    Uint8List sized(int n, int fill) => Uint8List.fromList(List.filled(n, fill));
    int presenceSize(dart_cid.CID cid) => (pb.Message_BlockPresence()
          ..cid = cid.toBytes()
          ..type = pb.Message_BlockPresenceType.Have)
        .writeToBuffer()
        .length;

    pb.Message_Wantlist_Entry entry(dart_cid.CID cid,
            {int priority = 1, bool have = false, bool sendDontHave = true}) =>
        pb.Message_Wantlist_Entry()
          ..block = cid.toBytes()
          ..priority = priority
          ..wantType = have ? pb.Message_Wantlist_WantType.Have : pb.Message_Wantlist_WantType.Block
          ..sendDontHave = sendDontHave;

    Future<void> startSized(Map<dart_cid.CID, Uint8List> stored) async {
      bitswap = Bitswap(
        host: swarm.host,
        blockStore: _SizedStore(stored),
        options: const BitswapOptions(serveBytesPerTurn: 1), // One block per turn
      );
      await bitswap.start();
      started = true;
    }

    List<int> pendingBytesSent() => [for (final m in swarm.sent[peerA]!) m.pendingBytes];

    test('counts the blocks still queued after each turn', () async {
      final a = sized(10, 1), b = sized(20, 2), c = sized(30, 3);
      await startSized({cidOf(a): a, cidOf(b): b, cidOf(c): c});

      await bitswap.handleIncomingMessage(peerA, pb.Message()
        ..wantlist = (pb.Message_Wantlist()
          ..entries.addAll([
            entry(cidOf(a), priority: 3),
            entry(cidOf(b), priority: 2),
            entry(cidOf(c), priority: 1),
          ])));
      await pumpEventQueue();

      expect(pendingBytesSent(), [50, 30, 0]);
    });

    test('counts presences, tiny blocks and DONT_HAVEs as boxo does', () async {
      final first = sized(10, 1), large = sized(2000, 2), tiny = sized(5, 3);
      final missing = cidOf(sized(1, 9)), missingQuiet = cidOf(sized(1, 8));
      await startSized({cidOf(first): first, cidOf(large): large, cidOf(tiny): tiny});

      await bitswap.handleIncomingMessage(peerA, pb.Message()
        ..wantlist = (pb.Message_Wantlist()
          ..entries.addAll([
            entry(cidOf(first), priority: 9), // Served on the first turn
            entry(cidOf(large), have: true), // HAVE
            entry(cidOf(tiny), have: true), // The block, replacing HAVE
            entry(missing), // DONT_HAVE
            entry(missingQuiet, sendDontHave: false), // Nothing
          ])));
      await pumpEventQueue();

      expect(pendingBytesSent().first, presenceSize(cidOf(large)) + tiny.length + presenceSize(missing));
    });

    test('is left unset with a plain BlockStore, which cannot size blocks', () async {
      await startWith(const BitswapOptions(serveBytesPerTurn: 1));

      await bitswap.handleIncomingMessage(peerA, wantBlocks(cids.sublist(0, 3)));
      await pumpEventQueue();

      expect(pendingBytesSent(), [0, 0, 0]);
      expect(swarm.sent[peerA]!.every((m) => !m.hasPendingBytes()), isTrue);
    });
  });
}

/// A [SizedBlockStore] that records which blocks were read.
class _SizedStore implements SizedBlockStore {
  final Map<String, Uint8List> _blocks;
  final List<String> gets = [];

  _SizedStore(Map<dart_cid.CID, Uint8List> blocks) : _blocks = {for (final e in blocks.entries) e.key.toString(): e.value};

  @override
  Future<bool> has(dart_cid.CID cid) async => _blocks.containsKey(cid.toString());

  @override
  Future<Uint8List?> get(dart_cid.CID cid) async {
    gets.add(cid.toString());
    return _blocks[cid.toString()];
  }

  @override
  Future<int?> getSize(dart_cid.CID cid) async => _blocks[cid.toString()]?.length;

  @override
  Future<void> put(dart_cid.CID cid, Uint8List data) async => _blocks[cid.toString()] = data;
}
