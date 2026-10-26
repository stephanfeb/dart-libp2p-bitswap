import 'dart:typed_data';

import 'package:dart_libp2p_bitswap/src/bitswap.dart';
import 'package:dart_libp2p_bitswap/src/proto/bitswap_message.pb.dart' as pb;
import 'package:dcid/dcid.dart' as dart_cid;
import 'package:fake_async/fake_async.dart';
import 'package:mockito/mockito.dart';
import 'package:test/test.dart';

import 'bitswap_test.mocks.dart';
import 'fake_swarm.dart';
import 'test_helpers.dart';

/// Timeouts and rebroadcasts, driven by fake time so they are exact and
/// independent of machine load.
void main() {
  final blockData = Uint8List.fromList([5, 6, 7]);
  final cid = dart_cid.CID.fromData(dart_cid.CID.V1, 'raw', blockData);
  final peerA = testPeer(1), peerB = testPeer(2);
  final blockMsg = pb.Message()
    ..payload.add(pb.Message_Block()
      ..prefix = [0x01, 0x55, 0x12, 0x20]
      ..data = blockData);
  final dontHave = pb.Message()
    ..blockPresences.add(pb.Message_BlockPresence()
      ..cid = cid.toBytes()
      ..type = pb.Message_BlockPresenceType.DontHave);

  /// Runs [body] in fake time against a started Bitswap over a [FakeSwarm].
  void inFakeTime(
    void Function(FakeAsync time, Bitswap bitswap, FakeSwarm swarm) body, {
    BitswapOptions options = const BitswapOptions(),
  }) {
    fakeAsync((time) {
      final swarm = FakeSwarm();
      final store = MockBlockStore();
      when(store.has(any)).thenAnswer((_) async => false);
      when(store.put(any, any)).thenAnswer((_) async {});
      final bitswap = Bitswap(host: swarm.host, blockStore: store, options: options);
      bitswap.start();
      time.flushMicrotasks();

      body(time, bitswap, swarm);

      bitswap.stop();
      time.flushMicrotasks();
    });
  }

  /// Captures [future]'s outcome so fake-time tests can inspect it synchronously.
  ({bool done, Object? value}) Function() track<T>(Future<T> future) {
    var done = false;
    Object? value;
    future.then((v) {
      done = true;
      value = v;
    });
    return () => (done: done, value: value);
  }

  group('getBlock timeout', () {
    test('returns null exactly at the default timeout and CANCELs the peer asked', () {
      inFakeTime((time, bitswap, swarm) {
        swarm.addBitswapPeer(peerA);
        final result = track(bitswap.getBlock(cid));

        time.elapse(const Duration(seconds: 29, milliseconds: 999));
        expect(result().done, isFalse);
        expect(swarm.entriesTo(peerA, cid).any((e) => e.cancel), isFalse);

        time.elapse(const Duration(milliseconds: 1));
        expect(result(), (done: true, value: null));
        expect(swarm.entriesTo(peerA, cid).last.cancel, isTrue);
      }, options: const BitswapOptions(getBlockTimeout: Duration(seconds: 30), rebroadcastInterval: Duration(hours: 1)));
    });

    test('an explicit timeout overrides the default', () {
      inFakeTime((time, bitswap, swarm) {
        final result = track(bitswap.getBlock(cid, timeout: const Duration(seconds: 2)));
        time.elapse(const Duration(seconds: 2));
        expect(result(), (done: true, value: null));
      });
    });

    test('one caller timing out does not abandon the want for another', () {
      inFakeTime((time, bitswap, swarm) {
        swarm.addBitswapPeer(peerA);
        final short = track(bitswap.getBlock(cid, timeout: const Duration(seconds: 1)));
        final long = track(bitswap.getBlock(cid, timeout: const Duration(seconds: 10)));

        time.elapse(const Duration(seconds: 1));
        expect(short(), (done: true, value: null));
        expect(swarm.entriesTo(peerA, cid).any((e) => e.cancel), isFalse, reason: 'still wanted by the other caller');

        bitswap.handleIncomingMessage(peerA, blockMsg);
        time.flushMicrotasks();
        expect(long().done, isTrue);
        expect(long().value, blockData);
      });
    });
  });

  group('rebroadcast', () {
    const interval = Duration(seconds: 10);
    const options = BitswapOptions(rebroadcastInterval: interval);

    test('sends wants made with no peers once the interval passes', () {
      inFakeTime((time, bitswap, swarm) {
        bitswap.wantBlocks([cid]);
        time.flushMicrotasks();
        swarm.addBitswapPeer(peerA);

        time.elapse(interval - const Duration(milliseconds: 1));
        expect(swarm.sent[peerA], isNull);

        time.elapse(const Duration(milliseconds: 1));
        expect(swarm.sentWantBlock(peerA, cid), isTrue);
        expect(swarm.sent[peerA]!.first.wantlist.full, isTrue);
      }, options: options);
    });

    test('re-targets WANT_BLOCK when every asked peer said DONT_HAVE, without re-asking them', () {
      inFakeTime((time, bitswap, swarm) {
        swarm.addBitswapPeer(peerA);
        bitswap.wantBlocks([cid]);
        time.flushMicrotasks();
        bitswap.handleIncomingMessage(peerA, dontHave);
        swarm.addBitswapPeer(peerB);

        time.elapse(interval * 3);

        expect(swarm.sentWantBlock(peerB, cid), isTrue);
        expect(swarm.entriesTo(peerA, cid), hasLength(1));
        expect(swarm.entriesTo(peerB, cid), hasLength(1));
      }, options: options);
    });

    test('sends nothing when the wantlist is empty', () {
      inFakeTime((time, bitswap, swarm) {
        swarm.addBitswapPeer(peerA);
        time.elapse(interval * 3);
        expect(swarm.sent, isEmpty);
      }, options: options);
    });

    test('stops after stop()', () {
      inFakeTime((time, bitswap, swarm) {
        bitswap.wantBlocks([cid]);
        time.flushMicrotasks();
        bitswap.stop(); // Not awaited: nothing may be sent even while it runs
        swarm.addBitswapPeer(peerA);

        time.elapse(interval * 3);
        expect(swarm.sent, isEmpty);
        expect(time.periodicTimerCount, 0);
      }, options: options);
    });
  });
}
