import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:dcid/dcid.dart';
import 'package:dart_libp2p_bitswap/src/client/local_wantlist.dart';


void main() {
  group('LocalWantlist waiter tests', () {
    late LocalWantlist wantlist;
    late CID sampleCid1;
    late CID sampleCid2;
    late Uint8List sampleData;

    setUp(() {
      wantlist = LocalWantlist();
      // Using a simple CIDv0 for testing. Replace with a valid CID generation if necessary.
      sampleCid1 = CID.fromString('QmQ2r6iMNpky5f1m4cnm3Yg8eWnLdnMYD8K5Z2cd8exNNo');
      sampleCid2 = CID.fromString('QmY7Yh4UquoXHLPFo2XbhXkhBvFoPwmQUSr93Re6K3LgR4');
      sampleData = Uint8List.fromList([1, 2, 3, 4, 5]);
    });

    test('Scenario A: Successful Block Reception', () async {
      // 1. Add a CID to the local wantlist
      wantlist.add(sampleCid1);
      expect(wantlist.contains(sampleCid1), isTrue, reason: 'CID should be in local wantlist after adding.');

      // 2. Call whenBlockReceived(cid) to get a Future
      final futureBlock = wantlist.addWaiter(sampleCid1);
      
      // Ensure the completer is there
      final entry = wantlist.all.firstWhere((e) => e.cid.toString() == sampleCid1.toString());
      expect(entry.completer, isNotNull, reason: 'LocalWant should have a completer.');
      expect(entry.completer.isCompleted, isFalse, reason: 'Completer should not be completed yet.');

      // 3. Call blockReceived(cid, data)
      wantlist.blockReceived(sampleCid1, sampleData);

      // Expected Outcome: The Future should complete successfully with the provided block data.
      await expectLater(futureBlock, completion(equals(sampleData)), reason: 'Future should complete with the sample data.');
      
      // The CID should also be removed from the local wantlist.
      expect(wantlist.contains(sampleCid1), isFalse, reason: 'CID should be removed from wantlist after block is received.');
    });

    test('Scenario B: Requesting a Block Not in Wantlist', () async {
      // Action: Call whenBlockReceived(cid) for a CID that has not been added
      final futureBlock = wantlist.addWaiter(sampleCid2);

      // Expected Outcome: The Future should complete with a StateError.
      await expectLater(futureBlock, throwsA(isA<StateError>()), reason: 'Future should complete with a StateError for a non-wanted block.');
    });

    test('Scenario C: Multiple Waiters for the Same Block', () async {
      // 1. Add a CID to the local wantlist
      wantlist.add(sampleCid1);

      // 2. Call whenBlockReceived(cid) multiple times
      final futureBlock1 = wantlist.addWaiter(sampleCid1);
      final futureBlock2 = wantlist.addWaiter(sampleCid1);
      final futureBlock3 = wantlist.addWaiter(sampleCid1);
      
      // Ensure the completer is the same for all (as it's tied to the LocalWant)
      final entry = wantlist.all.firstWhere((e) => e.cid.toString() == sampleCid1.toString());
      expect(entry.completer, isNotNull);

      // 3. Call blockReceived(cid, data)
      wantlist.blockReceived(sampleCid1, sampleData);

      // Expected Outcome: All Futures should complete successfully with the same block data.
      await expectLater(futureBlock1, completion(equals(sampleData)), reason: 'Future 1 should complete with sample data.');
      await expectLater(futureBlock2, completion(equals(sampleData)), reason: 'Future 2 should complete with sample data.');
      await expectLater(futureBlock3, completion(equals(sampleData)), reason: 'Future 3 should complete with sample data.');
      
      expect(wantlist.contains(sampleCid1), isFalse, reason: 'CID should be removed after block reception.');
    });

    test('Scenario D: Block Received *Before* whenBlockReceived is Called', () async {
      // 1. Add a CID to the local wantlist
      wantlist.add(sampleCid1);
      expect(wantlist.contains(sampleCid1), isTrue);

      // 2. Call blockReceived(cid, data)
      wantlist.blockReceived(sampleCid1, sampleData);
      
      // The want should now be removed
      expect(wantlist.contains(sampleCid1), isFalse, reason: 'CID should be removed after blockReceived.');

      // 3. *Then*, call whenBlockReceived(cid)
      final futureBlock = wantlist.addWaiter(sampleCid1);

      // Expected Outcome: The Future should complete with a StateError.
      await expectLater(futureBlock, throwsA(isA<StateError>()), reason: 'Future should complete with StateError if block already received and want removed.');
    });
    
    test('whenBlockReceived returns the same future if called multiple times before completion', () {
      wantlist.add(sampleCid1);
      final future1 = wantlist.addWaiter(sampleCid1);
      final future2 = wantlist.addWaiter(sampleCid1);
      expect(future1, same(future2), reason: 'Multiple calls to whenBlockReceived for the same wanted CID should return the same Future instance.');
    });

    test('blockReceived completes the future from whenBlockReceived', () async {
      wantlist.add(sampleCid1);
      final receivedFuture = wantlist.addWaiter(sampleCid1);
      
      // Simulate receiving the block
      wantlist.blockReceived(sampleCid1, sampleData);
      
      // The future should complete with the data
      final receivedData = await receivedFuture;
      expect(receivedData, equals(sampleData));
      expect(wantlist.contains(sampleCid1), isFalse, reason: 'Want should be removed after block is received.');
    });

    test('blockReceived on a non-wanted CID does not throw error and does not affect other wants', () async {
      // Add a want for sampleCid1
      wantlist.add(sampleCid1);
      final future1 = wantlist.addWaiter(sampleCid1);

      // Call blockReceived for sampleCid2 (which is not wanted)
      expect(() => wantlist.blockReceived(sampleCid2, sampleData), returnsNormally);
      
      // Ensure sampleCid1 is still wanted and its future is not completed
      expect(wantlist.contains(sampleCid1), isTrue);
      
      // If blockReceived for sampleCid2 incorrectly affected sampleCid1, its
      // future would complete once pending microtasks have run
      bool future1Completed = false;
      future1.then((_) => future1Completed = true).catchError((_) => future1Completed = true);

      await pumpEventQueue();
      expect(future1Completed, isFalse, reason: "Future for wanted CID shouldn't complete when an unrelated block is received.");

      // Now complete the actual wanted block
      wantlist.blockReceived(sampleCid1, sampleData);
      await expectLater(future1, completion(equals(sampleData)));
    });

  });
}
