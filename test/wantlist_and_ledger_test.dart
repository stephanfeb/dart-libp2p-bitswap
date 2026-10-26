import 'dart:typed_data'; // Import for Uint8List

import 'package:dart_libp2p_bitswap/src/client/local_wantlist.dart';
import 'package:dart_libp2p_bitswap/src/server/peer_ledger.dart';
import 'package:dart_libp2p_bitswap/src/proto/bitswap_message.pb.dart' as pb;
import 'package:dcid/dcid.dart' as dart_cid;
import 'package:test/test.dart';

import 'test_helpers.dart';

void main() {
  group('LocalWantlist and PeerLedger', () {
    late LocalWantlist wantlist;
    late PeerLedger ledger;
    late dart_cid.CID cid1;
    late dart_cid.CID cid2;
    late dart_cid.CID cid3;

    setUp(() {
      wantlist = LocalWantlist();
      ledger = PeerLedger();
      // Use known valid CIDs
      cid1 = dart_cid.CID.fromString(
          'bafkreihm362yepzlhjmcs3h2nryx2i6nfljmmqg43dsczfvtcndvklkuxy'); // "hello world"
      cid2 = dart_cid.CID.fromString(
          'bafkreicysg23kiwv34eg2d7qweipxw2w72v2edxywccodvrscfwb7na2i4'); // "Hello IPFS!"
      cid3 = dart_cid.CID.fromString(
          'bafkreibjo42q44jfv4zaz7z6n27m4goj4yose2nogxqk3b2x4laj5cuchi'); // "bitswap test"
    });

    group('Local Wantlist Management', () {
      test('addLocalWant adds a want and isWantingLocally reflects it', () {
        expect(wantlist.contains(cid1), isFalse);
        final entry = wantlist.add(cid1,
            priority: 10, wantType: pb.Message_Wantlist_WantType.Have);

        expect(entry, isNotNull);
        expect(entry!.cid, cid1);
        expect(entry.priority, 10);
        expect(entry.wantType, pb.Message_Wantlist_WantType.Have);
        expect(wantlist.contains(cid1), isTrue);
      });

      test('addLocalWant returns null if want already exists with same type',
          () {
        final originalEntry =
            wantlist.add(cid1, wantType: pb.Message_Wantlist_WantType.Block);
        expect(originalEntry, isNotNull,
            reason: "First call should return a non-null entry");

        final entry =
            wantlist.add(cid1, wantType: pb.Message_Wantlist_WantType.Block);
        expect(entry, isNull,
            reason: "Second call should return null (no change)");
      });

      test(
          'addLocalWant upgrades Have to Block in place, keeping the completer',
          () {
        final original =
            wantlist.add(cid1, wantType: pb.Message_Wantlist_WantType.Have)!;
        final upgraded =
            wantlist.add(cid1, wantType: pb.Message_Wantlist_WantType.Block);
        expect(upgraded, same(original));
        expect(upgraded!.wantType, pb.Message_Wantlist_WantType.Block);
      });

      test('addLocalWant does not downgrade Block to Have', () {
        wantlist.add(cid1, wantType: pb.Message_Wantlist_WantType.Block);
        final entry =
            wantlist.add(cid1, wantType: pb.Message_Wantlist_WantType.Have);
        expect(entry, isNull);
        expect(wantlist[cid1]!.wantType, pb.Message_Wantlist_WantType.Block);
      });

      test('releaseWaiter removes the want only when the last waiter leaves',
          () {
        wantlist.add(cid1);
        wantlist.addWaiter(cid1).ignore();
        wantlist.addWaiter(cid1).ignore();

        expect(wantlist.releaseWaiter(cid1), isNull);
        expect(wantlist.contains(cid1), isTrue);
        expect(wantlist.releaseWaiter(cid1), isNotNull);
        expect(wantlist.contains(cid1), isFalse);
      });

      test('removeLocalWant removes a want', () {
        wantlist.add(cid1);
        expect(wantlist.contains(cid1), isTrue);
        final removedEntry = wantlist.remove(cid1);
        expect(removedEntry, isNotNull);
        expect(removedEntry!.cid, cid1);
        expect(wantlist.contains(cid1), isFalse);
      });

      test('removeLocalWant returns null if want does not exist', () {
        final removedEntry = wantlist.remove(cid1);
        expect(removedEntry, isNull);
      });

      test('getLocalWants returns current local wants', () {
        expect(wantlist.all, isEmpty);
        wantlist.add(cid1);
        wantlist.add(cid2);
        final wants = wantlist.all;
        expect(wants.length, 2);
        expect(wants.any((e) => e.cid == cid1), isTrue);
        expect(wants.any((e) => e.cid == cid2), isTrue);
      });

      test('recordWantSentToPeer and getPeersSentWantForCid work correctly',
          () {
        wantlist.add(cid1);
        expect(wantlist.peersSentTo(cid1), isEmpty);

        wantlist.recordSentTo(cid1, testPeer(1));
        wantlist.recordSentTo(cid1, testPeer(2));

        final peers = wantlist.peersSentTo(cid1);
        expect(peers, containsAll([testPeer(1), testPeer(2)]));
        expect(peers.length, 2);

        expect(wantlist.peersSentTo(cid2),
            isEmpty); // For a CID not wanted or sent
      });
    });

    group('Peer Wantlist Management', () {
      final peerA = testPeer(1);
      final peerB = testPeer(2);

      test('addPeerWant adds a want for a peer', () {
        ledger.addWant(peerA, cid1,
            priority: 5, wantType: pb.Message_Wantlist_WantType.Have);
        final peerWants = ledger.wantsFor(peerA);
        expect(peerWants.length, 1);
        expect(peerWants[cid1.toString()], isNotNull);
        expect(peerWants[cid1.toString()]!.priority, 5);
        expect(peerWants[cid1.toString()]!.wantType,
            pb.Message_Wantlist_WantType.Have);
      });

      test('addPeerWant updates an existing want for a peer', () {
        ledger.addWant(peerA, cid1, priority: 1);
        ledger.addWant(peerA, cid1,
            priority: 100, wantType: pb.Message_Wantlist_WantType.Have);
        final peerWants = ledger.wantsFor(peerA);
        expect(peerWants.length, 1);
        expect(peerWants[cid1.toString()]!.priority, 100);
        expect(peerWants[cid1.toString()]!.wantType,
            pb.Message_Wantlist_WantType.Have);
      });

      test('removePeerWant removes a specific want for a peer', () {
        ledger.addWant(peerA, cid1);
        ledger.addWant(peerA, cid2);
        final removedEntry = ledger.removeWant(peerA, cid1);

        expect(removedEntry, isNotNull);
        expect(removedEntry!.cid, cid1);

        final peerWants = ledger.wantsFor(peerA);
        expect(peerWants.length, 1);
        expect(peerWants[cid2.toString()], isNotNull);
        expect(peerWants[cid1.toString()], isNull);
      });

      test('removePeerWant removes peer from map if last want is removed', () {
        ledger.addWant(peerA, cid1);
        ledger.removeWant(peerA, cid1);
        expect(ledger.wantsFor(peerA),
            isEmpty); // Should not throw, returns empty map
        // Internal check: _peerWantlists should not contain peerA
      });

      test('removePeerWant returns null if peer or want does not exist', () {
        expect(ledger.removeWant(peerA, cid1), isNull); // Peer doesn't exist
        ledger.addWant(peerA, cid1);
        expect(ledger.removeWant(peerA, cid2),
            isNull); // Want for cid2 doesn't exist for peerA
        expect(ledger.removeWant(peerB, cid1), isNull); // PeerB doesn't exist
      });

      test('clearPeerWantlist removes all wants for a specific peer', () {
        ledger.addWant(peerA, cid1);
        ledger.addWant(peerA, cid2);
        ledger.addWant(peerB, cid3);

        ledger.clearPeer(peerA);

        expect(ledger.wantsFor(peerA), isEmpty);
        expect(ledger.wantsFor(peerB).length, 1); // PeerB wants should remain
      });

      test('clearPeerWantlist does nothing if peer has no wants', () {
        ledger.clearPeer(peerA); // Should not throw
        expect(ledger.wantsFor(peerA), isEmpty);
      });
    });

    group('Notifications', () {
      test('blockReceived removes block from local wantlist', () {
        wantlist.add(cid1);
        expect(wantlist.contains(cid1), isTrue);

        wantlist.blockReceived(
            cid1, Uint8List(0)); // Data content doesn't matter for this test

        expect(wantlist.contains(cid1), isFalse);
      });

      test('blockReceived does nothing if block was not in local wantlist', () {
        expect(wantlist.contains(cid1), isFalse);
        wantlist.blockReceived(cid1, Uint8List(0));
        expect(wantlist.contains(cid1), isFalse); // Still false
      });
    });
  });
}
