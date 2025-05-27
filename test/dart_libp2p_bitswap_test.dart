import 'dart:typed_data'; // Import for Uint8List

import 'package:dart_libp2p_bitswap/src/want_manager.dart';
import 'package:dart_libp2p_bitswap/src/proto/bitswap_message.pb.dart' as pb;
import 'package:dart_cid/dart_cid.dart' as dart_cid;
import 'package:test/test.dart';
// Mockito is available but not strictly needed for WantManager tests as it has no external dependencies to mock.

void main() {
  group('WantManager Tests', () {
    late WantManager wantManager;
    late dart_cid.CID cid1;
    late dart_cid.CID cid2;
    late dart_cid.CID cid3;

    setUp(() {
      wantManager = WantManager();
      // Use known valid CIDs
      cid1 = dart_cid.CID.fromString('bafkreihm362yepzlhjmcs3h2nryx2i6nfljmmqg43dsczfvtcndvklkuxy'); // "hello world"
      cid2 = dart_cid.CID.fromString('bafkreicysg23kiwv34eg2d7qweipxw2w72v2edxywccodvrscfwb7na2i4'); // "Hello IPFS!"
      cid3 = dart_cid.CID.fromString('bafkreibjo42q44jfv4zaz7z6n27m4goj4yose2nogxqk3b2x4laj5cuchi'); // "bitswap test"
    });

    group('Local Wantlist Management', () {
      test('addLocalWant adds a want and isWantingLocally reflects it', () {
        expect(wantManager.isWantingLocally(cid1), isFalse);
        final entry = wantManager.addLocalWant(cid1, priority: 10, wantType: pb.Message_Wantlist_WantType.Have);
        
        expect(entry, isNotNull);
        expect(entry!.cid, cid1);
        expect(entry.priority, 10);
        expect(entry.wantType, pb.Message_Wantlist_WantType.Have);
        expect(wantManager.isWantingLocally(cid1), isTrue);
      });

      test('addLocalWant returns null if want already exists with same type', () {
        wantManager.addLocalWant(cid1, wantType: pb.Message_Wantlist_WantType.Block);
        final entry = wantManager.addLocalWant(cid1, wantType: pb.Message_Wantlist_WantType.Block);
        expect(entry, isNull);
      });
      
      test('addLocalWant adds new entry if want exists with different type', () {
        wantManager.addLocalWant(cid1, wantType: pb.Message_Wantlist_WantType.Block);
        final entry = wantManager.addLocalWant(cid1, wantType: pb.Message_Wantlist_WantType.Have);
        expect(entry, isNotNull);
        expect(entry!.wantType, pb.Message_Wantlist_WantType.Have);
      });

      test('removeLocalWant removes a want', () {
        wantManager.addLocalWant(cid1);
        expect(wantManager.isWantingLocally(cid1), isTrue);
        final removedEntry = wantManager.removeLocalWant(cid1);
        expect(removedEntry, isNotNull);
        expect(removedEntry!.cid, cid1);
        expect(wantManager.isWantingLocally(cid1), isFalse);
      });

      test('removeLocalWant returns null if want does not exist', () {
        final removedEntry = wantManager.removeLocalWant(cid1);
        expect(removedEntry, isNull);
      });

      test('getLocalWants returns current local wants', () {
        expect(wantManager.getLocalWants(), isEmpty);
        wantManager.addLocalWant(cid1);
        wantManager.addLocalWant(cid2);
        final wants = wantManager.getLocalWants();
        expect(wants.length, 2);
        expect(wants.any((e) => e.cid == cid1), isTrue);
        expect(wants.any((e) => e.cid == cid2), isTrue);
      });

      test('recordWantSentToPeer and getPeersSentWantForCid work correctly', () {
        wantManager.addLocalWant(cid1);
        expect(wantManager.getPeersSentWantForCid(cid1), isEmpty);
        
        wantManager.recordWantSentToPeer(cid1, 'peerA');
        wantManager.recordWantSentToPeer(cid1, 'peerB');
        
        final peers = wantManager.getPeersSentWantForCid(cid1);
        expect(peers, containsAll(['peerA', 'peerB']));
        expect(peers.length, 2);

        expect(wantManager.getPeersSentWantForCid(cid2), isEmpty); // For a CID not wanted or sent
      });
    });

    group('Peer Wantlist Management', () {
      final peerA = 'peerA';
      final peerB = 'peerB';

      test('addPeerWant adds a want for a peer', () {
        wantManager.addPeerWant(peerA, cid1, priority: 5, wantType: pb.Message_Wantlist_WantType.Have);
        final peerWants = wantManager.getWantsForPeer(peerA);
        expect(peerWants.length, 1);
        expect(peerWants[cid1.toString()], isNotNull);
        expect(peerWants[cid1.toString()]!.priority, 5);
        expect(peerWants[cid1.toString()]!.wantType, pb.Message_Wantlist_WantType.Have);
      });

      test('addPeerWant updates an existing want for a peer', () {
        wantManager.addPeerWant(peerA, cid1, priority: 1);
        wantManager.addPeerWant(peerA, cid1, priority: 100, wantType: pb.Message_Wantlist_WantType.Have);
        final peerWants = wantManager.getWantsForPeer(peerA);
        expect(peerWants.length, 1);
        expect(peerWants[cid1.toString()]!.priority, 100);
        expect(peerWants[cid1.toString()]!.wantType, pb.Message_Wantlist_WantType.Have);
      });

      test('removePeerWant removes a specific want for a peer', () {
        wantManager.addPeerWant(peerA, cid1);
        wantManager.addPeerWant(peerA, cid2);
        final removedEntry = wantManager.removePeerWant(peerA, cid1);
        
        expect(removedEntry, isNotNull);
        expect(removedEntry!.cid, cid1);
        
        final peerWants = wantManager.getWantsForPeer(peerA);
        expect(peerWants.length, 1);
        expect(peerWants[cid2.toString()], isNotNull);
        expect(peerWants[cid1.toString()], isNull);
      });
      
      test('removePeerWant removes peer from map if last want is removed', () {
        wantManager.addPeerWant(peerA, cid1);
        wantManager.removePeerWant(peerA, cid1);
        expect(wantManager.getWantsForPeer(peerA), isEmpty); // Should not throw, returns empty map
        // Internal check: _peerWantlists should not contain peerA
      });

      test('removePeerWant returns null if peer or want does not exist', () {
        expect(wantManager.removePeerWant(peerA, cid1), isNull); // Peer doesn't exist
        wantManager.addPeerWant(peerA, cid1);
        expect(wantManager.removePeerWant(peerA, cid2), isNull); // Want for cid2 doesn't exist for peerA
        expect(wantManager.removePeerWant(peerB, cid1), isNull); // PeerB doesn't exist
      });

      test('clearPeerWantlist removes all wants for a specific peer', () {
        wantManager.addPeerWant(peerA, cid1);
        wantManager.addPeerWant(peerA, cid2);
        wantManager.addPeerWant(peerB, cid3);
        
        wantManager.clearPeerWantlist(peerA);
        
        expect(wantManager.getWantsForPeer(peerA), isEmpty);
        expect(wantManager.getWantsForPeer(peerB).length, 1); // PeerB wants should remain
      });
      
      test('clearPeerWantlist does nothing if peer has no wants', () {
         wantManager.clearPeerWantlist(peerA); // Should not throw
         expect(wantManager.getWantsForPeer(peerA), isEmpty);
      });
    });

    group('Notifications', () {
      test('blockReceived removes block from local wantlist', () {
        wantManager.addLocalWant(cid1);
        expect(wantManager.isWantingLocally(cid1), isTrue);
        
        wantManager.blockReceived(cid1, Uint8List(0)); // Data content doesn't matter for this test
        
        expect(wantManager.isWantingLocally(cid1), isFalse);
      });

      test('blockReceived does nothing if block was not in local wantlist', () {
        expect(wantManager.isWantingLocally(cid1), isFalse);
        wantManager.blockReceived(cid1, Uint8List(0));
        expect(wantManager.isWantingLocally(cid1), isFalse); // Still false
      });

      // receivedHave and receivedDontHave currently only print.
      // Tests can be added here if their logic becomes more complex.
      test('receivedHave can be called', () {
        // This test mainly ensures the method can be called without error.
        // Actual logic for receivedHave is a TODO in WantManager.
        wantManager.addLocalWant(cid1);
        expect(() => wantManager.receivedHave('peerA', cid1), returnsNormally);
      });

      test('receivedDontHave can be called', () {
        // This test mainly ensures the method can be called without error.
        // Actual logic for receivedDontHave is a TODO in WantManager.
        wantManager.addLocalWant(cid1);
        expect(() => wantManager.receivedDontHave('peerA', cid1), returnsNormally);
      });
    });
  });
}
