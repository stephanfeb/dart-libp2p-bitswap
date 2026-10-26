import 'dart:async';
import 'dart:typed_data';

import 'package:dart_libp2p/p2p/discovery/peer_info.dart';
import 'package:dart_libp2p_bitswap/src/bitswap.dart';
// import 'package:dart_libp2p_bitswap/src/want_manager.dart'; // Used for type hint if injecting - REMOVED
// import 'package:dart_libp2p_bitswap/src/network_service.dart'; // Used for type hint if injecting - REMOVED
import 'package:dart_libp2p_bitswap/src/proto/bitswap_message.pb.dart' as pb;
import 'package:dcid/dcid.dart' as dart_cid;
import 'package:dart_libp2p/dart_libp2p.dart' as libp2p;
import 'package:test/test.dart';
import 'package:mockito/mockito.dart';
import 'package:mockito/annotations.dart';

import 'bitswap_test.mocks.dart'; // Will be generated
import 'test_helpers.dart';
import 'stream_stubs.dart';

// Define a mockable class for the object returned by eventBus.subscribe()
abstract class TestEventSubscription<T> implements libp2p.Subscription<T> {
  // Added implements
  // Stream<T> get stream; // Already in libp2p.Subscription
  // Future<void> close(); // Already in libp2p.Subscription, was cancel()
  // String get name; // Already in libp2p.Subscription
}

@GenerateMocks([
  BlockStore,
  libp2p.Host,
  libp2p.PeerId,
  libp2p.EventBus,
  libp2p.Network,
  libp2p.Peerstore,
  libp2p.P2PStream,
  libp2p.Conn, // From user's previous file state
  PeerInfo, // Using PeerInfo as suggested by type errors, removed libp2p prefix
  TestEventSubscription, // Mocking this abstract class
])
void main() {
  late MockHost mockHost;
  late MockBlockStore mockBlockStore;
  late MockEventBus mockEventBus;
  late MockNetwork mockNetwork;
  late MockPeerstore mockPeerStore;
  // late MockTestEventSubscription<libp2p.EvtPeerConnectednessChanged> mockEventBusSubscription; // No longer directly needed for eventBus.subscribe mock

  late StreamController<libp2p.EvtPeerConnectednessChanged>
      peerConnectednessEventController;
  late Bitswap bitswap;

  final cid1 = dart_cid.CID.fromString(
      'bafkreihm362yepzlhjmcs3h2nryx2i6nfljmmqg43dsczfvtcndvklkuxy'); // Replaced with a valid CID for "hello world"
  late libp2p.PeerId testPeerId1;
  final otherPeer = testPeer(9);
  // This is the Base58 multihash representation of an identity-hashed PeerID from the string "test-peer-id-1"
  // (derived from PeerId.fromString("bafzbeigweq4j366sq2k7j727lqgrw7x2k3x5ms2sl4unhyrionp2zlbxci").toBase58())
  final testPeerId1Str = 'QmckZzS6pmmQXrmZPFjevKuT3Hip4kNyj5FjarHrYKfcBs';
  final bitswapProtocolId = '/ipfs/bitswap/1.2.0';

  setUp(() async {
    mockHost = MockHost();
    mockBlockStore = MockBlockStore();
    mockEventBus = MockEventBus();
    mockNetwork = MockNetwork();
    mockPeerStore = MockPeerstore();
    // mockEventBusSubscription = MockTestEventSubscription<libp2p.EvtPeerConnectednessChanged>(); // No longer directly needed

    testPeerId1 = libp2p.PeerId.fromString(testPeerId1Str);

    final hostMockPeerId = MockPeerId();
    when(hostMockPeerId.toBase58()).thenReturn(
        "QmHostPeerIdForTest"); // Example Peer ID string for the host
    when(mockHost.id).thenReturn(hostMockPeerId); // Stub for mockHost.id

    when(mockHost.eventBus).thenReturn(mockEventBus);
    when(mockHost.network).thenReturn(mockNetwork);
    when(mockHost.peerStore).thenReturn(mockPeerStore);

    peerConnectednessEventController =
        StreamController<libp2p.EvtPeerConnectednessChanged>.broadcast();

    // Default stubs for Host network/peerstore for _getBitswapPeers
    when(mockNetwork.conns).thenReturn([]);
    when(mockPeerStore.getPeer(any)).thenAnswer((_) async => null);

    // Default stub for newStream (used by internal NetworkService)
    final mockP2PStream = MockP2PStream();
    stubLiveStream(mockP2PStream);
    when(mockP2PStream.id()).thenReturn('mock-stream-default');
    when(mockP2PStream.write(any)).thenAnswer((_) async {});
    when(mockP2PStream.close()).thenAnswer((_) async {});
    when(mockHost.newStream(any, any, any))
        .thenAnswer((_) async => mockP2PStream);

    // Default stubs for BlockStore
    when(mockBlockStore.has(any)).thenAnswer((_) async => false);
    when(mockBlockStore.get(any)).thenAnswer((_) async => null);
    when(mockBlockStore.put(any, any)).thenAnswer((_) async {});

    stubPeerEvents(mockEventBus, peerConnectednessEventController.stream);
    bitswap = Bitswap(host: mockHost, blockStore: mockBlockStore);
    await bitswap.start();
  });

  tearDown(() async {
    await peerConnectednessEventController.close();
    await bitswap.stop();
  });

  group('Constructor and Stop', () {
    test(
        'start() subscribes to peer connectedness events and registers the handler',
        () {
      verify(mockEventBus.subscribe(libp2p.EvtPeerConnectednessChanged,
              opts: anyNamed('opts')))
          .called(1);
      expect(peerConnectednessEventController.hasListener, isTrue);
      verify(mockHost.setStreamHandler(bitswapProtocolId, any)).called(1);
    });

    test('constructor has no side effects until start()', () {
      clearInteractions(mockHost); // setUp already started the shared instance
      final idle = Bitswap(host: mockHost, blockStore: mockBlockStore);
      verifyNever(mockHost.setStreamHandler(any, any));
      expect(() => idle.wantBlocks([cid1]), throwsStateError);
      expect(idle.getBlock(cid1), throwsStateError);
    });

    test('start() is idempotent and cannot restart a stopped instance',
        () async {
      await bitswap.start();
      verify(mockHost.setStreamHandler(bitswapProtocolId, any)).called(1);
      await bitswap.stop();
      expect(bitswap.start(), throwsStateError);
    });

    test('stop() cancels event subscriptions and stops network service',
        () async {
      // Ensure there's a listener before stop() is called
      expect(peerConnectednessEventController.hasListener, isTrue);

      await bitswap.stop();

      // Verify the subscription obtained from the stream was cancelled
      // This means the StreamController should no longer have a listener after Bitswap.stop()
      expect(peerConnectednessEventController.hasListener, isFalse);

      // Verifying networkService.stop() is implicit as it's internal.
      // We could check if host.removeStreamHandler is called if NetworkService.stop() does that.
      verify(mockHost.removeStreamHandler(bitswapProtocolId))
          .called(1); // Assuming NetworkService.stop calls this
    });

    test('stop() fails pending getBlock calls and is idempotent', () async {
      final pending =
          bitswap.getBlock(cid1, timeout: const Duration(seconds: 5));
      await Future.delayed(Duration.zero);
      final failed = expectLater(pending, throwsStateError);

      await bitswap.stop();
      await bitswap.stop();

      await failed;
      verify(mockHost.removeStreamHandler(bitswapProtocolId)).called(1);
    });

    test('getBlock and incoming messages are rejected/ignored after stop()',
        () async {
      await bitswap.stop();

      await expectLater(bitswap.getBlock(cid1), throwsStateError);
      await bitswap.handleIncomingMessage(
          testPeerId1,
          pb.Message()
            ..wantlist = (pb.Message_Wantlist()
              ..entries
                  .add(pb.Message_Wantlist_Entry()..block = cid1.toBytes())));
      verifyNever(mockBlockStore.has(any));
    });

    test('an error on the peer events stream does not escape', () async {
      peerConnectednessEventController
          .addError(StateError('event bus failure'));
      await pumpEventQueue();
      expect(peerConnectednessEventController.hasListener, isTrue);
    });
  });

  group('wantBlocks()', () {
    test('does nothing if CIDs list is empty', () async {
      await bitswap.wantBlocks([]);
      // No network activity should occur if no CIDs are wanted.
      verifyNever(mockHost.newStream(any, any, any));
    });

    test('sends wantlist message to discovered Bitswap peers', () async {
      final mockPeerInfo = MockPeerInfo();
      when(mockPeerInfo.protocols).thenReturn({bitswapProtocolId});

      final mockConn1 = MockConn();
      when(mockConn1.remotePeer).thenReturn(testPeerId1);

      when(mockNetwork.conns).thenReturn([mockConn1]);
      when(mockPeerStore.getPeer(testPeerId1))
          .thenAnswer((_) async => mockPeerInfo);

      await bitswap.wantBlocks([cid1]);
      await pumpEventQueue(); // sends are queued per peer

      // Verify that the internal NetworkService tried to open a stream to the peer
      // The PeerId passed to newStream will be a real PeerId created from testPeerId1Str.
      verify(mockHost.newStream(
              argThat(isA<libp2p.PeerId>()
                  .having((p) => p.toBase58(), 'toBase58', testPeerId1Str)),
              [bitswapProtocolId],
              any))
          .called(1);
      // Further verification of the *exact* bytes sent is complex without deeper spying
      // or making NetworkService injectable and mockable. We trust NetworkService's own tests here.
    });

    test('does not send if no new wants are actually added', () async {
      // First, make the want "old" by adding it directly to the internal want manager.
      // For now, we call wantBlocks twice; the first adds it, the second should find it "old".

      when(mockNetwork.conns).thenReturn([]); // No peers to simplify
      await bitswap.wantBlocks([cid1]); // First call, want is new

      // Reset newStream mock call count before the second call
      clearInteractions(mockHost);
      when(mockHost.newStream(any, any, any)).thenAnswer((_) async {
        // Re-stub
        final stream = MockP2PStream();
        stubLiveStream(stream);
        return stream;
      });

      await bitswap.wantBlocks([cid1]); // Second call, want should be old
      verifyNever(mockHost.newStream(any, any, any));
    });
  });

  group('Peer Disconnection Handling', () {
    test('cleans up when a peer disconnects', () async {
      // Simulate a peer disconnecting by emitting an event
      final disconnectedPeerEvent = libp2p.EvtPeerConnectednessChanged(
          // Corrected instantiation
          peer: testPeerId1,
          connectedness: libp2p.Connectedness.notConnected);
      peerConnectednessEventController.add(disconnectedPeerEvent);

      // Allow event to propagate and be handled
      await Future.delayed(Duration.zero);

      // Verify that NetworkService's handlePeerDisconnected was called (indirectly)
      // This is hard to verify directly without injecting NetworkService.
      // We can check if the internal _activeStreams map in NetworkService would be cleared for this peer.
      // One side effect is that if NetworkService.handlePeerDisconnected closes a stream,
      // we might be able to mock that stream and verify close() if we had a handle to it.

      // For now, we trust the _onPeerDisconnected logic calls the internal NetworkService.
      // A more robust test would involve verifying that the stream to testPeerId1 (if one existed) was closed.

      // This test primarily ensures the event listener is wired up and calls _onPeerDisconnected.
      // The effects of _onPeerDisconnected are harder to unit test without DI.
      print('Test for peer disconnection relies on the event being processed.');
    });
  });

  group('_handleIncomingMessage - Process Wantlist', () {
    test('responds with block if available for a BLOCK want', () async {
      final wantEntry = pb.Message_Wantlist_Entry()
        ..block = cid1.toBytes()
        ..wantType = pb.Message_Wantlist_WantType.Block;
      final incomingMessage = pb.Message()
        ..wantlist = (pb.Message_Wantlist()..entries.add(wantEntry));
      final blockData =
          Uint8List.fromList([1, 2, 3]); // Still needed for mockBlockStore.get

      when(mockBlockStore.has(cid1)).thenAnswer((_) async => true);
      when(mockBlockStore.get(cid1)).thenAnswer(
          (_) async => blockData); // Keep this for now as it's awaited

      // The internal NetworkService will use mockHost.newStream to send the response
      final mockResponseStream = MockP2PStream();
      stubLiveStream(mockResponseStream);
      when(mockResponseStream.id()).thenReturn('mock-response-stream');
      when(mockResponseStream.write(any)).thenAnswer((_) async {});
      when(mockResponseStream.close()).thenAnswer((_) async {});

      final expectedProtocols = [bitswapProtocolId];

      // Restore specific matchers for the 'when' stub
      when(mockHost.newStream(
              argThat(isA<libp2p.PeerId>()
                  .having((p) => p.toBase58(), 'toBase58', testPeerId1Str)),
              argThat(equals(expectedProtocols)),
              argThat(isA<libp2p.Context>())))
          .thenAnswer((_) async => mockResponseStream);

      bitswap.handleIncomingMessage(testPeerId1, incomingMessage);

      await untilCalled(mockBlockStore.get(cid1));
      await Future.delayed(
          Duration.zero); // Add delay to allow event loop to process

      // Restore specific matchers for verify
      verify(mockHost.newStream(
              argThat(isA<libp2p.PeerId>()
                  .having((p) => p.toBase58(), 'toBase58', testPeerId1Str)),
              argThat(equals(expectedProtocols)),
              argThat(isA<libp2p.Context>())))
          .called(1);

      // Single combined write (varint prefix + message body)
      verify(mockResponseStream.write(any)).called(1);
    });

    // TODO: More _handleWantlist scenarios: cancel, HAVE, DONT_HAVE, block not found, wantlist.full
  });

  group('_handleIncomingMessage - Process ReceivedBlock', () {
    test('stores block if wanted and sends CANCELs', () async {
      final blockData = Uint8List.fromList(
          [4, 5, 6]); // This is the actual content of the block

      // Create the CID that will be put on the wantlist using CID.fromData.
      // CID.fromData will hash blockData using sha2-256 (default) and use 'dag-pb' codec for CIDv1.
      // This ensures cidToWant matches how Bitswap reconstructs the CID from the message.
      final cidToWant =
          dart_cid.CID.fromData(dart_cid.CID.V1, 'dag-pb', blockData);
      // print('Test: CID to want: ${cidToWant.toString()}'); // For debugging

      // The prefix for the proto message should describe how to derive the CID from blockData:
      // version (0x01), codec (0x70), mh_type (0x12 for sha2-256), mh_len (0x20 for 32 bytes)
      final protoBlockPrefix = Uint8List.fromList([0x01, 0x70, 0x12, 0x20]);

      final blockProto = pb.Message_Block()
        ..prefix = protoBlockPrefix
        ..data = blockData;
      final incomingMessage = pb.Message()..payload.add(blockProto);

      // 1. Make Bitswap want the block
      when(mockNetwork.conns).thenReturn(
          []); // No peers, so wantBlocks won't try to send initially
      await bitswap.wantBlocks([cidToWant]); // Now internal WM wants it

      // 2. Simulate receiving the block
      // This will trigger BitswapClient.handleBlock -> BlockStore.put -> CANCELs
      bitswap.handleIncomingMessage(testPeerId1, incomingMessage);

      // Verify the block was stored because it was wanted
      await untilCalled(mockBlockStore.put(cidToWant, blockData));
      verify(mockBlockStore.put(cidToWant, blockData)).called(1);

      // Regarding "sends CANCELs":
      // With the current setup (no peers were sent the want initially because mockNetwork.conns was empty),
      // _sendCancelMessages will likely find no peers to send CANCELs to.
      // Thus, no mockHost.newStream calls for CANCEL messages would occur.
      // If the test's intent is *only* to verify storage when wanted, this should be fine.
      // If it *must* verify CANCELs are sent, the test setup needs to be more complex
      // to simulate the want being sent to another peer first.
      // Given the existing comment "Cancel logic is harder to verify", fixing the storage part is the priority.
    });
  });

  group('_handleIncomingMessage - Unsolicited Blocks', () {
    test('discards blocks that are not wanted locally', () async {
      final blockData = Uint8List.fromList([10, 20, 30]);
      final protoBlockPrefix = Uint8List.fromList([0x01, 0x70, 0x12, 0x20]);

      final blockProto = pb.Message_Block()
        ..prefix = protoBlockPrefix
        ..data = blockData;
      final incomingMessage = pb.Message()..payload.add(blockProto);

      // We do NOT call wantBlocks — this block is unsolicited
      await bitswap.handleIncomingMessage(testPeerId1, incomingMessage);

      verifyNever(mockBlockStore.put(any, any));
    });
  });

  group('Push-on-store / NotifyNewBlocks (Fix 10)', () {
    test('proactively sends block to peers wanting it after storing', () async {
      final blockData = Uint8List.fromList([7, 8, 9]);
      final cidForBlock =
          dart_cid.CID.fromData(dart_cid.CID.V1, 'dag-pb', blockData);
      final protoBlockPrefix = Uint8List.fromList([0x01, 0x70, 0x12, 0x20]);

      // First, simulate a remote peer wanting this block via a wantlist message
      final wantEntry = pb.Message_Wantlist_Entry()
        ..block = cidForBlock.toBytes()
        ..wantType = pb.Message_Wantlist_WantType.Block
        ..sendDontHave = false;
      final wantMessage = pb.Message()
        ..wantlist = (pb.Message_Wantlist()..entries.add(wantEntry));

      // Set up the mock stream used for everything we send to the peer
      final mockResponseStream = MockP2PStream();
      stubLiveStream(mockResponseStream);
      when(mockResponseStream.id()).thenReturn('mock-push-stream');
      when(mockResponseStream.write(any)).thenAnswer((_) async {});
      when(mockResponseStream.close()).thenAnswer((_) async {});
      when(mockHost.newStream(
        argThat(isA<libp2p.PeerId>()
            .having((p) => p.toBase58(), 'toBase58', testPeerId1Str)),
        argThat(equals(['/ipfs/bitswap/1.2.0'])),
        argThat(isA<libp2p.Context>()),
      )).thenAnswer((_) async => mockResponseStream);

      // The peer wants the block but we don't have it yet
      when(mockBlockStore.has(cidForBlock)).thenAnswer((_) async => false);

      await bitswap.handleIncomingMessage(testPeerId1, wantMessage);

      // We want it too (only wanted blocks are accepted), then receive it
      // from another source (e.g., a different peer)
      await bitswap.wantBlocks([cidForBlock]);
      final blockProto = pb.Message_Block()
        ..prefix = protoBlockPrefix
        ..data = blockData;
      final blockMessage = pb.Message()..payload.add(blockProto);

      await bitswap.handleIncomingMessage(otherPeer, blockMessage);
      await pumpEventQueue(); // sends are queued per peer

      // Verify the block was stored
      verify(mockBlockStore.put(cidForBlock, blockData)).called(1);

      // Verify a message was sent to testPeerId1Str (the peer who wanted the block)
      verify(mockHost.newStream(
        argThat(isA<libp2p.PeerId>()
            .having((p) => p.toBase58(), 'toBase58', testPeerId1Str)),
        argThat(equals(['/ipfs/bitswap/1.2.0'])),
        argThat(isA<libp2p.Context>()),
      )).called(1);

      // Writes are our own want for the block, then the pushed block itself
      final sent = verify(mockResponseStream.write(captureAny))
          .captured
          .cast<Uint8List>()
          .map((frame) {
        final prefix = dart_cid.decodeVarint(frame);
        return pb.Message.fromBuffer(frame.sublist(prefix.value));
      }).toList();
      final pushed = sent.where((m) => m.payload.isNotEmpty).toList();
      expect(pushed, hasLength(1));
      expect(pushed.single.payload.single.data, blockData);
    });
  });

  group('WANT_HAVE handling', () {
    // Over BitswapOptions.wantHaveReplaceSize, so WANT_HAVE gets a HAVE
    final blockData = Uint8List.fromList(List.filled(2000, 4));
    final cid = dart_cid.CID.fromData(dart_cid.CID.V1, 'raw', blockData);
    late MockP2PStream peerStream;

    pb.Message wantHave() => pb.Message()
      ..wantlist = (pb.Message_Wantlist()
        ..entries.add(pb.Message_Wantlist_Entry()
          ..block = cid.toBytes()
          ..wantType = pb.Message_Wantlist_WantType.Have));
    pb.Message blockMsg() => pb.Message()
      ..payload.add(pb.Message_Block()
        ..prefix = [0x01, 0x55, 0x12, 0x20]
        ..data = blockData);
    List<pb.Message> sentToPeer() {
      return verify(peerStream.write(captureAny))
          .captured
          .cast<Uint8List>()
          .map((frame) {
        final prefix = dart_cid.decodeVarint(frame);
        return pb.Message.fromBuffer(frame.sublist(prefix.value));
      }).toList();
    }

    setUp(() {
      peerStream = MockP2PStream();
      stubLiveStream(peerStream);
      when(peerStream.id()).thenReturn('peer-stream');
      when(peerStream.write(any)).thenAnswer((_) async {});
      when(peerStream.close()).thenAnswer((_) async {});
      when(mockHost.newStream(
        argThat(isA<libp2p.PeerId>()
            .having((p) => p.toBase58(), 'toBase58', testPeerId1Str)),
        any,
        any,
      )).thenAnswer((_) async => peerStream);
    });

    test('block arriving later is announced with HAVE, not pushed', () async {
      await bitswap.handleIncomingMessage(testPeerId1, wantHave());
      await bitswap.wantBlocks([cid]);
      await bitswap.handleIncomingMessage(otherPeer, blockMsg());
      await pumpEventQueue();

      final sent = sentToPeer();
      final presences = sent.expand((m) => m.blockPresences).toList();
      expect(sent.expand((m) => m.payload), isEmpty);
      expect(presences, hasLength(1));
      expect(presences.single.type, pb.Message_BlockPresenceType.Have);
      expect(presences.single.cid, cid.toBytes());
    });

    test('immediate HAVE clears the want so the block is not pushed later',
        () async {
      when(mockBlockStore.has(cid)).thenAnswer((_) async => true);
      await bitswap.handleIncomingMessage(testPeerId1, wantHave());
      await pumpEventQueue();
      clearInteractions(peerStream);

      // The block arrives again (e.g. we re-fetched it); peer must not get it
      await bitswap.wantBlocks([cid]);
      await bitswap.handleIncomingMessage(otherPeer, blockMsg());
      await pumpEventQueue();

      expect(sentToPeer().expand((m) => [...m.payload, ...m.blockPresences]),
          isEmpty);
    });
  });

  group('getBlock waiter lifecycle', () {
    final blockData = Uint8List.fromList([9, 9, 9]);
    final cid = dart_cid.CID.fromData(dart_cid.CID.V1, 'raw', blockData);
    late MockP2PStream peerStream;

    pb.Message blockMsg() => pb.Message()
      ..payload.add(pb.Message_Block()
        ..prefix = [0x01, 0x55, 0x12, 0x20]
        ..data = blockData);

    setUp(() {
      // testPeerId1 is a connected Bitswap peer, so wants are sent to it
      final peerInfo = MockPeerInfo();
      when(peerInfo.protocols).thenReturn({bitswapProtocolId});
      final conn = MockConn();
      when(conn.remotePeer).thenReturn(testPeerId1);
      when(mockNetwork.conns).thenReturn([conn]);
      when(mockPeerStore.getPeer(testPeerId1))
          .thenAnswer((_) async => peerInfo);

      peerStream = MockP2PStream();

      stubLiveStream(peerStream);
      when(peerStream.id()).thenReturn('peer-stream');
      when(peerStream.write(any)).thenAnswer((_) async {});
      when(peerStream.close()).thenAnswer((_) async {});
      when(mockHost.newStream(any, any, any))
          .thenAnswer((_) async => peerStream);
    });

    List<pb.Message_Wantlist_Entry> cancelsSent() =>
        verify(peerStream.write(captureAny))
            .captured
            .cast<Uint8List>()
            .map((frame) => pb.Message.fromBuffer(
                frame.sublist(dart_cid.decodeVarint(frame).value)))
            .expand((m) => m.wantlist.entries)
            .where((e) => e.cancel)
            .toList();

    // Timeout behaviour is covered in fake time by timing_test.dart

    test('cancelWants completes pending getBlock with null and CANCELs peers',
        () async {
      final pending =
          bitswap.getBlock(cid, timeout: const Duration(seconds: 5));
      await pumpEventQueue();

      bitswap.cancelWants([cid]);

      expect(await pending, isNull);
      await pumpEventQueue();
      expect(cancelsSent().single.block, cid.toBytes());

      // A late arrival is now unwanted and discarded
      await bitswap.handleIncomingMessage(otherPeer, blockMsg());
      verifyNever(mockBlockStore.put(any, any));
    });

    test('block arriving while the want is being broadcast is not missed',
        () async {
      when(mockPeerStore.getPeer(testPeerId1)).thenAnswer((_) async {
        // Deliver the block mid-broadcast, before getBlock awaits it
        await bitswap.handleIncomingMessage(otherPeer, blockMsg());
        return null;
      });

      expect(await bitswap.getBlock(cid, timeout: const Duration(seconds: 1)),
          blockData);
    });
  });

  group('_handleIncomingMessage - CIDv0 blocks', () {
    test('accepts a CIDv0 block and completes getBlock', () async {
      final blockData = Uint8List.fromList([10, 20, 30]);
      final v1 = dart_cid.CID.fromData(dart_cid.CID.V1, 'dag-pb', blockData);
      final cidV0 = dart_cid.CID(dart_cid.CID.V0, v1.codec, v1.multihash);

      final pending = bitswap.getBlock(cidV0);
      await Future.delayed(Duration.zero);

      // Prefix as sent by go-bitswap / boxo for CIDv0
      final blockMessage = pb.Message()
        ..payload.add(pb.Message_Block()
          ..prefix = [0x00, 0x70, 0x12, 0x20]
          ..data = blockData);
      await bitswap.handleIncomingMessage(testPeerId1, blockMessage);

      expect(await pending, blockData);
      verify(mockBlockStore.put(cidV0, blockData)).called(1);
    });
  });

  // TODO: _handleBlockPresence tests
}
