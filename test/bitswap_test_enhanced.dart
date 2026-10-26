import 'dart:async';
import 'dart:typed_data';

import 'package:dart_libp2p/dart_libp2p.dart'; // Avoid conflict if dart_libp2p exports them
// Assuming YamuxStream and related components are here or re-exported by a top-level yamux.dart
// Adjust these paths if necessary based on your dart_libp2p package structure
import 'package:dart_libp2p/p2p/transport/multiplexing/yamux/frame.dart';
import 'package:dart_libp2p/p2p/transport/multiplexing/yamux/stream.dart';

import 'package:dart_libp2p_bitswap/src/bitswap.dart';
import 'package:dcid/dcid.dart' as dart_cid;
import 'package:dart_libp2p/dart_libp2p.dart' as libp2p;
import 'package:test/test.dart';
import 'package:mockito/mockito.dart';

// Import the existing mocks file. Ensure @GenerateMocks in bitswap_test.dart covers all needed mocks.
// If not, this new file might need its own @GenerateMocks and build_runner execution.
// For now, assuming bitswap_test.mocks.dart is sufficient and accessible.
import 'bitswap_test.mocks.dart';
import 'test_helpers.dart';

// If PeerInfo or TestEventSubscription are not in bitswap_test.mocks.dart because they were
// part of a @GenerateMocks list specific to bitswap_test.dart's main(),
// we might need to redefine them or ensure they are generated globally.
// For now, assuming they are available via the import.

void main() {
  final bitswapProtocolId = '/ipfs/bitswap/1.2.0';

  group('Two-Party WANT-BLOCK Exchange (Mocked Network) - Enhanced Test', () {
    late MockHost mockHostA, mockHostB;
    late PeerId peerIdA, peerIdB; // Changed from MockPeerId
    late MockBlockStore mockBlockStoreA, mockBlockStoreB;
    late MockNetwork mockNetworkA, mockNetworkB;
    late MockPeerstore mockPeerStoreA, mockPeerStoreB;
    late StreamController<libp2p.EvtPeerConnectednessChanged> eventControllerA,
        eventControllerB;
    late Bitswap bitswapA, bitswapB;

    late dart_cid.CID targetCid;
    late Uint8List targetBlockData;

    libp2p.StreamHandler? streamHandlerA, streamHandlerB;
    // MockP2PStream? capturedOutgoingStreamBtoA, capturedOutgoingStreamAtoB; // Replaced by YamuxStreams
    // late List<Uint8List> dataWrittenToBtoA; // Data flow handled by YamuxStreams
    // late List<Uint8List> dataWrittenToAtoB;

    // Yamux stream pairs
    late YamuxStream bToAClientEnd, bToAServerEnd;
    late YamuxStream aToBClientEnd, aToBServerEnd;
    late StreamController<YamuxFrame>
        framesBToAOut; // Frames from bToAClientEnd.sendFrame
    late StreamController<YamuxFrame>
        framesBToAIn; // Frames from bToAServerEnd.sendFrame (e.g. window updates)
    late StreamController<YamuxFrame>
        framesAToBOut; // Frames from aToBClientEnd.sendFrame
    late StreamController<YamuxFrame>
        framesAToBIn; // Frames from aToBServerEnd.sendFrame

    setUp(() async {
      // dataWrittenToBtoA = []; // Not used directly anymore
      // dataWrittenToAtoB = []; // Not used directly anymore
      streamHandlerA = null;
      streamHandlerB = null;

      framesBToAOut = StreamController<YamuxFrame>.broadcast();
      framesBToAIn = StreamController<YamuxFrame>.broadcast();
      framesAToBOut = StreamController<YamuxFrame>.broadcast();
      framesAToBIn = StreamController<YamuxFrame>.broadcast();

      // Create real PeerId instances
      // SHA256 hash (0x12) of length 32 (0x20)
      final peerAIdBytes =
          Uint8List.fromList([0x12, 0x20] + List.generate(32, (i) => 1));
      peerIdA = PeerId.fromBytes(peerAIdBytes);
      final peerBIdBytes =
          Uint8List.fromList([0x12, 0x20] + List.generate(32, (i) => 2));
      peerIdB = PeerId.fromBytes(peerBIdBytes);

      // Node A setup
      mockHostA = MockHost();
      mockBlockStoreA = MockBlockStore();
      mockNetworkA = MockNetwork();
      mockPeerStoreA = MockPeerstore();
      eventControllerA =
          StreamController<libp2p.EvtPeerConnectednessChanged>.broadcast();

      // when(mockPeerIdA.toBase58()).thenReturn("mockPeerIdA_Enhanced_Str"); // Removed
      when(mockHostA.id).thenReturn(peerIdA);
      final mockEventBusA = MockEventBus();
      when(mockHostA.eventBus).thenReturn(mockEventBusA);
      when(mockHostA.network).thenReturn(mockNetworkA);
      when(mockHostA.peerStore).thenReturn(mockPeerStoreA);

      // mockHostA.newStream will be set up later with YamuxStream
      when(mockHostA.setStreamHandler(bitswapProtocolId, any))
          .thenAnswer((invocation) {
        streamHandlerA =
            invocation.positionalArguments[1] as libp2p.StreamHandler;
      });

      // Node B setup
      mockHostB = MockHost();
      mockBlockStoreB = MockBlockStore();
      mockNetworkB = MockNetwork();
      mockPeerStoreB = MockPeerstore();
      eventControllerB =
          StreamController<libp2p.EvtPeerConnectednessChanged>.broadcast();

      // when(mockPeerIdB.toBase58()).thenReturn("mockPeerIdB_Enhanced_Str"); // Removed
      when(mockHostB.id).thenReturn(peerIdB);
      final mockEventBusB = MockEventBus();
      when(mockHostB.eventBus).thenReturn(mockEventBusB);
      when(mockHostB.network).thenReturn(mockNetworkB);
      when(mockHostB.peerStore).thenReturn(mockPeerStoreB);

      // mockHostB.newStream will be set up later with YamuxStream
      when(mockHostB.setStreamHandler(bitswapProtocolId, any))
          .thenAnswer((invocation) {
        streamHandlerB =
            invocation.positionalArguments[1] as libp2p.StreamHandler;
      });

      // Setup Yamux Stream Pairs
      final mockConnBClient = MockConn();
      when(mockConnBClient.localPeer).thenReturn(peerIdB);
      when(mockConnBClient.remotePeer).thenReturn(peerIdA);
      when(mockConnBClient.isClosed).thenReturn(false);

      final mockConnBServer = MockConn(); // A's view of the connection from B
      when(mockConnBServer.localPeer).thenReturn(peerIdA);
      when(mockConnBServer.remotePeer).thenReturn(peerIdB);
      when(mockConnBServer.isClosed).thenReturn(false);

      bToAClientEnd = YamuxStream(
          id: 1,
          protocol: bitswapProtocolId,
          metadata: {},
          initialWindowSize: 256 * 1024,
          maxFrameSize: 16 * 1024,
          sendFrame: (frame) async => framesBToAOut.add(frame),
          parentConn: mockConnBClient,
          logPrefix: "TestStream_BtoA_ClientEnd(1)");
      bToAServerEnd = YamuxStream(
          id: 1,
          protocol: bitswapProtocolId,
          metadata: {},
          initialWindowSize: 256 * 1024,
          maxFrameSize: 16 * 1024,
          sendFrame: (frame) async => framesBToAIn.add(frame),
          parentConn: mockConnBServer,
          logPrefix: "TestStream_BtoA_ServerEnd(1)");

      framesBToAOut.stream.listen((frame) => bToAServerEnd.handleFrame(frame),
          onError: (e) => print("ERROR in framesBToAOut listener: $e"));
      framesBToAIn.stream.listen((frame) => bToAClientEnd.handleFrame(frame),
          onError: (e) => print("ERROR in framesBToAIn listener: $e"));

      final mockConnAClient = MockConn();
      when(mockConnAClient.localPeer).thenReturn(peerIdA);
      when(mockConnAClient.remotePeer).thenReturn(peerIdB);
      when(mockConnAClient.isClosed).thenReturn(false);

      final mockConnAServer = MockConn(); // B's view of the connection from A
      when(mockConnAServer.localPeer).thenReturn(peerIdB);
      when(mockConnAServer.remotePeer).thenReturn(peerIdA);
      when(mockConnAServer.isClosed).thenReturn(false);

      aToBClientEnd = YamuxStream(
          id: 2,
          protocol: bitswapProtocolId,
          metadata: {},
          initialWindowSize: 256 * 1024,
          maxFrameSize: 16 * 1024,
          sendFrame: (frame) async => framesAToBOut.add(frame),
          parentConn: mockConnAClient,
          logPrefix: "TestStream_AtoB_ClientEnd(2)");
      aToBServerEnd = YamuxStream(
          id: 2,
          protocol: bitswapProtocolId,
          metadata: {},
          initialWindowSize: 256 * 1024,
          maxFrameSize: 16 * 1024,
          sendFrame: (frame) async => framesAToBIn.add(frame),
          parentConn: mockConnAServer,
          logPrefix: "TestStream_AtoB_ServerEnd(2)");

      framesAToBOut.stream.listen((frame) => aToBServerEnd.handleFrame(frame),
          onError: (e) => print("ERROR in framesAToBOut listener: $e"));
      framesAToBIn.stream.listen((frame) => aToBClientEnd.handleFrame(frame),
          onError: (e) => print("ERROR in framesAToBIn listener: $e"));

      // Configure MockHost newStream calls
      when(mockHostB.newStream(peerIdA, [bitswapProtocolId], any))
          .thenAnswer((_) async {
        await bToAClientEnd.open();
        return bToAClientEnd;
      });
      when(mockHostA.newStream(peerIdB, [bitswapProtocolId], any))
          .thenAnswer((_) async {
        await aToBClientEnd.open();
        return aToBClientEnd;
      });

      targetBlockData = Uint8List.fromList([1, 2, 3, 4, 5, 6, 7, 8, 9, 0]);
      targetCid =
          dart_cid.CID.fromData(dart_cid.CID.V1, 'dag-pb', targetBlockData);

      when(mockBlockStoreA.has(targetCid)).thenAnswer((_) async => true);
      when(mockBlockStoreA.get(targetCid))
          .thenAnswer((_) async => targetBlockData);
      when(mockBlockStoreA.put(any, any)).thenAnswer((_) async {});

      when(mockBlockStoreB.has(targetCid)).thenAnswer((_) async => false);
      when(mockBlockStoreB.get(targetCid)).thenAnswer((_) async => null);
      when(mockBlockStoreB.put(any, any)).thenAnswer((_) async {});

      final mockConnBtoA = MockConn();
      when(mockConnBtoA.remotePeer).thenReturn(peerIdA); // Use real PeerId
      when(mockNetworkB.conns).thenReturn([mockConnBtoA]);
      final mockPeerInfoA = MockPeerInfo();
      when(mockPeerInfoA.protocols).thenReturn({bitswapProtocolId});
      when(mockPeerStoreB.getPeer(peerIdA))
          .thenAnswer((_) async => mockPeerInfoA); // Use real PeerId

      final mockConnAtoB = MockConn();
      when(mockConnAtoB.remotePeer).thenReturn(peerIdB); // Use real PeerId
      when(mockNetworkA.conns).thenReturn([mockConnAtoB]);
      final mockPeerInfoB = MockPeerInfo();
      when(mockPeerInfoB.protocols).thenReturn({bitswapProtocolId});
      when(mockPeerStoreA.getPeer(peerIdB))
          .thenAnswer((_) async => mockPeerInfoB); // Use real PeerId

      stubPeerEvents(mockEventBusA, eventControllerA.stream);
      bitswapA = Bitswap(host: mockHostA, blockStore: mockBlockStoreA);
      await bitswapA.start();
      stubPeerEvents(mockEventBusB, eventControllerB.stream);
      bitswapB = Bitswap(host: mockHostB, blockStore: mockBlockStoreB);
      await bitswapB.start();

      await Future.delayed(Duration.zero);
      expect(streamHandlerA, isNotNull,
          reason:
              "Stream handler for Node A should be set by Bitswap constructor");
      expect(streamHandlerB, isNotNull,
          reason:
              "Stream handler for Node B should be set by Bitswap constructor");
    });

    tearDown(() async {
      await bitswapA.stop();
      await bitswapB.stop();
      await eventControllerA.close();
      await eventControllerB.close();
      await framesBToAOut.close();
      await framesBToAIn.close();
      await framesAToBOut.close();
      await framesAToBIn.close();

      // Ensure YamuxStreams are closed if they weren't already by the test logic
      if (!bToAClientEnd.isClosed) await bToAClientEnd.close();
      if (!bToAServerEnd.isClosed) await bToAServerEnd.close();
      if (!aToBClientEnd.isClosed) await aToBClientEnd.close();
      if (!aToBServerEnd.isClosed) await aToBServerEnd.close();
    });

    test(
        'Node B requests block from Node A, Node A sends it, Node B receives it',
        () async {
      final getBlockFuture = bitswapB.getBlock(targetCid);

      // Allow BitswapB to initiate the stream and send the WANT message
      await Future.delayed(Duration.zero);

      verify(mockHostB.newStream(peerIdA, [bitswapProtocolId], any)).called(1);

      // Simulate Node A's host invoking its Bitswap handler for the incoming stream from B
      expect(streamHandlerA, isNotNull,
          reason: "Stream handler for Node A must be captured.");
      // The stream B opened to A is bToAClientEnd, its other end is bToAServerEnd.
      // A's Bitswap handler (onIncomingStream) should be called with bToAServerEnd.
      // No explicit open needed here; handleFrame on bToAServerEnd should open it upon receiving data.
      // The handler reads until the stream closes (streams are long-lived), so
      // run it in the background as a real host would rather than awaiting it.
      await waitUntilOpen(bToAServerEnd);
      unawaited(streamHandlerA!(bToAServerEnd, peerIdB));

      // Wait for BitswapA to process the WANT and open a stream back to B
      await untilCalled(mockHostA.newStream(peerIdB, [bitswapProtocolId], any))
          .timeout(const Duration(seconds: 5));

      verify(mockHostA.newStream(peerIdB, [bitswapProtocolId], any)).called(1);

      // Simulate Node B's host invoking its Bitswap handler for the incoming stream from A
      expect(streamHandlerB, isNotNull,
          reason: "Stream handler for Node B must be captured.");
      // The stream A opened to B is aToBClientEnd, its other end is aToBServerEnd.
      // B's Bitswap handler should be called with aToBServerEnd.
      // No explicit open needed here; handleFrame on aToBServerEnd should open it upon receiving data.
      await waitUntilOpen(aToBServerEnd);
      unawaited(streamHandlerB!(aToBServerEnd, peerIdA));

      final receivedBlockOnB =
          await getBlockFuture.timeout(Duration(seconds: 5), onTimeout: () {
        // Increased timeout
        fail("getBlockFuture timed out with YamuxStreams");
        // return null; // Not needed due to fail
      });
      expect(receivedBlockOnB, isNotNull);
      expect(receivedBlockOnB, equals(targetBlockData));

      verify(mockBlockStoreB.put(targetCid, targetBlockData)).called(1);
    });
  });
}

/// A real host only dispatches an inbound stream to its handler once the
/// remote's SYN has opened it; wait for that before invoking the handler.
Future<void> waitUntilOpen(YamuxStream stream) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (stream.streamState != YamuxStreamState.open) {
    if (DateTime.now().isAfter(deadline)) {
      fail('Stream ${stream.id()} did not open (state: ${stream.streamState})');
    }
    await Future.delayed(const Duration(milliseconds: 1));
  }
}
