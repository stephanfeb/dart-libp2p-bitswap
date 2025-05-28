import 'dart:async';
import 'dart:typed_data';

import 'package:dart_libp2p/dart_libp2p.dart'; // Avoid conflict if dart_libp2p exports them
import 'package:dart_libp2p/p2p/discovery/peer_info.dart';
// Assuming YamuxStream and related components are here or re-exported by a top-level yamux.dart
// Adjust these paths if necessary based on your dart_libp2p package structure
import 'package:dart_libp2p/core/network/conn.dart';
import 'package:dart_libp2p/p2p/transport/multiplexing/yamux/frame.dart';
import 'package:dart_libp2p/p2p/transport/multiplexing/yamux/stream.dart';


import 'package:dart_libp2p_bitswap/src/bitswap.dart';
import 'package:dart_libp2p_bitswap/src/proto/bitswap_message.pb.dart' as pb;
import 'package:dart_cid/dart_cid.dart' as dart_cid;
import 'package:dart_libp2p/dart_libp2p.dart' as libp2p;
import 'package:test/test.dart';
import 'package:mockito/mockito.dart';

// Import the existing mocks file. Ensure @GenerateMocks in bitswap_test.dart covers all needed mocks.
// If not, this new file might need its own @GenerateMocks and build_runner execution.
// For now, assuming bitswap_test.mocks.dart is sufficient and accessible.
import 'bitswap_test.mocks.dart'; 

// If PeerInfo or TestEventSubscription are not in bitswap_test.mocks.dart because they were
// part of a @GenerateMocks list specific to bitswap_test.dart's main(),
// we might need to redefine them or ensure they are generated globally.
// For now, assuming they are available via the import.

void main() {
  final bitswapProtocolId = '/ipfs/bitswap/1.2.0';

  group('Two-Party WANT-BLOCK Exchange (Mocked Network) - Enhanced Test', () {
    late MockHost mockHostA, mockHostB;
    late PeerId peerIdA, peerIdB; // Changed from MockPeerId
    late MockIBlockStore mockBlockStoreA, mockBlockStoreB;
    late MockNetwork mockNetworkA, mockNetworkB;
    late MockPeerstore mockPeerStoreA, mockPeerStoreB;
    late StreamController<libp2p.EvtPeerConnectednessChanged> eventControllerA, eventControllerB;
    late Bitswap bitswapA, bitswapB;

    late dart_cid.CID targetCid;
    late Uint8List targetBlockData;

    libp2p.StreamHandler? streamHandlerA, streamHandlerB;
    // MockP2PStream? capturedOutgoingStreamBtoA, capturedOutgoingStreamAtoB; // Replaced by YamuxStreams
    // late List<Uint8List> dataWrittenToBtoA; // Data flow handled by YamuxStreams
    // late List<Uint8List> dataWrittenToAtoB;

    // Yamux stream pairs
    late YamuxStream b_to_a_clientEnd, b_to_a_serverEnd;
    late YamuxStream a_to_b_clientEnd, a_to_b_serverEnd;
    late StreamController<YamuxFrame> frames_b_to_a_out_controller; // Frames from b_to_a_clientEnd.sendFrame
    late StreamController<YamuxFrame> frames_b_to_a_in_controller;  // Frames from b_to_a_serverEnd.sendFrame (e.g. window updates)
    late StreamController<YamuxFrame> frames_a_to_b_out_controller; // Frames from a_to_b_clientEnd.sendFrame
    late StreamController<YamuxFrame> frames_a_to_b_in_controller;  // Frames from a_to_b_serverEnd.sendFrame

    setUp(() async {
      // dataWrittenToBtoA = []; // Not used directly anymore
      // dataWrittenToAtoB = []; // Not used directly anymore
      streamHandlerA = null;
      streamHandlerB = null;

      frames_b_to_a_out_controller = StreamController<YamuxFrame>.broadcast();
      frames_b_to_a_in_controller = StreamController<YamuxFrame>.broadcast();
      frames_a_to_b_out_controller = StreamController<YamuxFrame>.broadcast();
      frames_a_to_b_in_controller = StreamController<YamuxFrame>.broadcast();

      // Create real PeerId instances
      // SHA256 hash (0x12) of length 32 (0x20)
      final peerAIdBytes = Uint8List.fromList([0x12, 0x20] + List.generate(32, (i) => 1));
      peerIdA = PeerId.fromBytes(peerAIdBytes);
      final peerBIdBytes = Uint8List.fromList([0x12, 0x20] + List.generate(32, (i) => 2));
      peerIdB = PeerId.fromBytes(peerBIdBytes);

      // Node A setup
      mockHostA = MockHost();
      mockBlockStoreA = MockIBlockStore();
      mockNetworkA = MockNetwork();
      mockPeerStoreA = MockPeerstore();
      eventControllerA = StreamController<libp2p.EvtPeerConnectednessChanged>.broadcast();

      // when(mockPeerIdA.toBase58()).thenReturn("mockPeerIdA_Enhanced_Str"); // Removed
      when(mockHostA.id).thenReturn(peerIdA);
      final mockEventBusA = MockEventBus();
      when(mockHostA.eventBus).thenReturn(mockEventBusA);
      when(mockHostA.network).thenReturn(mockNetworkA);
      when(mockHostA.peerStore).thenReturn(mockPeerStoreA);
      
      // mockHostA.newStream will be set up later with YamuxStream
       when(mockHostA.setStreamHandler(bitswapProtocolId, any)).thenAnswer((invocation) {
        streamHandlerA = invocation.positionalArguments[1] as libp2p.StreamHandler;
      });

      // Node B setup
      mockHostB = MockHost();
      mockBlockStoreB = MockIBlockStore();
      mockNetworkB = MockNetwork();
      mockPeerStoreB = MockPeerstore();
      eventControllerB = StreamController<libp2p.EvtPeerConnectednessChanged>.broadcast();

      // when(mockPeerIdB.toBase58()).thenReturn("mockPeerIdB_Enhanced_Str"); // Removed
      when(mockHostB.id).thenReturn(peerIdB);
      final mockEventBusB = MockEventBus();
      when(mockHostB.eventBus).thenReturn(mockEventBusB);
      when(mockHostB.network).thenReturn(mockNetworkB);
      when(mockHostB.peerStore).thenReturn(mockPeerStoreB);

      // mockHostB.newStream will be set up later with YamuxStream
      when(mockHostB.setStreamHandler(bitswapProtocolId, any)).thenAnswer((invocation) {
        streamHandlerB = invocation.positionalArguments[1] as libp2p.StreamHandler;
      });

      // Setup Yamux Stream Pairs
      final mockConn_B_client = MockConn();
      when(mockConn_B_client.localPeer).thenReturn(peerIdB);
      when(mockConn_B_client.remotePeer).thenReturn(peerIdA);
      when(mockConn_B_client.isClosed).thenReturn(false);

      final mockConn_B_server = MockConn(); // A's view of the connection from B
      when(mockConn_B_server.localPeer).thenReturn(peerIdA);
      when(mockConn_B_server.remotePeer).thenReturn(peerIdB);
      when(mockConn_B_server.isClosed).thenReturn(false);
      
      b_to_a_clientEnd = YamuxStream(
          id: 1, protocol: bitswapProtocolId, metadata: {}, initialWindowSize: 256 * 1024,
          sendFrame: (frame) async => frames_b_to_a_out_controller.add(frame),
          parentConn: mockConn_B_client, logPrefix: "TestStream_BtoA_ClientEnd(1)");
      b_to_a_serverEnd = YamuxStream(
          id: 1, protocol: bitswapProtocolId, metadata: {}, initialWindowSize: 256 * 1024,
          sendFrame: (frame) async => frames_b_to_a_in_controller.add(frame),
          parentConn: mockConn_B_server, logPrefix: "TestStream_BtoA_ServerEnd(1)");

      frames_b_to_a_out_controller.stream.listen((frame) => b_to_a_serverEnd.handleFrame(frame), 
        onError: (e) => print("ERROR in frames_b_to_a_out_controller listener: $e"));
      frames_b_to_a_in_controller.stream.listen((frame) => b_to_a_clientEnd.handleFrame(frame),
        onError: (e) => print("ERROR in frames_b_to_a_in_controller listener: $e"));

      final mockConn_A_client = MockConn();
      when(mockConn_A_client.localPeer).thenReturn(peerIdA);
      when(mockConn_A_client.remotePeer).thenReturn(peerIdB);
      when(mockConn_A_client.isClosed).thenReturn(false);

      final mockConn_A_server = MockConn(); // B's view of the connection from A
      when(mockConn_A_server.localPeer).thenReturn(peerIdB);
      when(mockConn_A_server.remotePeer).thenReturn(peerIdA);
      when(mockConn_A_server.isClosed).thenReturn(false);

      a_to_b_clientEnd = YamuxStream(
          id: 2, protocol: bitswapProtocolId, metadata: {}, initialWindowSize: 256 * 1024,
          sendFrame: (frame) async => frames_a_to_b_out_controller.add(frame),
          parentConn: mockConn_A_client, logPrefix: "TestStream_AtoB_ClientEnd(2)");
      a_to_b_serverEnd = YamuxStream(
          id: 2, protocol: bitswapProtocolId, metadata: {}, initialWindowSize: 256 * 1024,
          sendFrame: (frame) async => frames_a_to_b_in_controller.add(frame),
          parentConn: mockConn_A_server, logPrefix: "TestStream_AtoB_ServerEnd(2)");
      
      frames_a_to_b_out_controller.stream.listen((frame) => a_to_b_serverEnd.handleFrame(frame),
        onError: (e) => print("ERROR in frames_a_to_b_out_controller listener: $e"));
      frames_a_to_b_in_controller.stream.listen((frame) => a_to_b_clientEnd.handleFrame(frame),
        onError: (e) => print("ERROR in frames_a_to_b_in_controller listener: $e"));

      // Configure MockHost newStream calls
      when(mockHostB.newStream(peerIdA, [bitswapProtocolId], any)).thenAnswer((_) async {
        await b_to_a_clientEnd.open();
        return b_to_a_clientEnd;
      });
      when(mockHostA.newStream(peerIdB, [bitswapProtocolId], any)).thenAnswer((_) async {
        await a_to_b_clientEnd.open();
        return a_to_b_clientEnd;
      });
      
      targetBlockData = Uint8List.fromList([1, 2, 3, 4, 5, 6, 7, 8, 9, 0]);
      targetCid = dart_cid.CID.fromData(dart_cid.CID.V1, 'dag-pb', targetBlockData);

      when(mockBlockStoreA.has(targetCid)).thenAnswer((_) async => true);
      when(mockBlockStoreA.get(targetCid)).thenAnswer((_) async => targetBlockData);
      when(mockBlockStoreA.put(any, any)).thenAnswer((_) async {});

      when(mockBlockStoreB.has(targetCid)).thenAnswer((_) async => false);
      when(mockBlockStoreB.get(targetCid)).thenAnswer((_) async => null);
      when(mockBlockStoreB.put(any, any)).thenAnswer((_) async {});

      final mockConnBtoA = MockConn();
      when(mockConnBtoA.remotePeer).thenReturn(peerIdA); // Use real PeerId
      when(mockNetworkB.conns).thenReturn([mockConnBtoA]);
      final mockPeerInfoA = MockPeerInfo();
      when(mockPeerInfoA.protocols).thenReturn({bitswapProtocolId});
      when(mockPeerStoreB.getPeer(peerIdA)).thenAnswer((_) async => mockPeerInfoA); // Use real PeerId
      
      final mockConnAtoB = MockConn();
      when(mockConnAtoB.remotePeer).thenReturn(peerIdB); // Use real PeerId
      when(mockNetworkA.conns).thenReturn([mockConnAtoB]);
      final mockPeerInfoB = MockPeerInfo();
      when(mockPeerInfoB.protocols).thenReturn({bitswapProtocolId});
      when(mockPeerStoreA.getPeer(peerIdB)).thenAnswer((_) async => mockPeerInfoB); // Use real PeerId

      bitswapA = Bitswap(
        host: mockHostA,
        blockStore: mockBlockStoreA,
        peerEventsStream: eventControllerA.stream,
      );
      bitswapB = Bitswap(
        host: mockHostB,
        blockStore: mockBlockStoreB,
        peerEventsStream: eventControllerB.stream,
      );
      
      await Future.delayed(Duration.zero); 
      expect(streamHandlerA, isNotNull, reason: "Stream handler for Node A should be set by Bitswap constructor");
      expect(streamHandlerB, isNotNull, reason: "Stream handler for Node B should be set by Bitswap constructor");
    });

    tearDown(() async {
      await eventControllerA.close();
      await eventControllerB.close();
      await frames_b_to_a_out_controller.close();
      await frames_b_to_a_in_controller.close();
      await frames_a_to_b_out_controller.close();
      await frames_a_to_b_in_controller.close();
      
      // Ensure YamuxStreams are closed if they weren't already by the test logic
      if (!b_to_a_clientEnd.isClosed) await b_to_a_clientEnd.close();
      if (!b_to_a_serverEnd.isClosed) await b_to_a_serverEnd.close();
      if (!a_to_b_clientEnd.isClosed) await a_to_b_clientEnd.close();
      if (!a_to_b_serverEnd.isClosed) await a_to_b_serverEnd.close();
    });

    test('Node B requests block from Node A, Node A sends it, Node B receives it', () async {
      final getBlockFuture = bitswapB.getBlock(targetCid);

      // Allow BitswapB to initiate the stream and send the WANT message
      await Future.delayed(Duration.zero); 
      
      verify(mockHostB.newStream(
        peerIdA,
        [bitswapProtocolId],
        any
      )).called(1);

      // Simulate Node A's host invoking its Bitswap handler for the incoming stream from B
      expect(streamHandlerA, isNotNull, reason: "Stream handler for Node A must be captured.");
      // The stream B opened to A is b_to_a_clientEnd, its other end is b_to_a_serverEnd.
      // A's Bitswap handler (onIncomingStream) should be called with b_to_a_serverEnd.
      if (!b_to_a_serverEnd.isClosed) await b_to_a_serverEnd.open(); // Ensure server end is open
      await streamHandlerA!(b_to_a_serverEnd, peerIdB);
      
      // Allow BitswapA to process WANT, initiate stream to B, and send BLOCK
      await Future.delayed(Duration.zero);

      verify(mockHostA.newStream(
        peerIdB,
        [bitswapProtocolId],
        any
      )).called(1);

      // Simulate Node B's host invoking its Bitswap handler for the incoming stream from A
      expect(streamHandlerB, isNotNull, reason: "Stream handler for Node B must be captured.");
      // The stream A opened to B is a_to_b_clientEnd, its other end is a_to_b_serverEnd.
      // B's Bitswap handler should be called with a_to_b_serverEnd.
      if (!a_to_b_serverEnd.isClosed) await a_to_b_serverEnd.open(); // Ensure server end is open
      await streamHandlerB!(a_to_b_serverEnd, peerIdA);
      
      final receivedBlockOnB = await getBlockFuture.timeout(Duration(seconds: 5), onTimeout: () { // Increased timeout
        fail("getBlockFuture timed out with YamuxStreams");
        // return null; // Not needed due to fail
      });
      expect(receivedBlockOnB, isNotNull);
      expect(receivedBlockOnB, equals(targetBlockData));

      verify(mockBlockStoreB.put(targetCid, targetBlockData)).called(1);
    });
  });
}
