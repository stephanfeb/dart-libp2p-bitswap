import 'dart:async';
import 'dart:typed_data';

import 'package:dart_libp2p_bitswap/src/network_service.dart';
import 'package:dart_libp2p_bitswap/src/proto/bitswap_message.pb.dart' as pb;
import 'package:dart_cid/dart_cid.dart' as dart_cid;
import 'package:dart_libp2p/dart_libp2p.dart' as libp2p;
import 'package:test/test.dart';
import 'package:mockito/mockito.dart';
import 'package:mockito/annotations.dart';

// Import the generated mocks
import 'network_service_test.mocks.dart';

// Annotation to generate mocks
@GenerateMocks([
  libp2p.Host,
  libp2p.P2PStream,
  libp2p.PeerId,
  // libp2p.Connection, // If needed for Host interactions
  // libp2p.StreamHandler, // If we need to mock/verify the handler function itself
  // libp2p.Context, // If specific context interactions are needed
])
void main() {
  late MockHost mockHost;
  late BitswapNetworkService networkService;
  late BitswapMessageHandler mockMessageHandler;
  
  // A list to capture messages passed to the handler
  final List<Map<String, dynamic>> receivedMessagesLog = [];

  setUp(() {
    mockHost = MockHost();
    receivedMessagesLog.clear();
    mockMessageHandler = (String peerId, pb.Message message) {
      receivedMessagesLog.add({'peerId': peerId, 'message': message});
    };
    networkService = BitswapNetworkService(mockHost, mockMessageHandler);

    // Stub for PeerId.fromString if it's used internally by the service
    // This might not be necessary if PeerId objects are directly passed or constructed differently.
    // However, sendMessage uses PeerId.fromString.
    // Mockito cannot mock static methods directly. If PeerId.fromString is problematic,
    // it might need a wrapper or a different approach for testing.
    // For now, we assume it works or we pass constructed MockPeerId where needed.
  });

  group('BitswapNetworkService Tests', () {
    final testPeerIdStr = 'QmTestPeer123';
    late MockPeerId mockPeerId;
    late MockP2PStream mockStream;

    setUp(() {
      mockPeerId = MockPeerId();
      when(mockPeerId.toString()).thenReturn(testPeerIdStr);
      // It's important that PeerId.fromString(testPeerIdStr) would return something compatible
      // For tests involving newStream, we'll ensure the mockHost returns our mockStream.

      mockStream = MockP2PStream();
      when(mockStream.id()).thenReturn('testStreamId'); // Changed to method call
      when(mockStream.protocol()).thenReturn(networkService.protocolId); // Changed to method call
      when(mockStream.write(any)).thenAnswer((_) async => {}); // Default success for writes
      when(mockStream.close()).thenAnswer((_) async => {});    // Default success for close
    });

    test('start() registers stream handler', () async {
      await networkService.start();
      verify(mockHost.setStreamHandler(networkService.protocolId, any)).called(1);
    });

    test('stop() removes stream handler and closes active streams', () async {
      // Simulate an active stream
      final peerIdForStream = 'QmAnotherPeer';
      final mockPeerIdForStream = MockPeerId();
      when(mockPeerIdForStream.toString()).thenReturn(peerIdForStream);
      
      // Manually add a stream to simulate it being active (since newStream is complex to fully mock here for this specific test)
      // This requires _activeStreams to be non-final or a way to inject it, or test via sendMessage then stop.
      // For simplicity, let's test via sendMessage then stop.
      
      when(mockHost.newStream(any, any, any)).thenAnswer((_) async => mockStream);
      
      final message = pb.Message(); // Empty message
      await networkService.sendMessage(peerIdForStream, message); // This will add mockStream to _activeStreams

      await networkService.stop();
      
      verify(mockHost.removeStreamHandler(networkService.protocolId)).called(1);
      verify(mockStream.close()).called(1); // Verify the active stream was closed
      expect(networkService.getActiveStreamsCount(), 0); // Helper method needed or check internal state
    });

    group('sendMessage()', () {
      final testMessage = pb.Message()..pendingBytes = 123;

      test('opens a new stream if none exists and sends data', () async {
        when(mockHost.newStream(any, [networkService.protocolId], any)).thenAnswer((_) async => mockStream);
        
        await networkService.sendMessage(testPeerIdStr, testMessage);

        verify(mockHost.newStream(any, [networkService.protocolId], any)).called(1);
        
        final messageBytes = testMessage.writeToBuffer();
        final lengthPrefix = dart_cid.encodeVarint(messageBytes.length);
        
        verify(mockStream.write(lengthPrefix)).called(1);
        verify(mockStream.write(messageBytes)).called(1);
        // verify(mockStream.close()).called(1); // Stream is NOT closed for reuse
        expect(networkService.getActiveStreamsCount(), 1);
      });

      test('reuses an existing stream', () async {
        when(mockHost.newStream(any, any, any)).thenAnswer((_) async => mockStream);
        
        // First send opens the stream
        await networkService.sendMessage(testPeerIdStr, testMessage);
        expect(networkService.getActiveStreamsCount(), 1);

        // Second send should reuse
        final anotherMessage = pb.Message()..pendingBytes = 456;
        await networkService.sendMessage(testPeerIdStr, anotherMessage);

        verifyNever(mockHost.newStream(any, any, any)); // Should not be called again for the same peer
        
        final messageBytes2 = anotherMessage.writeToBuffer();
        final lengthPrefix2 = dart_cid.encodeVarint(messageBytes2.length);
        verify(mockStream.write(lengthPrefix2)).called(1);
        verify(mockStream.write(messageBytes2)).called(1);
        expect(networkService.getActiveStreamsCount(), 1);
      });

      test('handles stream write error, closes and removes stream', () async {
        when(mockHost.newStream(any, any, any)).thenAnswer((_) async => mockStream);
        when(mockStream.write(any)).thenThrow(Exception('Write failed'));

        await networkService.sendMessage(testPeerIdStr, testMessage);

        verify(mockStream.close()).called(1);
        expect(networkService.getActiveStreamsCount(), 0);
      });
    });

    group('_onIncomingStream()', () {
      test('correctly decodes and handles a valid message', () async {
        final wantlist = pb.Message_Wantlist_Entry()..block = dart_cid.CID.fromString('bafyreibm6jg3ux5qumhcn2b3flc3tyu6dmlb4xa7u5bf44yegnrjhc4yeq').toBytes();
        final testPbMessage = pb.Message()..wantlist = (pb.Message_Wantlist()..entries.add(wantlist));
        final messageBytes = testPbMessage.writeToBuffer();
        final lengthPrefix = dart_cid.encodeVarint(messageBytes.length);
        final fullFrame = Uint8List.fromList([...lengthPrefix, ...messageBytes]);

        when(mockStream.read()).thenAnswer((_) async => fullFrame);

        await networkService.handleIncomingStreamForTest(mockStream, mockPeerId); // Use a test helper

        expect(receivedMessagesLog.length, 1);
        expect(receivedMessagesLog.first['peerId'], testPeerIdStr);
        expect(receivedMessagesLog.first['message'], testPbMessage);
      });

      test('handles empty data frame', () async {
        when(mockStream.read()).thenAnswer((_) async => Uint8List(0));
        await networkService.handleIncomingStreamForTest(mockStream, mockPeerId);
        expect(receivedMessagesLog, isEmpty);
      });

      test('handles varint decoding error', () async {
        when(mockStream.read()).thenAnswer((_) async => Uint8List.fromList([0xff, 0xff])); // Invalid varint
        await networkService.handleIncomingStreamForTest(mockStream, mockPeerId);
        expect(receivedMessagesLog, isEmpty);
      });

      test('handles insufficient data for declared message length', () async {
        final smallMessage = pb.Message()..pendingBytes = 10;
        final messageBytes = smallMessage.writeToBuffer(); // Corrected: call on Message object
        final lengthPrefix = dart_cid.encodeVarint(messageBytes.length + 5); // Declare longer length
        final fullFrame = Uint8List.fromList([...lengthPrefix, ...messageBytes]); // Data is shorter

        when(mockStream.read()).thenAnswer((_) async => fullFrame);
        await networkService.handleIncomingStreamForTest(mockStream, mockPeerId);
        expect(receivedMessagesLog, isEmpty);
      });
       test('handles extra data in frame after message', () async {
        final testPbMessage = pb.Message()..pendingBytes = 77;
        final messageBytes = testPbMessage.writeToBuffer();
        final lengthPrefix = dart_cid.encodeVarint(messageBytes.length);
        final extraData = Uint8List.fromList([1,2,3,4,5]);
        final fullFrame = Uint8List.fromList([...lengthPrefix, ...messageBytes, ...extraData]);

        when(mockStream.read()).thenAnswer((_) async => fullFrame);

        await networkService.handleIncomingStreamForTest(mockStream, mockPeerId);

        expect(receivedMessagesLog.length, 1);
        expect(receivedMessagesLog.first['peerId'], testPeerIdStr);
        expect(receivedMessagesLog.first['message'], testPbMessage);
        // Verify warning for extra data (would need log spy or similar)
      });
    });
    
    test('handlePeerDisconnected closes and removes stream', () async {
      // Add a stream
      when(mockHost.newStream(any, any, any)).thenAnswer((_) async => mockStream);
      await networkService.sendMessage(testPeerIdStr, pb.Message());
      expect(networkService.getActiveStreamsCount(), 1);

      await networkService.handlePeerDisconnected(testPeerIdStr);
      
      verify(mockStream.close()).called(1);
      expect(networkService.getActiveStreamsCount(), 0);
    });
  });
}

// Helper extension to access internal state for testing (not for production)
extension NetworkServiceTestHelper on BitswapNetworkService {
  int getActiveStreamsCount() => activeStreams.length; // Expose for test, use renamed
  Future<void> handleIncomingStreamForTest(libp2p.P2PStream stream, libp2p.PeerId peerId) =>
      onIncomingStream(stream, peerId); // Expose for test, use renamed
}
