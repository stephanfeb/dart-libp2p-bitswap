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
    // Use a full, parsable identity CID string for PeerId.fromString()
    final parsablePeerIdFullStr = 'bafzbeigweq4j366sq2k7j727lqgrw7x2k3x5ms2sl4unhyrionp2zlbxci'; // "test-peer-id-1"
    // The corresponding Base58 representation
    final expectedBase58PeerId = 'QmckZzS6pmmQXrmZPFjevKuT3Hip4kNyj5FjarHrYKfcBs';

    final parsablePeerIdForStopTestFullStr = "bafzbeibcolx5jffocx4p4qaccrk2hvzuhqpd76byk7pbuo2pmyptifh2q4"; // "another-test-peer"
    // This is the actual Base58 for "bafzbeibcolx5jffocx4p4qaccrk2hvzuhqpd76byk7pbuo2pmyptifh2q4"
    final expectedBase58PeerIdForStopTest = "QmQf8Dq1H9FWfiCBBA1VvZ8xDLNYFDAd1LadvRbSwvn5Rp"; 


    late MockPeerId mockPeerId; // This mock is primarily for _onIncomingStream testing
    late MockP2PStream mockStream;

    setUp(() {
      mockPeerId = MockPeerId();
      // This mockPeerId is passed to handleIncomingStreamForTest.
      // Its toBase58() method will be called by the service.
      when(mockPeerId.toBase58()).thenReturn(expectedBase58PeerId);

      mockStream = MockP2PStream();
      when(mockStream.id()).thenReturn('testStreamId');
      when(mockStream.protocol()).thenReturn(networkService.protocolId);
      when(mockStream.write(any)).thenAnswer((_) async => {});
      when(mockStream.close()).thenAnswer((_) async => {});
    });

    test('start() registers stream handler', () async {
      await networkService.start();
      verify(mockHost.setStreamHandler(networkService.protocolId, captureAny)).called(1);
    });

    test('stop() removes stream handler and closes active streams', () async {
      // final mockPeerIdForStopTest = MockPeerId(); // Not strictly needed if we don't interact with its methods
      // when(mockPeerIdForStopTest.toBase58()).thenReturn(expectedBase58PeerIdForStopTest);

      when(mockHost.newStream(
        argThat(isA<libp2p.PeerId>().having((p) => p.toBase58(), 'toBase58', expectedBase58PeerIdForStopTest)), 
        any, 
        any
      )).thenAnswer((_) async => mockStream);
      
      final message = pb.Message();
      // Use the parsable full CID string here
      await networkService.sendMessage(parsablePeerIdForStopTestFullStr, message);

      await networkService.stop();
      
      verify(mockHost.removeStreamHandler(networkService.protocolId)).called(1);
      verify(mockStream.close()).called(1);
      expect(networkService.getActiveStreamsCount(), 0);
    });

    group('sendMessage()', () {
      final testMessage = pb.Message()..pendingBytes = 123;

      test('opens a new stream if none exists and sends data', () async {
        // Expect newStream to be called with a PeerId whose toBase58() matches expectedBase58PeerId
        when(mockHost.newStream(
          argThat(isA<libp2p.PeerId>().having((p) => p.toBase58(), 'toBase58', expectedBase58PeerId)), 
          [networkService.protocolId], 
          any
        )).thenAnswer((_) async => mockStream);
        
        await networkService.sendMessage(parsablePeerIdFullStr, testMessage);

        verify(mockHost.newStream(
          argThat(isA<libp2p.PeerId>().having((p) => p.toBase58(), 'toBase58', expectedBase58PeerId)),
          [networkService.protocolId],
          any
        )).called(1);
        
        final messageBytes = testMessage.writeToBuffer();
        final lengthPrefix = dart_cid.encodeVarint(messageBytes.length);
        
        verify(mockStream.write(lengthPrefix)).called(1);
        verify(mockStream.write(messageBytes)).called(1);
        expect(networkService.getActiveStreamsCount(), 1);
      });

      test('reuses an existing stream', () async {
        when(mockHost.newStream(
          argThat(isA<libp2p.PeerId>().having((p) => p.toBase58(), 'toBase58', expectedBase58PeerId)), 
          any, 
          any
        )).thenAnswer((_) async => mockStream);
        
        await networkService.sendMessage(parsablePeerIdFullStr, testMessage);
        expect(networkService.getActiveStreamsCount(), 1);
        clearInteractions(mockHost); // Clear interactions to verify newStream is not called again
         when(mockHost.newStream( // Re-stub after clearing
          argThat(isA<libp2p.PeerId>().having((p) => p.toBase58(), 'toBase58', expectedBase58PeerId)), 
          any, 
          any
        )).thenAnswer((_) async => mockStream);


        final anotherMessage = pb.Message()..pendingBytes = 456;
        await networkService.sendMessage(parsablePeerIdFullStr, anotherMessage);

        verifyNever(mockHost.newStream(
            argThat(isA<libp2p.PeerId>().having((p) => p.toBase58(), 'toBase58', expectedBase58PeerId)),
            any,
            any));

        final messageBytes2 = anotherMessage.writeToBuffer();
        final lengthPrefix2 = dart_cid.encodeVarint(messageBytes2.length);
        verify(mockStream.write(lengthPrefix2)).called(1);
        verify(mockStream.write(messageBytes2)).called(1);
        expect(networkService.getActiveStreamsCount(), 1);
      });

      test('handles stream write error, closes and removes stream', () async {
        when(mockHost.newStream(
          argThat(isA<libp2p.PeerId>().having((p) => p.toBase58(), 'toBase58', expectedBase58PeerId)), 
          any, 
          any
        )).thenAnswer((_) async => mockStream);
        when(mockStream.write(any)).thenThrow(Exception('Write failed'));

        await networkService.sendMessage(parsablePeerIdFullStr, testMessage);

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

        // mockPeerId is already set up in the outer group's setUp to return expectedBase58PeerId for toBase58()
        await networkService.handleIncomingStreamForTest(mockStream, mockPeerId);

        expect(receivedMessagesLog.length, 1);
        expect(receivedMessagesLog.first['peerId'], expectedBase58PeerId); // Check against the Base58 string
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
        final messageBytes = smallMessage.writeToBuffer();
        final lengthPrefix = dart_cid.encodeVarint(messageBytes.length + 5); // Declare longer length
        final fullFrame = Uint8List.fromList([...lengthPrefix, ...messageBytes]);

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
        expect(receivedMessagesLog.first['peerId'], expectedBase58PeerId); // Check against Base58
        expect(receivedMessagesLog.first['message'], testPbMessage);
        // Verify warning for extra data (would need log spy or similar)
      });
    });
    
    test('handlePeerDisconnected closes and removes stream', () async {
      // Add a stream using the parsable full CID string
      when(mockHost.newStream(
        argThat(isA<libp2p.PeerId>().having((p) => p.toBase58(), 'toBase58', expectedBase58PeerId)), 
        any, 
        any
      )).thenAnswer((_) async => mockStream);
      await networkService.sendMessage(parsablePeerIdFullStr, pb.Message());
      expect(networkService.getActiveStreamsCount(), 1);

      // handlePeerDisconnected uses the string key that was used to add the stream
      await networkService.handlePeerDisconnected(parsablePeerIdFullStr);
      
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
