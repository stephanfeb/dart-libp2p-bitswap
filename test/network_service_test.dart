import 'dart:async';
import 'dart:typed_data';

import 'package:dart_libp2p_bitswap/src/network/network_service.dart';
import 'package:dart_libp2p_bitswap/src/proto/bitswap_message.pb.dart' as pb;
import 'package:dcid/dcid.dart' as dart_cid;
import 'package:dart_libp2p/dart_libp2p.dart' as libp2p;
import 'package:test/test.dart';
import 'package:mockito/mockito.dart';
import 'package:mockito/annotations.dart';

// Import the generated mocks
import 'network_service_test.mocks.dart';
import 'stream_stubs.dart';

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
    mockMessageHandler = (libp2p.PeerId peerId, pb.Message message) async {
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
    final remotePeer = libp2p.PeerId.fromString('bafzbeigweq4j366sq2k7j727lqgrw7x2k3x5ms2sl4unhyrionp2zlbxci'); // "test-peer-id-1"
    // The corresponding Base58 representation
    final expectedBase58PeerId = 'QmckZzS6pmmQXrmZPFjevKuT3Hip4kNyj5FjarHrYKfcBs';

    final stopTestPeer = libp2p.PeerId.fromString('bafzbeibcolx5jffocx4p4qaccrk2hvzuhqpd76byk7pbuo2pmyptifh2q4'); // "another-test-peer"
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

      stubLiveStream(mockStream);
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
      await networkService.sendMessage(stopTestPeer, message);

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
        
        await networkService.sendMessage(remotePeer, testMessage);

        verify(mockHost.newStream(
          argThat(isA<libp2p.PeerId>().having((p) => p.toBase58(), 'toBase58', expectedBase58PeerId)),
          [networkService.protocolId],
          any
        )).called(1);
        
        // Single combined write: prefix + message bytes
        verify(mockStream.write(any)).called(1);
        expect(networkService.getActiveStreamsCount(), 1);
      });

      test('reuses an existing stream', () async {
        when(mockHost.newStream(
          argThat(isA<libp2p.PeerId>().having((p) => p.toBase58(), 'toBase58', expectedBase58PeerId)), 
          any, 
          any
        )).thenAnswer((_) async => mockStream);
        
        await networkService.sendMessage(remotePeer, testMessage);
        expect(networkService.getActiveStreamsCount(), 1);
        clearInteractions(mockHost);
        clearInteractions(mockStream);
         when(mockHost.newStream( // Re-stub after clearing
          argThat(isA<libp2p.PeerId>().having((p) => p.toBase58(), 'toBase58', expectedBase58PeerId)), 
          any, 
          any
        )).thenAnswer((_) async => mockStream);


        final anotherMessage = pb.Message()..pendingBytes = 456;
        await networkService.sendMessage(remotePeer, anotherMessage);

        verifyNever(mockHost.newStream(
            argThat(isA<libp2p.PeerId>().having((p) => p.toBase58(), 'toBase58', expectedBase58PeerId)),
            any,
            any));

        // Single combined write for the second message
        verify(mockStream.write(any)).called(1);
        expect(networkService.getActiveStreamsCount(), 1);
      });

      test('handles stream write error, closes and removes stream', () async {
        when(mockHost.newStream(
          argThat(isA<libp2p.PeerId>().having((p) => p.toBase58(), 'toBase58', expectedBase58PeerId)), 
          any, 
          any
        )).thenAnswer((_) async => mockStream);
        when(mockStream.write(any)).thenThrow(Exception('Write failed'));

        await networkService.sendMessage(remotePeer, testMessage);

        verify(mockStream.close()).called(1);
        expect(networkService.getActiveStreamsCount(), 0);
      });
    });

    group('sendMessage() queueing', () {
      pb.Message msg(int marker) => pb.Message()..pendingBytes = marker;
      int markerOf(Uint8List frame) {
        final prefix = dart_cid.decodeVarint(frame);
        return pb.Message.fromBuffer(frame.sublist(prefix.value)).pendingBytes;
      }

      test('concurrent sends share one stream and preserve call order', () async {
        final written = <int>[];
        when(mockStream.write(any)).thenAnswer((inv) async {
          // Later writes complete faster, so ordering relies on the queue
          final m = markerOf(inv.positionalArguments[0] as Uint8List);
          await Future.delayed(Duration(milliseconds: 10 - m));
          written.add(m);
        });
        when(mockHost.newStream(any, any, any)).thenAnswer((_) async {
          await Future.delayed(const Duration(milliseconds: 5));
          return mockStream;
        });

        await Future.wait([
          for (var i = 1; i <= 5; i++) networkService.sendMessage(remotePeer, msg(i)),
        ]);

        verify(mockHost.newStream(any, any, any)).called(1);
        expect(written, [1, 2, 3, 4, 5]);
      });

      test('retries once on a fresh stream when a reused stream fails', () async {
        final staleStream = MockP2PStream();
        stubLiveStream(staleStream);
        when(staleStream.id()).thenReturn('stale');
        when(staleStream.close()).thenAnswer((_) async {});
        var staleWrites = 0;
        when(staleStream.write(any)).thenAnswer((_) async {
          if (++staleWrites > 1) throw Exception('stream reset by remote');
        });
        final streams = [staleStream, mockStream];
        when(mockHost.newStream(any, any, any)).thenAnswer((_) async => streams.removeAt(0));

        await networkService.sendMessage(remotePeer, msg(1)); // opens staleStream
        await networkService.sendMessage(remotePeer, msg(2)); // fails, retried

        verify(staleStream.close()).called(1);
        verify(mockHost.newStream(any, any, any)).called(2);
        final retried = verify(mockStream.write(captureAny)).captured.single as Uint8List;
        expect(markerOf(retried), 2);
        expect(networkService.getActiveStreamsCount(), 1);
      });

      test('does not retry when a freshly opened stream fails', () async {
        when(mockStream.write(any)).thenThrow(Exception('write failed'));
        when(mockHost.newStream(any, any, any)).thenAnswer((_) async => mockStream);

        await networkService.sendMessage(remotePeer, msg(1));

        verify(mockHost.newStream(any, any, any)).called(1);
        expect(networkService.getActiveStreamsCount(), 0);
      });

      test('a failed send does not block later sends to the same peer', () async {
        var calls = 0;
        when(mockHost.newStream(any, any, any)).thenAnswer((_) async {
          if (++calls == 1) throw Exception('dial failed');
          return mockStream;
        });

        await Future.wait([
          networkService.sendMessage(remotePeer, msg(1)),
          networkService.sendMessage(remotePeer, msg(2)),
        ]);

        final sent = verify(mockStream.write(captureAny)).captured.cast<Uint8List>();
        expect(sent.map(markerOf), [2]);
      });
    });

    group('remote close of outgoing streams', () {
      late MockP2PStream first;
      late Completer<Uint8List> firstRead;

      setUp(() {
        first = MockP2PStream();
        firstRead = Completer<Uint8List>();
        when(first.isClosed).thenReturn(false);
        when(first.read(any)).thenAnswer((_) => firstRead.future);
        when(first.id()).thenReturn('first');
        when(first.write(any)).thenAnswer((_) async {});
        when(first.close()).thenAnswer((_) async {});
        final streams = [first, mockStream];
        when(mockHost.newStream(any, any, any)).thenAnswer((_) async => streams.removeAt(0));
      });

      Future<void> expectNextSendUsesFreshStream() async {
        await networkService.sendMessage(remotePeer, pb.Message());
        verify(first.write(any)).called(1); // Only the first message
        verify(mockStream.write(any)).called(1);
        verify(mockHost.newStream(any, any, any)).called(2);
      }

      test('EOF on our outgoing stream drops it before the next send', () async {
        await networkService.sendMessage(remotePeer, pb.Message());

        firstRead.complete(Uint8List(0));
        await pumpEventQueue();
        expect(networkService.getActiveStreamsCount(), 0);
        verify(first.close()).called(1);

        await expectNextSendUsesFreshStream();
      });

      test('a reset of our outgoing stream drops it before the next send', () async {
        await networkService.sendMessage(remotePeer, pb.Message());

        firstRead.completeError(Exception('stream reset'));
        await pumpEventQueue();
        expect(networkService.getActiveStreamsCount(), 0);

        await expectNextSendUsesFreshStream();
      });

      test('a cached stream already marked closed is not written to', () async {
        await networkService.sendMessage(remotePeer, pb.Message());
        when(first.isClosed).thenReturn(true);

        await expectNextSendUsesFreshStream();
      });

      test('stop() copes with closes that end the watched read', () async {
        // As on a real stream: closing it completes the pending read with EOF
        when(first.close()).thenAnswer((_) async {
          if (!firstRead.isCompleted) firstRead.complete(Uint8List(0));
        });
        final otherPeer = libp2p.PeerId.fromBytes(Uint8List.fromList([0x12, 0x20, ...List.filled(32, 7)]));
        await networkService.sendMessage(remotePeer, pb.Message()); // first
        await networkService.sendMessage(otherPeer, pb.Message()); // mockStream

        await networkService.stop();

        verify(first.close()).called(1);
        verify(mockStream.close()).called(1);
        expect(networkService.getActiveStreamsCount(), 0);
      });

      test('a late EOF on a replaced stream does not drop its replacement', () async {
        await networkService.sendMessage(remotePeer, pb.Message()); // first
        await networkService.handlePeerDisconnected(remotePeer); // closes first
        await networkService.sendMessage(remotePeer, pb.Message()); // mockStream

        firstRead.complete(Uint8List(0));
        await pumpEventQueue();

        expect(networkService.getActiveStreamsCount(), 1);
        await networkService.sendMessage(remotePeer, pb.Message());
        verify(mockHost.newStream(any, any, any)).called(2);
        verify(mockStream.write(any)).called(2);
      });
    });

    group('_onIncomingStream()', () {
      test('correctly decodes and handles a valid message', () async {
        final wantlist = pb.Message_Wantlist_Entry()..block = dart_cid.CID.fromString('bafyreibm6jg3ux5qumhcn2b3flc3tyu6dmlb4xa7u5bf44yegnrjhc4yeq').toBytes();
        final testPbMessage = pb.Message()..wantlist = (pb.Message_Wantlist()..entries.add(wantlist));
        final messageBytes = testPbMessage.writeToBuffer();
        final lengthPrefix = dart_cid.encodeVarint(messageBytes.length);
        final fullFrame = Uint8List.fromList([...lengthPrefix, ...messageBytes]);

        stubReads(mockStream, [fullFrame]);

        // mockPeerId is already set up in the outer group's setUp to return expectedBase58PeerId for toBase58()
        await networkService.handleIncomingStreamForTest(mockStream, mockPeerId);

        expect(receivedMessagesLog.length, 1);
        expect(receivedMessagesLog.first['peerId'], same(mockPeerId));
        expect(receivedMessagesLog.first['message'], testPbMessage);
      });

      test('handles empty data frame', () async {
        stubReads(mockStream, []);
        await networkService.handleIncomingStreamForTest(mockStream, mockPeerId);
        expect(receivedMessagesLog, isEmpty);
      });

      test('handles varint decoding error', () async {
        stubReads(mockStream, List.filled(5, Uint8List.fromList([0xff, 0xff]))); // Unterminated varint
        await networkService.handleIncomingStreamForTest(mockStream, mockPeerId);
        expect(receivedMessagesLog, isEmpty);
      });

      test('handles insufficient data for declared message length', () async {
        final smallMessage = pb.Message()..pendingBytes = 10;
        final messageBytes = smallMessage.writeToBuffer();
        final lengthPrefix = dart_cid.encodeVarint(messageBytes.length + 5); // Declare longer length
        final fullFrame = Uint8List.fromList([...lengthPrefix, ...messageBytes]);

        stubReads(mockStream, [fullFrame]);
        await networkService.handleIncomingStreamForTest(mockStream, mockPeerId);
        expect(receivedMessagesLog, isEmpty);
      });
      test('resets the stream when declared length exceeds the size limit', () async {
        when(mockStream.reset()).thenAnswer((_) async {});
        var reads = 0;
        final prefix = dart_cid.encodeVarint(maxBitswapMessageSize + 1);
        // After the prefix, a hostile peer would stream bytes indefinitely
        when(mockStream.read()).thenAnswer((_) async {
          reads++;
          return reads == 1 ? Uint8List.fromList(prefix) : Uint8List(1024);
        });

        await networkService.handleIncomingStreamForTest(mockStream, mockPeerId);

        expect(receivedMessagesLog, isEmpty);
        expect(reads, 1, reason: 'should not buffer the body of an oversized message');
        verify(mockStream.reset()).called(1);
      });

       test('handles two messages in a single read (pipelining)', () async {
        final msg1 = pb.Message()..pendingBytes = 77;
        final msg1Bytes = msg1.writeToBuffer();
        final prefix1 = dart_cid.encodeVarint(msg1Bytes.length);

        final msg2 = pb.Message()..pendingBytes = 88;
        final msg2Bytes = msg2.writeToBuffer();
        final prefix2 = dart_cid.encodeVarint(msg2Bytes.length);

        final fullFrame = Uint8List.fromList([
          ...prefix1, ...msg1Bytes,
          ...prefix2, ...msg2Bytes,
        ]);

        int readCount = 0;
        when(mockStream.read()).thenAnswer((_) async {
          readCount++;
          if (readCount == 1) return fullFrame;
          return Uint8List(0); // stream closed
        });

        await networkService.handleIncomingStreamForTest(mockStream, mockPeerId);

        expect(receivedMessagesLog.length, 2);
        expect((receivedMessagesLog[0]['message'] as pb.Message).pendingBytes, 77);
        expect((receivedMessagesLog[1]['message'] as pb.Message).pendingBytes, 88);
      });

      test('handles multiple messages across multiple reads', () async {
        final msg1 = pb.Message()..pendingBytes = 11;
        final msg1Bytes = msg1.writeToBuffer();
        final prefix1 = dart_cid.encodeVarint(msg1Bytes.length);
        final frame1 = Uint8List.fromList([...prefix1, ...msg1Bytes]);

        final msg2 = pb.Message()..pendingBytes = 22;
        final msg2Bytes = msg2.writeToBuffer();
        final prefix2 = dart_cid.encodeVarint(msg2Bytes.length);
        final frame2 = Uint8List.fromList([...prefix2, ...msg2Bytes]);

        int readCount = 0;
        when(mockStream.read()).thenAnswer((_) async {
          readCount++;
          if (readCount == 1) return frame1;
          if (readCount == 2) return frame2;
          return Uint8List(0); // stream closed
        });

        await networkService.handleIncomingStreamForTest(mockStream, mockPeerId);

        expect(receivedMessagesLog.length, 2);
        expect((receivedMessagesLog[0]['message'] as pb.Message).pendingBytes, 11);
        expect((receivedMessagesLog[1]['message'] as pb.Message).pendingBytes, 22);
      });
    });
    
    test('stop() resets inbound streams so their read loops end', () async {
      final readGate = Completer<Uint8List>();
      when(mockStream.read()).thenAnswer((_) => readGate.future);
      when(mockStream.reset()).thenAnswer((_) async {
        if (!readGate.isCompleted) readGate.complete(Uint8List(0)); // reset unblocks the reader
      });

      final handler = networkService.handleIncomingStreamForTest(mockStream, mockPeerId);
      await pumpEventQueue();
      await networkService.stop();

      await handler.timeout(const Duration(seconds: 1));
      verify(mockStream.reset()).called(1);
    });

    test('sendMessage is a no-op after stop()', () async {
      await networkService.stop();
      await networkService.sendMessage(remotePeer, pb.Message());
      verifyNever(mockHost.newStream(any, any, any));
    });

    test('handlePeerDisconnected closes and removes stream', () async {
      // Add a stream using the parsable full CID string
      when(mockHost.newStream(
        argThat(isA<libp2p.PeerId>().having((p) => p.toBase58(), 'toBase58', expectedBase58PeerId)), 
        any, 
        any
      )).thenAnswer((_) async => mockStream);
      await networkService.sendMessage(remotePeer, pb.Message());
      expect(networkService.getActiveStreamsCount(), 1);

      // handlePeerDisconnected uses the string key that was used to add the stream
      await networkService.handlePeerDisconnected(remotePeer);
      
      verify(mockStream.close()).called(1);
      expect(networkService.getActiveStreamsCount(), 0);
    });
  });
}

/// Stubs [stream] so successive read() calls return [chunks] in order, then
/// an empty chunk (EOF) forever after.
void stubReads(MockP2PStream stream, List<Uint8List> chunks) {
  var i = 0;
  when(stream.read()).thenAnswer((_) async => i < chunks.length ? chunks[i++] : Uint8List(0));
}

// Helper extension to access internal state for testing (not for production)
extension NetworkServiceTestHelper on BitswapNetworkService {
  int getActiveStreamsCount() => activeStreams.length; // Expose for test, use renamed
  Future<void> handleIncomingStreamForTest(libp2p.P2PStream stream, libp2p.PeerId peerId) =>
      onIncomingStream(stream, peerId); // Expose for test, use renamed
}
