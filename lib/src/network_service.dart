import 'dart:async';
import 'dart:typed_data';

import 'package:dart_libp2p/dart_libp2p.dart' as libp2p;
// Assuming 'varint' package for unsigned varint encoding/decoding
// If not available, a helper function would be needed.
import 'package:dart_cid/dart_cid.dart' as dart_cid; // Using varint from dart_libp2p
// Placeholder for varint, actual package might differ or need implementation

import './proto/bitswap_message.pb.dart' as pb;

// Callback type for when a full Bitswap message is received
typedef BitswapMessageHandler = void Function(String peerId, pb.Message message);

class BitswapNetworkService {
  final libp2p.Host _host;
  final BitswapMessageHandler _messageHandler;
  final String protocolId = '/ipfs/bitswap/1.2.0'; // Bitswap 1.2.0 protocol ID

  // Manage active outgoing streams to peers
  final Map<String, libp2p.P2PStream> activeStreams = {}; // Renamed

  BitswapNetworkService(this._host, this._messageHandler) {
    print('// BSW_LOG: BitswapNetworkService instance created.');
    print('BitswapNetworkService instance created.');
  }

  Future<void> start() async {
    _host.setStreamHandler(protocolId, onIncomingStream); // Renamed
    print('// BSW_LOG: BitswapNetworkService: Bitswap protocol handler registered for $protocolId');
    print('Bitswap protocol handler registered for $protocolId');
    // TODO: Potentially discover existing peers and establish connections
    // or wait for them to connect to us.
  }

  Future<void> stop() async {
    _host.removeStreamHandler(protocolId);
    print('// BSW_LOG: BitswapNetworkService: Bitswap protocol handler unregistered for $protocolId');
    print('Bitswap protocol handler unregistered for $protocolId');
    // Close any active outgoing streams
    for (final stream in activeStreams.values) { // Renamed
      try {
        await stream.close();
      } catch (e) {
        print('// BSW_LOG: BitswapNetworkService: Error closing stream ${stream.id} during stop: $e');
        print('Error closing stream during stop: $e');
      }
    }
    activeStreams.clear(); // Renamed
    print('// BSW_LOG: BitswapNetworkService: All active streams cleared.');
  }

  // Called by Bitswap when a peer disconnects to clean up the stream
  Future<void> handlePeerDisconnected(String peerId) async {
    final stream = activeStreams.remove(peerId); // Renamed
    if (stream != null) {
      print('// BSW_LOG: BitswapNetworkService: Peer $peerId disconnected, closing and removing active stream ${stream.id}.');
      print('Peer $peerId disconnected, closing and removing active stream ${stream.id}.');
      try {
        await stream.close();
      } catch (e) {
        print('// BSW_LOG: BitswapNetworkService: Error closing stream for disconnected peer $peerId: $e');
        print('Error closing stream for disconnected peer $peerId: $e');
      }
    } else {
      print('// BSW_LOG: BitswapNetworkService: handlePeerDisconnected called for $peerId, but no active stream found.');
    }
  }

  // Handles a new incoming libp2p stream for the Bitswap protocol
  // This method's signature might still need adjustment based on your Host.setStreamHandler
  Future<void> onIncomingStream(libp2p.P2PStream stream, libp2p.PeerId peerIdObj) async {
    final peerId = peerIdObj.toBase58();
    print('// BSW_LOG: BitswapNetworkService: Received incoming Bitswap stream ${stream.id} from $peerId');
    print('Received incoming Bitswap stream from $peerId');

    final BytesBuilder bytesBuilder = BytesBuilder();
    int? messageLength;
    int? varintBytesConsumed;
    const maxVarintLen = 9; // Max length for a 64-bit varint

    try {
      // Loop to read data until we can parse the varint length prefix
      while (true) {
        final Uint8List chunk = await stream.read();
        if (chunk.isEmpty) {
          print('// BSW_LOG: BitswapNetworkService: Stream from $peerId closed while reading for varint prefix. Buffer length: ${bytesBuilder.length}');
          if (bytesBuilder.isEmpty) return; // No data received at all
          // If some data was received but not enough for a full message, it's an error or incomplete message.
          print('// BSW_LOG: BitswapNetworkService: Incomplete message from $peerId, stream closed. Discarding ${bytesBuilder.length} buffered bytes.');
          return;
        }
        bytesBuilder.add(chunk);
        print('// BSW_LOG: BitswapNetworkService: Read chunk of ${chunk.length} bytes from $peerId. Total buffered: ${bytesBuilder.length}');

        if (bytesBuilder.length >= 1) { // Need at least 1 byte to attempt varint decoding
          try {
            final decodedVarint = dart_cid.decodeVarint(bytesBuilder.toBytes()); // Use toBytes() to get a copy
            messageLength = decodedVarint.key;
            varintBytesConsumed = decodedVarint.value;
            print('// BSW_LOG: BitswapNetworkService: Decoded varint from $peerId. MessageLength: $messageLength, VarintBytesConsumed: $varintBytesConsumed. Buffered: ${bytesBuilder.length}');
            
            if (varintBytesConsumed! <= 0) {
              print('// BSW_LOG: BitswapNetworkService: Error: Varint decoding resulted in 0 or negative bytes consumed from $peerId.');
              return;
            }
            // Successfully decoded varint, break from this loop to proceed to message body reading
            break; 
          } catch (e) {
            if (bytesBuilder.length >= maxVarintLen) {
              // If we've read maxVarintLen bytes and still can't decode, it's likely a bad varint.
              print('// BSW_LOG: BitswapNetworkService: Error decoding varint prefix from $peerId after ${bytesBuilder.length} bytes. Error: $e');
              return;
            }
            // Not enough bytes to decode varint yet, or a partial varint. Continue reading.
            print('// BSW_LOG: BitswapNetworkService: Could not decode varint yet from $peerId (buffered: ${bytesBuilder.length}, error: $e). Reading more.');
          }
        }
      }

      // At this point, messageLength and varintBytesConsumed are set.
      final int totalExpectedLength = varintBytesConsumed! + messageLength!;
      print('// BSW_LOG: BitswapNetworkService: Expecting total message length (prefix + payload) of $totalExpectedLength bytes from $peerId.');

      // Loop to read the rest of the message body if not all received yet
      while (bytesBuilder.length < totalExpectedLength) {
        final Uint8List chunk = await stream.read();
        if (chunk.isEmpty) {
          print('// BSW_LOG: BitswapNetworkService: Stream from $peerId closed while reading message body. Expected $totalExpectedLength, got ${bytesBuilder.length}.');
          print('// BSW_LOG: BitswapNetworkService: Incomplete message body from $peerId. Discarding.');
          return;
        }
        bytesBuilder.add(chunk);
        print('// BSW_LOG: BitswapNetworkService: Read body chunk of ${chunk.length} bytes from $peerId. Total buffered: ${bytesBuilder.length} / $totalExpectedLength');
      }

      // We have enough data (or more, if messages are pipelined, though we don't handle that yet)
      final Uint8List allReceivedData = bytesBuilder.toBytes();

      if (allReceivedData.length < totalExpectedLength) {
         // This should ideally not be reached if the loop above works correctly with stream closure checks
        print('// BSW_LOG: BitswapNetworkService: Received data from $peerId is still too short after read loop. Expected: $totalExpectedLength, Got: ${allReceivedData.length}');
        return;
      }
      
      // Extract the specific message payload
      final messagePayloadBytes = Uint8List.sublistView(
          allReceivedData, varintBytesConsumed!, totalExpectedLength);
      print('// BSW_LOG: BitswapNetworkService: Extracted message payload of ${messagePayloadBytes.length} bytes for $peerId.');
      
      // Deserialize the message
      final message = pb.Message.fromBuffer(messagePayloadBytes);
      print('// BSW_LOG: BitswapNetworkService: Deserialized message from $peerId. Calling message handler.');
      _messageHandler(peerId, message);

      // Basic handling for extra data: if more data was received than one message,
      // it implies pipelining or a framing issue not handled here.
      // A more robust solution would re-process the remainder of allReceivedData.
      if (allReceivedData.length > totalExpectedLength) {
          print("// BSW_LOG: BitswapNetworkService: WARN: Extra data received in stream after full Bitswap message from $peerId. Total received: ${allReceivedData.length}, Processed: $totalExpectedLength. Ignoring ${allReceivedData.length - totalExpectedLength} bytes.");
          // For a truly robust system, you'd take allReceivedData.sublist(totalExpectedLength)
          // and feed it back into the start of a new message processing cycle for this stream.
          // This example does not implement such re-entrancy for simplicity.
      }

    } catch (e, s) {
      print('// BSW_LOG: BitswapNetworkService: Error processing message from $peerId on stream ${stream.id}: $e\nStack: $s');
      print('Error processing message from $peerId on stream: $e');
      // Consider closing the stream or other error handling.
    } finally {
      // If P2PStream is single-message, it might be closed by the libp2p layer after read().
      // If it's a persistent stream, we might not close it here.
      // For now, let's assume the stream lifecycle is managed by the caller or libp2p.
      print('// BSW_LOG: BitswapNetworkService: Finished processing incoming stream ${stream.id} from $peerId.');
    }
  }

  Future<void> sendMessage(String peerId, pb.Message message) async {
    print('// BSW_LOG: BitswapNetworkService: Attempting to send Bitswap message to $peerId. Message summary: Wantlist: ${message.hasWantlist()}, Payloads: ${message.payload.length}, Presences: ${message.blockPresences.length}');
    print('Attempting to send Bitswap message to $peerId.');
    libp2p.P2PStream? stream = activeStreams[peerId]; // Renamed

    try {
      if (stream == null) {
        print('// BSW_LOG: BitswapNetworkService: No active stream to $peerId, opening new one.');
        print('No active stream to $peerId, opening new one.');
        final peerIdObj = libp2p.PeerId.fromString(peerId);
        print('// BSW_LOG: BitswapNetworkService: _host runtimeType: ${_host.runtimeType}, hashCode: ${_host.hashCode}');
        print('NetworkService: _host runtimeType: ${_host.runtimeType}, hashCode: ${_host.hashCode}');
        print('// BSW_LOG: BitswapNetworkService: Calling _host.newStream for peer $peerId ($peerIdObj) with protocol $protocolId');
        print('NetworkService: Calling _host.newStream for peer $peerId ($peerIdObj)');
        try {
          stream = await _host.newStream(peerIdObj, [protocolId], libp2p.Context());
          print('// BSW_LOG: BitswapNetworkService: _host.newStream completed. Stream ID: ${stream?.id}');
          print('NetworkService: _host.newStream completed. Stream: ${stream?.id}');
        } catch (e, s) {
          print('// BSW_LOG: BitswapNetworkService: Error calling _host.newStream for $peerId: $e\nStack: $s');
          print('NetworkService: Error calling _host.newStream: $e\nStack: $s');
          rethrow;
        }
        if (stream == null) {
          print('// BSW_LOG: BitswapNetworkService: _host.newStream returned null for $peerId! Cannot send message.');
          print('NetworkService: _host.newStream returned null for $peerId!');
          // Potentially throw an error here or handle this case, 
          // as not having a stream means we can't send the message.
          return; // Exit if stream is null
        }
        activeStreams[peerId] = stream; // Renamed
        print('// BSW_LOG: BitswapNetworkService: New stream ${stream.id} to $peerId added to active streams.');
        // TODO: Listen for stream close/error events to remove it from activeStreams
        // For example, stream.onClose(() => activeStreams.remove(peerId));
        // or handle errors in the write operation below to remove the stream.
      } else {
        print('// BSW_LOG: BitswapNetworkService: Reusing existing stream ${stream.id} to $peerId.');
        print('Reusing existing stream to $peerId.');
      }

      final messageBytes = message.writeToBuffer();
      final lengthPrefix = dart_cid.encodeVarint(messageBytes.length);
      print('// BSW_LOG: BitswapNetworkService: Sending to $peerId on stream ${stream.id}. Prefix length: ${lengthPrefix.length}, Message length: ${messageBytes.length}.');

      await stream.write(lengthPrefix);
      await stream.write(messageBytes);
      
      // DO NOT close the stream here if we want to reuse it.
      // stream.close(); 
      print('// BSW_LOG: BitswapNetworkService: Bitswap message sent to $peerId using stream ${stream.id}.');
      print('Bitswap message sent to $peerId using stream ${stream.id}.');

    } catch (e) {
      print('// BSW_LOG: BitswapNetworkService: Error sending Bitswap message to $peerId: $e');
      print('Error sending Bitswap message to $peerId: $e');
      // If an error occurs, the stream might be unusable. Remove it.
      if (stream != null) { // Check if stream was successfully assigned before trying to remove/close
        print('// BSW_LOG: BitswapNetworkService: Removing and closing stream ${stream.id} to $peerId due to send error.');
        activeStreams.remove(peerId); // Renamed
        try {
          await stream.close(); // Attempt to close it gracefully
        } catch (closeError) {
          print('// BSW_LOG: BitswapNetworkService: Error closing stream ${stream.id} after send error: $closeError');
          print('NetworkService: Error closing stream after send error: $closeError');
        }
      } else {
        // If stream was null (e.g. newStream failed and returned null, or threw before assignment)
        // we might still want to remove the peerId key if it was speculatively added or if there's a different logic path.
        // For now, just log that the stream was null at this point.
        print('// BSW_LOG: BitswapNetworkService: Stream was null when catching send error for $peerId.');
        print('NetworkService: Stream was null when catching send error for $peerId.');
      }
    }
  }
}
