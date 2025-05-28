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
  Future<void> onIncomingStream(libp2p.P2PStream stream, libp2p.PeerId peerIdObj) async { // Renamed
    final peerId = peerIdObj.toBase58(); // Changed from toString() to toBase58()
    print('// BSW_LOG: BitswapNetworkService: Received incoming Bitswap stream ${stream.id} from $peerId');
    print('Received incoming Bitswap stream from $peerId');

    try {
      // Assuming stream.read() returns a Uint8List containing one complete
      // "upstream framed block", which in turn contains the varint-prefixed Bitswap message.
      final Uint8List upstreamFrameData = await stream.read(); 
      print('// BSW_LOG: BitswapNetworkService: Read ${upstreamFrameData.length} bytes from stream ${stream.id} from $peerId.');

      if (upstreamFrameData.isEmpty) {
        print('// BSW_LOG: BitswapNetworkService: Stream from $peerId read empty data frame.');
        print('Stream from $peerId read empty data frame.');
        return;
      }

      // 1. Decode the varint length prefix from the start of upstreamFrameData
      // CRITICAL: The p2p_varint.decodeVarint API and how to get bytesConsumed is vital here.
      // The following is a placeholder and likely needs adjustment based on your varint utility.
      int messageLength;
      int varintBytesConsumed;

      try {
        // Use dart_cid.decodeVarint which returns a MapEntry<int, int>
        // where key is the decoded value (messageLength)
        // and value is the number of bytes consumed for the varint (varintBytesConsumed)
        final decodedVarint = dart_cid.decodeVarint(upstreamFrameData);
        messageLength = decodedVarint.key;
        varintBytesConsumed = decodedVarint.value;
        print('// BSW_LOG: BitswapNetworkService: Decoded varint from $peerId. MessageLength: $messageLength, VarintBytesConsumed: $varintBytesConsumed.');
        
        if (varintBytesConsumed <= 0) {
          print('// BSW_LOG: BitswapNetworkService: Error: Varint decoding resulted in 0 or negative bytes consumed from $peerId.');
          print('Error: Varint decoding resulted in 0 or negative bytes consumed from $peerId.');
          return;
        }

        if (varintBytesConsumed > upstreamFrameData.length) {
            print('// BSW_LOG: BitswapNetworkService: Error: Varint prefix itself is longer than received data from $peerId. VarintBytes: $varintBytesConsumed, FrameLen: ${upstreamFrameData.length}');
            print('Error: Varint prefix itself is longer than received data from $peerId.');
            return;
        }

      } catch (e) {
        print('// BSW_LOG: BitswapNetworkService: Error decoding varint prefix from $peerId. Data: $upstreamFrameData. Error: $e');
        print('Error decoding varint prefix from $peerId. Data: $upstreamFrameData. Error: $e');
        return;
      }
      
      // 2. Check if we have the full message payload
      if (upstreamFrameData.length < varintBytesConsumed + messageLength) {
        print('// BSW_LOG: BitswapNetworkService: Received data frame from $peerId is too short for declared message length. Frame: ${upstreamFrameData.length}, Varint: $varintBytesConsumed, MsgLen: $messageLength');
        print('Received data frame from $peerId is too short for declared message length. Frame: ${upstreamFrameData.length}, Varint: $varintBytesConsumed, MsgLen: $messageLength');
        // This implies the "upstream framing" is not giving us one full Bitswap message + prefix,
        // or there's an issue with varint decoding.
        return;
      }

      // 3. Extract the protobuf payload
      final messagePayloadBytes = Uint8List.sublistView(
          upstreamFrameData, varintBytesConsumed, varintBytesConsumed + messageLength);
      print('// BSW_LOG: BitswapNetworkService: Extracted message payload of ${messagePayloadBytes.length} bytes for $peerId.');

      // 4. Deserialize the message
      final message = pb.Message.fromBuffer(messagePayloadBytes);
      print('// BSW_LOG: BitswapNetworkService: Deserialized message from $peerId. Calling message handler.');
      _messageHandler(peerId, message);

      // TODO: If upstreamFrameData can contain more data after this Bitswap message,
      // that needs to be handled (e.g., if multiple Bitswap messages are packed into one upstream frame).
      // For now, assuming one Bitswap message per upstream frame.
      if (upstreamFrameData.length > varintBytesConsumed + messageLength) {
          print("// BSW_LOG: BitswapNetworkService: WARN: Extra data received in upstream frame after Bitswap message from $peerId. Ignoring ${upstreamFrameData.length - (varintBytesConsumed + messageLength)} bytes.");
          print("WARN: Extra data received in upstream frame after Bitswap message from $peerId. Ignoring ${upstreamFrameData.length - (varintBytesConsumed + messageLength)} bytes.");
      }

    } catch (e) {
      print('// BSW_LOG: BitswapNetworkService: Error processing message from $peerId on stream ${stream.id}: $e');
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
