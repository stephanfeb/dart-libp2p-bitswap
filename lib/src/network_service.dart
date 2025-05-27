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
    print('BitswapNetworkService instance created.');
  }

  Future<void> start() async {
    _host.setStreamHandler(protocolId, onIncomingStream); // Renamed
    print('Bitswap protocol handler registered for $protocolId');
    // TODO: Potentially discover existing peers and establish connections
    // or wait for them to connect to us.
  }

  Future<void> stop() async {
    _host.removeStreamHandler(protocolId);
    print('Bitswap protocol handler unregistered for $protocolId');
    // Close any active outgoing streams
    for (final stream in activeStreams.values) { // Renamed
      try {
        await stream.close();
      } catch (e) {
        print('Error closing stream during stop: $e');
      }
    }
    activeStreams.clear(); // Renamed
  }

  // Called by Bitswap when a peer disconnects to clean up the stream
  Future<void> handlePeerDisconnected(String peerId) async {
    final stream = activeStreams.remove(peerId); // Renamed
    if (stream != null) {
      print('Peer $peerId disconnected, closing and removing active stream ${stream.id}.');
      try {
        await stream.close();
      } catch (e) {
        print('Error closing stream for disconnected peer $peerId: $e');
      }
    }
  }

  // Handles a new incoming libp2p stream for the Bitswap protocol
  // This method's signature might still need adjustment based on your Host.setStreamHandler
  Future<void> onIncomingStream(libp2p.P2PStream stream, libp2p.PeerId peerIdObj) async { // Renamed
    final peerId = peerIdObj.toString();
    print('Received incoming Bitswap stream from $peerId');

    try {
      // Assuming stream.read() returns a Uint8List containing one complete
      // "upstream framed block", which in turn contains the varint-prefixed Bitswap message.
      final Uint8List upstreamFrameData = await stream.read(); 

      if (upstreamFrameData.isEmpty) {
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
        
        if (varintBytesConsumed <= 0) {
          print('Error: Varint decoding resulted in 0 or negative bytes consumed from $peerId.');
          return;
        }

        if (varintBytesConsumed > upstreamFrameData.length) {
            print('Error: Varint prefix itself is longer than received data from $peerId.');
            return;
        }

      } catch (e) {
        print('Error decoding varint prefix from $peerId. Data: $upstreamFrameData. Error: $e');
        return;
      }
      
      // 2. Check if we have the full message payload
      if (upstreamFrameData.length < varintBytesConsumed + messageLength) {
        print('Received data frame from $peerId is too short for declared message length. Frame: ${upstreamFrameData.length}, Varint: $varintBytesConsumed, MsgLen: $messageLength');
        // This implies the "upstream framing" is not giving us one full Bitswap message + prefix,
        // or there's an issue with varint decoding.
        return;
      }

      // 3. Extract the protobuf payload
      final messagePayloadBytes = Uint8List.sublistView(
          upstreamFrameData, varintBytesConsumed, varintBytesConsumed + messageLength);

      // 4. Deserialize the message
      final message = pb.Message.fromBuffer(messagePayloadBytes);
      _messageHandler(peerId, message);

      // TODO: If upstreamFrameData can contain more data after this Bitswap message,
      // that needs to be handled (e.g., if multiple Bitswap messages are packed into one upstream frame).
      // For now, assuming one Bitswap message per upstream frame.
      if (upstreamFrameData.length > varintBytesConsumed + messageLength) {
          print("WARN: Extra data received in upstream frame after Bitswap message from $peerId. Ignoring ${upstreamFrameData.length - (varintBytesConsumed + messageLength)} bytes.");
      }

    } catch (e) {
      print('Error processing message from $peerId on stream: $e');
      // Consider closing the stream or other error handling.
    } finally {
      // If P2PStream is single-message, it might be closed by the libp2p layer after read().
      // If it's a persistent stream, we might not close it here.
      // For now, let's assume the stream lifecycle is managed by the caller or libp2p.
    }
  }

  Future<void> sendMessage(String peerId, pb.Message message) async {
    print('Attempting to send Bitswap message to $peerId.');
    libp2p.P2PStream? stream = activeStreams[peerId]; // Renamed

    try {
      if (stream == null) {
        print('No active stream to $peerId, opening new one.');
        final peerIdObj = libp2p.PeerId.fromString(peerId);
        stream = await _host.newStream(peerIdObj, [protocolId], libp2p.Context());
        activeStreams[peerId] = stream; // Renamed
        // TODO: Listen for stream close/error events to remove it from activeStreams
        // For example, stream.onClose(() => activeStreams.remove(peerId));
        // or handle errors in the write operation below to remove the stream.
      } else {
        print('Reusing existing stream to $peerId.');
      }

      final messageBytes = message.writeToBuffer();
      final lengthPrefix = dart_cid.encodeVarint(messageBytes.length);

      await stream.write(lengthPrefix);
      await stream.write(messageBytes);
      
      // DO NOT close the stream here if we want to reuse it.
      // stream.close(); 
      print('Bitswap message sent to $peerId using stream ${stream.id}.');

    } catch (e) {
      print('Error sending Bitswap message to $peerId: $e');
      // If an error occurs, the stream might be unusable. Remove it.
      if (stream != null) {
        activeStreams.remove(peerId); // Renamed
        try {
          await stream.close(); // Attempt to close it gracefully
        } catch (closeError) {
          print('Error closing stream after send error: $closeError');
        }
      }
    }
  }
}
