import 'dart:async';
import 'dart:typed_data';

import 'package:dart_libp2p/dart_libp2p.dart' as libp2p;
// Assuming 'varint' package for unsigned varint encoding/decoding
// If not available, a helper function would be needed.
import 'package:dcid/dcid.dart' as dart_cid; // Using varint from dart_libp2p
import 'package:logging/logging.dart';
// Placeholder for varint, actual package might differ or need implementation

import '../proto/bitswap_message.pb.dart' as pb;

final _log = Logger('bitswap.network');

// Callback type for when a full Bitswap message is received
typedef BitswapMessageHandler = Future<void> Function(libp2p.PeerId peerId, pb.Message message);

/// Largest Bitswap message we accept or send (matches libp2p's
/// network.MessageSizeMax used by go-bitswap / boxo).
const int maxBitswapMessageSize = 4 * 1024 * 1024;

class BitswapNetworkService {
  final libp2p.Host _host;
  final BitswapMessageHandler _messageHandler;
  final String protocolId = '/ipfs/bitswap/1.2.0'; // Bitswap 1.2.0 protocol ID

  // Manage active outgoing streams to peers
  final Map<libp2p.PeerId, libp2p.P2PStream> activeStreams = {};

  // Tail of each peer's outgoing send chain; see sendMessage
  final Map<libp2p.PeerId, Future<void>> _sendQueues = {};

  // Inbound streams currently being read, so stop() can end their loops
  final Set<libp2p.P2PStream> _inboundStreams = {};
  bool _stopped = false;

  BitswapNetworkService(this._host, this._messageHandler) {
    _log.fine('BitswapNetworkService instance created.');
  }

  Future<void> start() async {
    _host.setStreamHandler(protocolId, onIncomingStream); // Renamed
    _log.fine('Bitswap protocol handler registered for $protocolId');
    // TODO: Potentially discover existing peers and establish connections
    // or wait for them to connect to us.
  }

  Future<void> stop() async {
    if (_stopped) return;
    _stopped = true;
    _host.removeStreamHandler(protocolId);
    _log.fine('Bitswap protocol handler unregistered for $protocolId');
    // Reset inbound streams so their read loops terminate
    for (final stream in _inboundStreams.toList()) {
      try {
        await stream.reset();
      } catch (e) {
        _log.warning('Error resetting inbound stream ${stream.id()} during stop: $e');
      }
    }
    _inboundStreams.clear();
    _sendQueues.clear();
    // Close any active outgoing streams. Take them out of the map first:
    // closing one ends its watcher's read, and the watcher edits the map.
    final outgoing = activeStreams.values.toList();
    activeStreams.clear();
    for (final stream in outgoing) {
      try {
        await stream.close();
      } catch (e) {
        _log.warning('Error closing stream ${stream.id()} during stop: $e');
      }
    }
    _log.fine('All active streams cleared.');
  }

  // Called by Bitswap when a peer disconnects to clean up the stream
  Future<void> handlePeerDisconnected(libp2p.PeerId peerId) async {
    final stream = activeStreams.remove(peerId); // Renamed
    if (stream != null) {
      _log.fine('Peer $peerId disconnected, closing and removing active stream ${stream.id()}.');
      try {
        await stream.close();
      } catch (e) {
        _log.warning('Error closing stream for disconnected peer $peerId: $e');
      }
    } else {
      _log.fine('handlePeerDisconnected called for $peerId, but no active stream found.');
    }
  }

  // Handles a new incoming libp2p stream for the Bitswap protocol
  // This method's signature might still need adjustment based on your Host.setStreamHandler
  Future<void> onIncomingStream(libp2p.P2PStream stream, libp2p.PeerId peerId) async {
    _log.fine('Received incoming Bitswap stream ${stream.id()} from $peerId');
    if (_stopped) {
      await stream.reset();
      return;
    }
    _inboundStreams.add(stream);

    const maxVarintLen = 9; // Max length for a 64-bit varint
    // Buffer that carries leftover bytes between messages
    Uint8List leftover = Uint8List(0);

    try {
      // Outer loop: process multiple messages on the same stream
      while (true) {
        final BytesBuilder bytesBuilder = BytesBuilder();
        int? messageLength;
        int? varintBytesConsumed;

        // Seed with leftover from previous message, if any
        if (leftover.isNotEmpty) {
          bytesBuilder.add(leftover);
          leftover = Uint8List(0);
        }

        // Inner loop: read until we can decode the varint length prefix
        bool streamClosed = false;
        while (messageLength == null) {
          // Try to decode varint from what we have so far
          if (bytesBuilder.length >= 1) {
            try {
              final decodedVarint = dart_cid.decodeVarint(bytesBuilder.toBytes());
              messageLength = decodedVarint.key;
              varintBytesConsumed = decodedVarint.value;
              _log.finest('Decoded varint from $peerId. MessageLength: $messageLength, VarintBytesConsumed: $varintBytesConsumed. Buffered: ${bytesBuilder.length}');

              if (varintBytesConsumed <= 0) {
                _log.warning('Error: Varint decoding resulted in 0 or negative bytes consumed from $peerId.');
                return;
              }
              break; // Successfully decoded varint
            } catch (e) {
              if (bytesBuilder.length >= maxVarintLen) {
                _log.warning('Error decoding varint prefix from $peerId after ${bytesBuilder.length} bytes. Error: $e');
                return;
              }
              // Not enough bytes yet, continue reading below
            }
          }

          final Uint8List chunk = await stream.read();
          if (chunk.isEmpty) {
            streamClosed = true;
            break;
          }
          bytesBuilder.add(chunk);
          _log.finest('Read chunk of ${chunk.length} bytes from $peerId. Total buffered: ${bytesBuilder.length}');
        }

        if (streamClosed) {
          if (bytesBuilder.isEmpty) {
            _log.fine('Stream from $peerId closed cleanly (no pending data).');
          } else {
            _log.fine('Stream from $peerId closed with ${bytesBuilder.length} incomplete bytes. Discarding.');
          }
          return;
        }

        // At this point, messageLength and varintBytesConsumed are set.
        if (messageLength! > maxBitswapMessageSize) {
          _log.warning('Message from $peerId declares $messageLength bytes, exceeding limit of $maxBitswapMessageSize. Resetting stream.');
          await stream.reset();
          return;
        }
        final int totalExpectedLength = varintBytesConsumed! + messageLength;
        _log.finest('Expecting total message length (prefix + payload) of $totalExpectedLength bytes from $peerId.');

        // Read the rest of the message body if not all received yet
        while (bytesBuilder.length < totalExpectedLength) {
          final Uint8List chunk = await stream.read();
          if (chunk.isEmpty) {
            _log.fine('Stream from $peerId closed while reading message body. Expected $totalExpectedLength, got ${bytesBuilder.length}.');
            return;
          }
          bytesBuilder.add(chunk);
          _log.finest('Read body chunk of ${chunk.length} bytes from $peerId. Total buffered: ${bytesBuilder.length} / $totalExpectedLength');
        }

        final Uint8List allReceivedData = bytesBuilder.toBytes();

        // Extract the message payload
        final messagePayloadBytes = Uint8List.sublistView(
            allReceivedData, varintBytesConsumed, totalExpectedLength);
        _log.finest('Extracted message payload of ${messagePayloadBytes.length} bytes for $peerId.');

        // Deserialize and handle the message
        final message = pb.Message.fromBuffer(messagePayloadBytes);
        _log.finest('Deserialized message from $peerId. Calling message handler.');
        if (_stopped) return;
        await _messageHandler(peerId, message);

        // Carry forward any leftover bytes for the next message
        if (allReceivedData.length > totalExpectedLength) {
          leftover = Uint8List.sublistView(allReceivedData, totalExpectedLength);
          _log.finest('${leftover.length} leftover bytes carried to next message from $peerId.');
        }
        // Loop back to process the next message
      }
    } catch (e, s) {
      _log.warning('Error processing message from $peerId on stream ${stream.id()}', e, s);
    } finally {
      _inboundStreams.remove(stream);
      _log.fine('Finished processing incoming stream ${stream.id()} from $peerId.');
    }
  }

  /// Sends [message] to [peerId].
  ///
  /// Sends to the same peer are serialised in call order, so messages are
  /// never reordered and concurrent callers share a single stream. The
  /// returned future completes once this message has been written (or has
  /// failed); it never throws — failures are logged and the message dropped.
  Future<void> sendMessage(libp2p.PeerId peerId, pb.Message message) {
    if (_stopped) return Future<void>.value();
    final previous = _sendQueues[peerId] ?? Future<void>.value();
    final next = previous.then((_) => _sendNow(peerId, message));
    _sendQueues[peerId] = next;
    next.whenComplete(() {
      if (identical(_sendQueues[peerId], next)) _sendQueues.remove(peerId);
    });
    return next;
  }

  Future<void> _sendNow(libp2p.PeerId peerId, pb.Message message) async {
    if (_stopped) return;
    _log.finest('Sending to $peerId. Wantlist: ${message.hasWantlist()}, Payloads: ${message.payload.length}, Presences: ${message.blockPresences.length}');

    final messageBytes = message.writeToBuffer();
    final lengthPrefix = dart_cid.encodeVarint(messageBytes.length);
    // Prefix + body in a single write to avoid partial message delivery
    final frame = Uint8List(lengthPrefix.length + messageBytes.length)
      ..setAll(0, lengthPrefix)
      ..setAll(lengthPrefix.length, messageBytes);

    // A reused stream may have been closed by the remote since our last
    // write without us noticing yet, so a failure on one is retried once on
    // a fresh stream.
    for (var attempt = 0; attempt < 2; attempt++) {
      var existing = activeStreams[peerId];
      if (existing != null && existing.isClosed) {
        activeStreams.remove(peerId);
        existing = null;
      }
      libp2p.P2PStream stream;
      if (existing != null) {
        stream = existing;
      } else {
        try {
          stream = await _host.newStream(peerId, [protocolId], libp2p.Context());
        } catch (e) {
          _log.warning('Failed to open stream to $peerId: $e');
          return;
        }
        activeStreams[peerId] = stream;
        _log.fine('Opened stream ${stream.id()} to $peerId.');
        unawaited(_watchOutgoing(peerId, stream));
      }

      try {
        await stream.write(frame);
        _log.finest('Sent ${frame.length} bytes to $peerId on stream ${stream.id()}.');
        return;
      } catch (e) {
        _log.warning('Error writing to $peerId on stream ${stream.id()}: $e');
        if (identical(activeStreams[peerId], stream)) activeStreams.remove(peerId);
        try {
          await stream.close();
        } catch (closeError) {
          _log.warning('Error closing stream ${stream.id()} after send error: $closeError');
        }
        if (existing == null) return; // Fresh stream failed too; give up
      }
    }
  }

  /// Drops [stream] from the cache once the remote closes or resets it, so
  /// the next send opens a fresh stream instead of first failing a write.
  /// Bitswap peers never write on our outgoing streams, so a read that ends
  /// (EOF) or fails means the remote is done with the stream.
  Future<void> _watchOutgoing(libp2p.PeerId peerId, libp2p.P2PStream stream) async {
    try {
      while ((await stream.read()).isNotEmpty) {}
    } catch (_) {
      // Reset by the remote, or closed by us
    }
    if (!identical(activeStreams[peerId], stream)) return; // Already replaced or closed by us
    _log.fine('Stream ${stream.id()} to $peerId was closed by the remote; dropping it.');
    activeStreams.remove(peerId);
    try {
      await stream.close();
    } catch (_) {
      // Already gone
    }
  }
}
