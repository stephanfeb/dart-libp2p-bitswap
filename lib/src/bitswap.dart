import 'dart:async'; // Added for StreamSubscription
import 'dart:typed_data';

import 'package:dart_libp2p/dart_libp2p.dart' as libp2p; // Assuming this is the corrected import
import 'package:dart_cid/dart_cid.dart' as dart_cid; // Using alias for clarity with CID class
import 'package:dart_multihash/dart_multihash.dart' as dart_multihash;
import 'package:crypto/crypto.dart' as crypto;

import './proto/bitswap_message.pb.dart' as pb;
import './want_manager.dart';
import './network_service.dart';
import 'package:convert/convert.dart'; // For hex encoding in logs

// Interface for a block store
abstract class IBlockStore {
  Future<bool> has(dart_cid.CID cid);
  Future<Uint8List?> get(dart_cid.CID cid);
  Future<void> put(dart_cid.CID cid, Uint8List data);
}

class Bitswap {
  // Helper to keep track of peers known to support Bitswap.
  // This could be populated when streams are initiated or messages are received.
  // For simplicity, not fully managed here yet.
  final Set<String> _activeBitswapPeers = {}; 
  final libp2p.Host _host;
  final IBlockStore _blockStore;
  late final WantManager _wantManager;
  late final BitswapNetworkService _networkService;
  // Using dynamic for the subscription type as the event type from bus might be generic
  StreamSubscription<dynamic>? _connectednessChangeSubscription;

  Bitswap({
    required libp2p.Host host,
    required IBlockStore blockStore,
    required Stream<dynamic> peerEventsStream, // Added stream parameter
  })  : _host = host,
        _blockStore = blockStore {
    _wantManager = WantManager();
    _networkService = BitswapNetworkService(_host, handleIncomingMessage); // Updated method name
    _networkService.start(); // Start listening for network messages

    // Listen to the provided stream for peer connectedness changes
    try {
      _connectednessChangeSubscription = peerEventsStream.listen((event) {
        // The stream is already typed, so direct access should be fine.
        // No need for 'if (event is libp2p.EvtPeerConnectednessChanged)' if stream is correctly typed.
        if (event is libp2p.EvtPeerConnectednessChanged) {
          if (event.connectedness == libp2p.Connectedness.notConnected) {
            onPeerDisconnected(event.peer); // Updated method name
          } else if (event.connectedness == libp2p.Connectedness.connected) {
            // Handle peer connected if needed in the future
            print('Bitswap: Peer connected: ${event.peer.toString()}');
          }
        }
      });
      print('Bitswap: Listening to provided peer connectedness event stream.');
    } catch (e) {
      print('Bitswap: Error listening to peer connectedness event stream: $e. Disconnection cleanup might not work.');
    }
    print('Bitswap instance created and network service started.');
  }

  Future<void> stop() async {
    print('Bitswap stopping...');
    await _connectednessChangeSubscription?.cancel(); // Reverted to cancel()
    await _networkService.stop();
    print('Bitswap stopped.');
  }
  
  // Made package-private for testability. Ideally, use DI or other patterns.
  void onPeerDisconnected(libp2p.PeerId peerId) {
    final peerIdStr = peerId.toString();
    print('Bitswap: Peer disconnected: $peerIdStr. Cleaning up...');
    _activeBitswapPeers.remove(peerIdStr);
    _wantManager.clearPeerWantlist(peerIdStr);
    // Also, tell the network service to clean up any streams for this peer.
    _networkService.handlePeerDisconnected(peerIdStr);
    // TODO: Consider if local wants that were only being pursued with this peer need re-evaluation.
  }

  // This method will be passed to BitswapNetworkService
  // Made package-private for testability. Ideally, use DI or other patterns.
  void handleIncomingMessage(String peerId, pb.Message message) {
    // Process wantlist entries
    if (message.hasWantlist()) {
      _handleWantlist(peerId, message.wantlist);
    }

    // Process received blocks
    for (final block in message.payload) {
      _handleReceivedBlock(peerId, block);
    }

    // Process block presences (HAVE/DONT_HAVE)
    for (final presence in message.blockPresences) {
      _handleBlockPresence(peerId, presence);
    }

    // Handle pending bytes (informational for now)
    if (message.hasPendingBytes()) {
      print('Peer $peerId has ${message.pendingBytes} pending bytes for us.');
    }
  }

  void _handleWantlist(String peerId, pb.Message_Wantlist wantlist) async {
    print('Received wantlist from $peerId: ${wantlist.entries.length} entries, full: ${wantlist.full}');
    
    // When we successfully handle a message from a peer, consider them an active Bitswap peer.
    _activeBitswapPeers.add(peerId);

    if (wantlist.full) {
      print('Received full wantlist from $peerId. Clearing existing wants for this peer.');
      // Assuming WantManager has a method to clear all wants for a specific peer.
      // This method will need to be implemented in WantManager if it doesn't exist.
      _wantManager.clearPeerWantlist(peerId); 
    }

    for (final entry in wantlist.entries) {
      dart_cid.CID cid; 
      try {
        cid = dart_cid.CID.fromBytes(Uint8List.fromList(entry.block));
      } catch (e) {
        print('Error parsing CID from wantlist entry from $peerId: $e');
        continue; // Skip this invalid entry
      }

      if (entry.cancel) {
        print('Peer $peerId cancelled want for CID $cid');
        _wantManager.removePeerWant(peerId, cid);
        // TODO: Stop any in-progress sending efforts for this block to this peer
      } else {
        print('Peer $peerId wants CID $cid, type: ${entry.wantType}, priority: ${entry.priority}, sendDontHave: ${entry.sendDontHave}');
        _wantManager.addPeerWant(
          peerId,
          cid,
          priority: entry.priority,
          wantType: entry.wantType,
          sendDontHave: entry.sendDontHave,
        );

        // Respond to the want
        bool hasBlock = await _blockStore.has(cid);

        if (entry.wantType == pb.Message_Wantlist_WantType.Block) {
          if (hasBlock) {
            final blockData = await _blockStore.get(cid);
            if (blockData != null) {
              // final prefix = [cid.codec, codecNameToCode[cid.codecName]];
              BytesBuilder prefixBuilder = BytesBuilder() ;
              // Construct prefix as per dart-cid's CID.toBytes() for CIDv1 (version, codec, multihash_code, multihash_len)
              // This assumes the user's previous fix for cid.prefix was to provide these components.
              // The `blockProto.prefix` should be version, codec, multihash_type_code, multihash_digest_length
              // This part needs to align with how `blockProto.prefix` is defined by Bitswap spec.
              // Spec: "CID prefix (all of the CID components except for the digest of the multihash)"
              // This means: cidV1: [version][codec][mh_type][mh_len]
              //             cidV0: empty or [mh_type=sha256][mh_len=32] (ambiguous in spec, but CIDv0 IS a multihash)
              // For sending, we need to provide what the spec means by "prefix".
              // Your previous code for constructing prefix was:
              // prefix.add(hex.decode(cid.version.toRadixString(16)));
              // prefix.add(hex.decode(cid.codec.toRadixString(16)));
              // prefix.add(hex.decode(codecNameToCode[cid.codecName]?.toRadixString(16) ?? '00'));
              // This seems to be constructing [version][codec][multihash_type_code] if codecNameToCode maps to mh_type.
              // This is NOT the full CID prefix as per Bitswap spec if it needs mh_len.
              // Let's assume for now your `Cid` object's `cid.toBytes()` gives the full CID,
              // and `cid.multihash` gives the full multihash.
              // The `blockProto.prefix` should be the CID bytes *excluding* the digest part of the multihash.

              // Re-evaluating: The `prefix` field in `pb.Message_Block` is for the *sender* to provide.
              // The sender (our node) should construct this prefix correctly.
              // Your previous logic for constructing `prefix.toBytes()` was:
              //   version_bytes + codec_bytes + multihash_type_code_bytes
              // This seems like a plausible interpretation of "CID components except for the digest".
              // Let's stick to your existing logic for constructing the prefix to send.
              prefixBuilder.add(dart_cid.encodeVarint(cid.version)); 
              prefixBuilder.add(dart_cid.encodeVarint(cid.codec));
              
              final decodedMh = dart_multihash.Multihash.decode(cid.multihash);
              prefixBuilder.add(dart_cid.encodeVarint(decodedMh.code));
              prefixBuilder.add(dart_cid.encodeVarint(decodedMh.digest.length)); // Use digest.length for the length of the digest

              final blockProto = pb.Message_Block()
                ..prefix = prefixBuilder.toBytes()
                ..data = blockData;
              final responseMessage = pb.Message()..payload.add(blockProto);
              print('Sending block $cid to $peerId');
              _networkService.sendMessage(peerId, responseMessage);
            }
          } else if (entry.sendDontHave) {
            final presenceProto = pb.Message_BlockPresence()
              ..cid = cid.toBytes()
              ..type = pb.Message_BlockPresenceType.DontHave;
            final responseMessage = pb.Message()..blockPresences.add(presenceProto);
            print('Sending DONT_HAVE for $cid to $peerId');
            _networkService.sendMessage(peerId, responseMessage);
          }
        } else if (entry.wantType == pb.Message_Wantlist_WantType.Have) {
          if (hasBlock) {
            final presenceProto = pb.Message_BlockPresence()
              ..cid = cid.toBytes()
              ..type = pb.Message_BlockPresenceType.Have;
            final responseMessage = pb.Message()..blockPresences.add(presenceProto);
            print('Sending HAVE for $cid to $peerId');
            _networkService.sendMessage(peerId, responseMessage);
          } else if (entry.sendDontHave) {
            final presenceProto = pb.Message_BlockPresence()
              ..cid = cid.toBytes()
              ..type = pb.Message_BlockPresenceType.DontHave;
            final responseMessage = pb.Message()..blockPresences.add(presenceProto);
            print('Sending DONT_HAVE for $cid to $peerId');
            _networkService.sendMessage(peerId, responseMessage);
          }
        }
      }
    }
  }

  void _handleReceivedBlock(String peerId, pb.Message_Block blockProto) async {
    print('Received block from $peerId. Prefix length: ${blockProto.prefix.length}, Data length: ${blockProto.data.length}.');

    final Uint8List prefixBytes = Uint8List.fromList(blockProto.prefix);
    final Uint8List dataBytes = Uint8List.fromList(blockProto.data);

    dart_cid.CID? receivedCid;

    try {
      // Step 1: Parse `prefixBytes` to get CID version, codec, and multihash type code + digest length.
      // The prefix for CIDv1 is: [version][codec][mh_type][mh_len_of_digest]
      // The prefix for CIDv0 is effectively empty or just [mh_type=sha256][mh_len=32]
      // as CIDv0 itself is a multihash.
      
      int offset = 0;
      final versionEntry = dart_cid.decodeVarint(prefixBytes, offset);
      int cidVersion = versionEntry.key;
      offset += versionEntry.value;

      int cidCodec;
      int multihashCode;
      int digestLength; // Length of the digest that was hashed from data, not length of mh itself

      if (cidVersion == dart_cid.CID.V1) {
        final codecEntry = dart_cid.decodeVarint(prefixBytes, offset);
        cidCodec = codecEntry.key;
        offset += codecEntry.value;

        final mhCodeEntry = dart_cid.decodeVarint(prefixBytes, offset);
        multihashCode = mhCodeEntry.key;
        offset += mhCodeEntry.value;

        final mhLenEntry = dart_cid.decodeVarint(prefixBytes, offset);
        digestLength = mhLenEntry.key;
        offset += mhLenEntry.value;

        if (offset != prefixBytes.length) {
          print('WARN: CIDv1 prefix bytes length mismatch after parsing. Expected $offset, got ${prefixBytes.length}');
          // This might indicate an issue or a non-standard prefix.
        }
      } else if (cidVersion == dart_cid.CID.V0) {
        // For CIDv0, the prefix sent by another Bitswap node might be ambiguous.
        // If it's a full CIDv0 multihash, it would be [0x12, 0x20, ...digest...].
        // If `blockProto.prefix` is just the mh_type and mh_len for a CIDv0:
        if (prefixBytes.length >= 2) { // Assuming at least type and len
            // For CIDv0, prefix from sender might be just mh_type and mh_len
            // Or it could be an attempt to send the full CIDv0 multihash as prefix if data is empty (not typical)
            // Let's assume prefix contains mh_type and mh_len for CIDv0 if version is 0.
            final mhCodeEntryV0 = dart_cid.decodeVarint(prefixBytes, offset);
            multihashCode = mhCodeEntryV0.key;
            offset += mhCodeEntryV0.value;

            final mhLenEntryV0 = dart_cid.decodeVarint(prefixBytes, offset);
            digestLength = mhLenEntryV0.key;
            offset += mhLenEntryV0.value;
            
            cidCodec = dart_cid.codecNameToCode['dag-pb']!; // CIDv0 is implicitly dag-pb
            
            // Validate for CIDv0 (sha2-256, 32-byte digest)
            if (multihashCode != dart_cid.codecNameToCode['sha2-256'] || digestLength != 32) {
                 print('WARN: Received CIDv0 prefix components (mhCode: $multihashCode, digestLen: $digestLength) do not conform to sha2-256 32-byte digest.');
                 return; // Cannot reliably reconstruct
            }
        } else {
            print('ERROR: Received CIDv0 prefix is too short. Prefix: ${hex.encode(prefixBytes)}');
            return;
        }
      } else {
        print('ERROR: Unknown CID version $cidVersion parsed from prefix.');
        return;
      }

      // Step 2: Compute the multihash digest of `dataBytes`.
      Uint8List digest;
      String? multihashName = dart_cid.codecCodeToName[multihashCode];
      
      if (multihashName == null) {
        print('ERROR: Unknown multihash code $multihashCode from prefix. Cannot determine hash algorithm.');
        return;
      }

      if (multihashName == 'sha2-256') {
        digest = Uint8List.fromList(crypto.sha256.convert(dataBytes).bytes);
      } else if (multihashName == 'sha2-512') {
        digest = Uint8List.fromList(crypto.sha512.convert(dataBytes).bytes);
      } else {
        print('ERROR: Unsupported multihash type code $multihashCode ($multihashName) for hashing received block data.');
        return;
      }

      if (digest.length != digestLength) {
        print('ERROR: Computed digest length ${digest.length} does not match digest length from prefix $digestLength.');
        return;
      }

      // Step 3: Construct the full Multihash (code + length_of_digest + digest)
      final fullMultihashBytes = dart_multihash.Multihash.encode(multihashName, digest).toBytes();
      
      // Step 4: Construct the full `CID` object.
      receivedCid = dart_cid.CID(cidVersion, cidCodec, fullMultihashBytes);

      print('Successfully reconstructed CID: $receivedCid from $peerId.');

      if (_wantManager.isWantingLocally(receivedCid)) { 
        await _blockStore.put(receivedCid, dataBytes);
        _wantManager.blockReceived(receivedCid, dataBytes); 
        print('Stored block $receivedCid from $peerId');
        
        _sendCancelMessages(receivedCid);
      } else {
        print('Received unsolicited block $receivedCid from $peerId. Prefix: ${hex.encode(prefixBytes)}');
      }
    } catch (e) {
      print('Error processing received block from $peerId: $e. Prefix: ${hex.encode(prefixBytes)}, DataLen: ${dataBytes.length}');
    }
  }

  void _handleBlockPresence(String peerId, pb.Message_BlockPresence presence) {
    try {
      final cid = dart_cid.CID.fromBytes(Uint8List.fromList(presence.cid)); 
      _activeBitswapPeers.add(peerId);
      print('Received block presence from $peerId for CID $cid: ${presence.type}');
      if (presence.type == pb.Message_BlockPresenceType.Have) {
        _wantManager.receivedHave(peerId, cid);
      } else if (presence.type == pb.Message_BlockPresenceType.DontHave) {
        _wantManager.receivedDontHave(peerId, cid);
      }
    } catch (e) {
      print('Error parsing CID from block presence from $peerId: $e');
    }
  }

  // Method to request blocks from the network
  Future<void> wantBlocks(List<dart_cid.CID> cidsToWant) async { 
    if (cidsToWant.isEmpty) return;

    final wantlistEntries = <pb.Message_Wantlist_Entry>[];
    for (final cid in cidsToWant) {
      final entry = _wantManager.addLocalWant(cid); // Default priority, wantType=Block, sendDontHave=false
      if (entry != null) { // Only add if it's a new want or type changed
        wantlistEntries.add(pb.Message_Wantlist_Entry()
          ..block = cid.toBytes()
          ..wantType = pb.Message_Wantlist_WantType.Block // Or make configurable
          // priority, sendDontHave can be added if needed
        );
      }
    }
    
    if (wantlistEntries.isEmpty) {
      print('No new wants to send.');
      return;
    }

    final wantlistProto = pb.Message_Wantlist()..entries.addAll(wantlistEntries);
    final message = pb.Message()..wantlist = wantlistProto;

    print('Wanting ${wantlistEntries.length} blocks. Broadcasting wantlist.');
    
    final List<String> bitswapPeers = await _getBitswapPeers(); // Added await

    if (bitswapPeers.isEmpty) {
      print('No known Bitswap peers to send wantlist to.');
      // TODO: Potentially queue the wantlist or try discovery / connect to seed peers.
    }

    // If we are already connected to peers, send the wantlist immediately.
    // Otherwise, the want is in the wantlist and will be sent when peers connect.
    for (final peerId in bitswapPeers) {
      print('Sending wantlist to peer $peerId for ${wantlistEntries.length} CIDs.');
      _networkService.sendMessage(peerId, message);
      // Record that we've sent these wants to this peer
      for (final entryProto in wantlistEntries) {
        // We need the CID object to record it in WantManager
        // This assumes entryProto.block are valid CID bytes
        try {
            final cidForRecording = dart_cid.CID.fromBytes(Uint8List.fromList(entryProto.block));
            _wantManager.recordWantSentToPeer(cidForRecording, peerId);
        } catch (e) {
            print('Error creating CID for recording sent want to $peerId: $e');
        }
      }
    }
  }

  Future<List<String>> _getBitswapPeers() async { // Made async, returns Future
    final Set<String> peers = {}; // Use a set to avoid duplicates

    // Prioritize already known active Bitswap peers
    peers.addAll(_activeBitswapPeers);

    // Discover other connected peers supporting Bitswap
    // Assumes dart_libp2p Host API:
    // - _host.connections() (or similar) returns Iterable<libp2p.Connection>
    // - connection.remotePeer returns libp2p.PeerId
    // - _host.peerStore.getPeer(peerId) should return a PeerInfo-like object.
    // - PeerInfo object should have a `protocols` getter (Set<String>).
    try {
      // This assumes _host.connectionManager.activeConnections returns an Iterable.
      // And conn.remotePeer is a PeerId object.
      // And _host.peerStore.getPeer(PeerId) returns a Future<PeerInfo?>.
      final connections = _host.network.conns;
      for (final conn in connections) {
        final remotePeerId = conn.remotePeer; // This should be a libp2p.PeerId
        final peerInfo = await _host.peerStore.getPeer(remotePeerId); // Added await
        
        if (peerInfo != null && peerInfo.protocols.contains(_networkService.protocolId)) {
          peers.add(remotePeerId.toBase58()); // Use toBase58() instead of toString()
        }
      }
    } catch (e) {
      print('Error querying connected peers for Bitswap support: $e. Falling back to known active peers.');
      // If there's an error (e.g., API assumptions are wrong),
      // we rely on the initially added _activeBitswapPeers.
      // If _activeBitswapPeers was empty, peers will remain empty or contain only those from _activeBitswapPeers.
    }

    if (peers.isEmpty && _activeBitswapPeers.isEmpty) { // Adjusted condition to avoid warning if _activeBitswapPeers had content
      print('WARN: _getBitswapPeers() found no Bitswap-supporting peers after checking active and connected peers.');
    }
    return peers.toList();
  }

  void _sendCancelMessages(dart_cid.CID cid) { 
    // Construct a CANCEL message
    final cancelEntry = pb.Message_Wantlist_Entry()
      ..block = cid.toBytes()
      ..cancel = true;
    final cancelWantlist = pb.Message_Wantlist()..entries.add(cancelEntry);
    final cancelMessage = pb.Message()..wantlist = cancelWantlist;

    final Set<String> peersToCancel = _wantManager.getPeersSentWantForCid(cid);

    if (peersToCancel.isEmpty) {
      print('No peers to send CANCEL for CID $cid.');
      return;
    }

    for (final peerId in peersToCancel) {
      print('Sending CANCEL for CID $cid to peer $peerId');
      _networkService.sendMessage(peerId, cancelMessage);
    }
  }

  // TODO: Add methods for handling peer connections/disconnections for wantlist cleanup etc.
  // For example, when a peer disconnects, their wants in _peerWantlists should be cleared.
  // Also, if we disconnect from a peer, we might need to re-evaluate our local wants.

  /// Retrieves a block by its CID.
  ///
  /// This method will first check the local block store. If the block is not found,
  /// it will add the CID to the wantlist and attempt to fetch it from the network.
  /// It returns a Future that completes with the block data (Uint8List) when available,
  /// or null if the block cannot be retrieved (e.g., not found after timeout - timeout not yet implemented).
  Future<Uint8List?> getBlock(dart_cid.CID cid) async {
    // 1. Check local store first
    if (await _blockStore.has(cid)) {
      print('Block $cid found in local store.');
      return _blockStore.get(cid);
    }

    print('Block $cid not in local store. Adding to wantlist and attempting to fetch.');
    // 2. Add to wantlist (this ensures a Completer is ready in WantManager)
    // wantBlocks will call addLocalWant internally.
    // We call wantBlocks to ensure the want is broadcast if necessary.
    await wantBlocks([cid]);

    // 3. Get the future from WantManager that will complete when the block is received.
    // This relies on wantBlocks having successfully added the want to the _wantManager.
    try {
      // Add a timeout to prevent waiting indefinitely
      // TODO: Make timeout configurable
      return await _wantManager.whenBlockReceived(cid).timeout(const Duration(seconds: 30));
    } on TimeoutException {
      print('Timeout waiting for block $cid');
      _wantManager.removeLocalWant(cid); // Clean up the want if it timed out
      return null;
    } catch (e) {
      print('Error while waiting for block $cid: $e');
      _wantManager.removeLocalWant(cid); // Clean up
      return null;
    }
  }
}
