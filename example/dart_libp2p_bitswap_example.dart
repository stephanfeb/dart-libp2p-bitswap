import 'package:dart_libp2p_bitswap/dart_libp2p_bitswap.dart';
import 'package:dart_libp2p/dart_libp2p.dart' as libp2p; // For Host, PeerId etc.
import 'package:dart_cid/dart_cid.dart' as dart_cid; // For CID
import 'dart:typed_data';

// A simple in-memory block store implementation for example purposes
class InMemoryBlockStore implements IBlockStore {
  final Map<String, Uint8List> _storage = {};

  @override
  Future<Uint8List?> get(dart_cid.CID cid) async {
    print('[BlockStore] GET ${cid.toString()}');
    return _storage[cid.toString()];
  }

  @override
  Future<bool> has(dart_cid.CID cid) async {
    final present = _storage.containsKey(cid.toString());
    print('[BlockStore] HAS ${cid.toString()}: $present');
    return present;
  }

  @override
  Future<void> put(dart_cid.CID cid, Uint8List data) async {
    print('[BlockStore] PUT ${cid.toString()} (data length: ${data.length})');
    _storage[cid.toString()] = data;
  }
}

Future<void> main() async {
  print('Bitswap Example');

  // 1. Setup a libp2p Host (details depend on your dart_libp2p setup)
  // This is a highly simplified placeholder. A real host needs transports, security, muxers.
  // final libp2p.Host host = MinimalHost(libp2p.PeerId.random()); // Replace with actual host setup
  // await host.start();
  // print('Libp2p Host started with PeerId: ${host.peerId}');

  // 2. Implement IBlockStore
  final blockStore = InMemoryBlockStore();

  // 3. Instantiate Bitswap
  // Ensure your host is properly initialized and started before this.
  // Bitswap bitswap = Bitswap(host: host, blockStore: blockStore);
  // print('Bitswap service initialized.');

  // 4. Example: Adding a block to the local store
  // final exampleData = Uint8List.fromList('Hello Bitswap!'.codeUnits);
  // final exampleCid = await computeCid(exampleData); // You'd need a CID computation function
  // await blockStore.put(exampleCid, exampleData);
  // print('Added block ${exampleCid.toString()} to local store.');

  // 5. Example: Wanting a block from the network
  // final cidToWant = dart_cid.CID.parse('bafkreibm6jg3ux5qumhcn2b3flc3tyu6dmlb4xa7u5bf44yegnrjhc4yeq'); // Example CID
  // print('Attempting to want block: ${cidToWant.toString()}');
  // await bitswap.wantBlocks([cidToWant]);

  // Keep the example running for a bit to allow for network interactions
  // await Future.delayed(Duration(seconds: 30));

  // 6. Stop Bitswap and the Host
  // await bitswap.stop();
  // await host.stop();
  // print('Bitswap and Host stopped.');

  print('--------------------------------------------------------------------');
  print('NOTE: This example is a skeleton.');
  print('You need to replace placeholder Host setup with your actual libp2p Host initialization.');
  print('You may also need a function to compute CIDs for data to use wantBlocks effectively.');
  print('Uncomment and adapt sections to test Bitswap functionality.');
  print('--------------------------------------------------------------------');
}

// Helper function to compute CID (example, replace with actual CID generation)
/*
Future<dart_cid.CID> computeCid(Uint8List data) async {
  // This is a placeholder. Actual CID generation involves:
  // 1. Choosing a multihash (e.g., sha2-256)
  // 2. Hashing the data
  // 3. Encoding the hash as a multihash (prefixing with hash code and length)
  // 4. Choosing a CID version (e.g., v1)
  // 5. Choosing a codec (e.g., dag-pb for raw data)
  // 6. Combining version, codec, and multihash into a CID
  // Example using dart_multihash and dart_cid (ensure these packages are in pubspec.yaml)
  // final hash = crypto.sha256.convert(data);
  // final mh = dart_multihash.Multihash.encode('sha2-256', Uint8List.fromList(hash.bytes));
  // return dart_cid.CID(1, dart_cid.Codec.raw, mh.toBytes()); // Using 'raw' codec for raw bytes
}
*/
