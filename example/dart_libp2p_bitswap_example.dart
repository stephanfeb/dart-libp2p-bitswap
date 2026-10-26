// Two in-process libp2p hosts exchange a block over Bitswap.
//
// Run with: dart run example/dart_libp2p_bitswap_example.dart
import 'dart:io';
import 'dart:typed_data';

import 'package:dart_libp2p/config/config.dart' as p2p_config;
import 'package:dart_libp2p/core/crypto/ed25519.dart' as crypto_ed25519;
import 'package:dart_libp2p/dart_libp2p.dart';
import 'package:dart_libp2p/p2p/security/noise/noise_protocol.dart';
import 'package:dart_libp2p/p2p/transport/connection_manager.dart';
import 'package:dart_libp2p/p2p/transport/udx_transport.dart';
import 'package:dart_libp2p_bitswap/dart_libp2p_bitswap.dart';
import 'package:dart_udx/dart_udx.dart';
import 'package:dcid/dcid.dart' as dart_cid;
import 'package:logging/logging.dart';

/// A minimal in-memory [BlockStore].
class InMemoryBlockStore implements BlockStore {
  final Map<String, Uint8List> _blocks = {};

  @override
  Future<bool> has(dart_cid.CID cid) async =>
      _blocks.containsKey(cid.toString());

  @override
  Future<Uint8List?> get(dart_cid.CID cid) async => _blocks[cid.toString()];

  @override
  Future<void> put(dart_cid.CID cid, Uint8List data) async =>
      _blocks[cid.toString()] = data;
}

/// A host listening on a random local UDP port, with Noise security and
/// (by default) Yamux multiplexing. The default address factory drops
/// loopback addresses, which would leave these local hosts undialable.
Future<Host> createHost() async {
  final keyPair = await crypto_ed25519.generateEd25519KeyPair();
  final connManager = ConnectionManager();
  final host = await p2p_config.Libp2p.new_([
    p2p_config.Libp2p.identity(keyPair),
    p2p_config.Libp2p.connManager(connManager),
    p2p_config.Libp2p.transport(
        UDXTransport(connManager: connManager, udxInstance: UDX())),
    p2p_config.Libp2p.security(await NoiseSecurity.create(keyPair)),
    p2p_config.Libp2p.listenAddrs([MultiAddr('/ip4/127.0.0.1/udp/0/udx')]),
    p2p_config.Libp2p.addrsFactory((addrs) => addrs),
  ]);
  await host.start();
  return host;
}

Future<void> main() async {
  // Bitswap logs through package:logging; show its records only (the
  // libp2p stack is chatty)
  Logger.root.level = Level.FINE;
  Logger.root.onRecord
      .where((r) => r.loggerName.startsWith('bitswap'))
      .listen((r) => print('[${r.loggerName}] ${r.message}'));

  final provider = await createHost();
  final fetcher = await createHost();
  print('Provider: ${provider.id.toBase58()}');
  print('Fetcher:  ${fetcher.id.toBase58()}');

  // Wants made before the fetcher learns (via identify) that the provider
  // speaks Bitswap go out on the next rebroadcast, so keep it short here
  const options = BitswapOptions(rebroadcastInterval: Duration(seconds: 1));

  final providerStore = InMemoryBlockStore();
  final providerBitswap =
      Bitswap(host: provider, blockStore: providerStore, options: options);
  final fetcherBitswap = Bitswap(
      host: fetcher, blockStore: InMemoryBlockStore(), options: options);
  await providerBitswap.start();
  await fetcherBitswap.start();

  // The provider has a block...
  final data = Uint8List.fromList('Hello from Bitswap!'.codeUnits);
  final cid = dart_cid.CID.fromData(dart_cid.CID.V1, 'raw', data);
  await providerStore.put(cid, data);
  print('Provider stored $cid');

  // ...and the fetcher connects and asks for it
  await fetcher.connect(AddrInfo(provider.id, provider.addrs));
  final received =
      await fetcherBitswap.getBlock(cid, timeout: const Duration(seconds: 15));

  var ok = false;
  if (received == null) {
    print('Fetcher did not receive $cid in time');
  } else {
    ok = String.fromCharCodes(received) == String.fromCharCodes(data);
    print('Fetcher received: "${String.fromCharCodes(received)}"');
  }

  await fetcherBitswap.stop();
  await providerBitswap.stop();
  await fetcher.close();
  await provider.close();
  exit(ok ? 0 : 1);
}
