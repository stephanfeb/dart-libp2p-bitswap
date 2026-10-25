import 'dart:async';
import 'dart:typed_data';

import 'package:dart_libp2p/dart_libp2p.dart' as libp2p;
import 'package:dcid/dcid.dart' as dart_cid;
import 'package:logging/logging.dart';
import 'package:meta/meta.dart';

import './block_store.dart';
import './client/client.dart';
import './network/network_service.dart';
import './proto/bitswap_message.pb.dart' as pb;
import './server/engine.dart';

export './block_store.dart';

final _log = Logger('bitswap');

/// A Bitswap 1.2.0 node: fetches blocks from peers and serves blocks from a
/// [BlockStore]. Call [start] before use and [stop] when done.
///
/// This is a facade over [BitswapClient] (fetching) and [BitswapServer]
/// (serving), which share one [BitswapNetworkService].
class Bitswap {
  final libp2p.Host _host;
  final BitswapOptions options;
  late final BitswapNetworkService _network;
  late final BitswapClient _client;
  late final BitswapServer _server;
  libp2p.Subscription<dynamic>? _peerEvents;
  StreamSubscription<dynamic>? _peerEventsListener;
  bool _started = false;
  bool _stopped = false;

  /// Creates a Bitswap node for [host]. Has no side effects until [start].
  Bitswap({
    required libp2p.Host host,
    required BlockStore blockStore,
    this.options = const BitswapOptions(),
  }) : _host = host {
    _network = BitswapNetworkService(host, handleIncomingMessage);
    _server = BitswapServer(blockStore, _network);
    _client = BitswapClient(host, blockStore, _network, options, _server.blockAvailable);
  }

  /// Registers the Bitswap protocol handler and starts tracking peer
  /// connections. Idempotent; a stopped instance cannot be restarted.
  Future<void> start() async {
    if (_stopped) throw StateError('Bitswap stopped');
    if (_started) return;
    _started = true;
    await _network.start();

    final subscription = _host.eventBus.subscribe(libp2p.EvtPeerConnectednessChanged);
    _peerEvents = subscription;
    _peerEventsListener = subscription.stream.listen(
      (event) {
        if (event is! libp2p.EvtPeerConnectednessChanged) return;
        if (event.connectedness == libp2p.Connectedness.notConnected) {
          onPeerDisconnected(event.peer);
        } else if (event.connectedness == libp2p.Connectedness.connected) {
          _onPeerConnected(event.peer);
        }
      },
      onError: (Object e, StackTrace s) {
        _log.warning('Error on peer connectedness event stream; disconnection cleanup may be missed', e, s);
      },
    );
    _log.fine('Bitswap started for host: ${_host.id.toBase58()}.');
  }

  /// Stops Bitswap: unregisters the protocol handler, closes all streams and
  /// fails any pending [getBlock] calls with a [StateError]. Idempotent.
  Future<void> stop() async {
    if (_stopped) return;
    _stopped = true;
    await _peerEventsListener?.cancel();
    await _peerEvents?.close();
    await _network.stop();
    _client.stop(StateError('Bitswap stopped'));
    _log.fine('Bitswap stopped.');
  }

  /// Retrieves a block by its CID.
  ///
  /// Returns the block from the local block store if present; otherwise wants
  /// it from the network and waits up to [timeout] (default
  /// [BitswapOptions.getBlockTimeout]) for it to arrive. Returns null if the
  /// block did not arrive in time or its want was cancelled via [cancelWants].
  /// Concurrent calls for the same CID share one want, which is cancelled
  /// with peers once the last caller stops waiting without receiving it.
  Future<Uint8List?> getBlock(dart_cid.CID cid, {Duration? timeout}) async {
    _checkRunning();
    return _client.getBlock(cid, timeout: timeout);
  }

  /// Adds [cids] to the wantlist and broadcasts them. Blocks are stored in
  /// the [BlockStore] as they arrive; use [getBlock] to await one.
  Future<void> wantBlocks(List<dart_cid.CID> cids) async {
    _checkRunning();
    await _client.wantBlocks(cids);
  }

  /// Removes [cids] from the wantlist and sends CANCEL to the peers that were
  /// asked. Pending [getBlock] calls for them complete with null.
  void cancelWants(List<dart_cid.CID> cids) => _client.cancelWants(cids);

  /// Entry point for decoded messages from [BitswapNetworkService].
  @visibleForTesting
  Future<void> handleIncomingMessage(libp2p.PeerId peerId, pb.Message message) async {
    if (_stopped) return;
    if (message.hasWantlist()) {
      _client.peerActive(peerId);
      await _server.handleWantlist(peerId, message.wantlist);
    }
    for (final block in message.payload) {
      await _client.handleBlock(peerId, block);
    }
    for (final presence in message.blockPresences) {
      _client.handlePresence(peerId, presence);
    }
  }

  @visibleForTesting
  void onPeerDisconnected(libp2p.PeerId peerId) {
    _log.fine('Peer disconnected: $peerId. Cleaning up.');
    _client.peerDisconnected(peerId);
    _server.peerDisconnected(peerId);
    _network.handlePeerDisconnected(peerId);
  }

  void _onPeerConnected(libp2p.PeerId peerId) {
    _log.fine('Peer connected: $peerId');
    _client.peerConnected(peerId);
  }

  void _checkRunning() {
    if (_stopped) throw StateError('Bitswap stopped');
    if (!_started) throw StateError('Bitswap not started; call start() first');
  }
}
