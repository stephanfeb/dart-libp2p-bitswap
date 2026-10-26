import 'dart:async';
import 'dart:typed_data';

import 'package:dart_libp2p/dart_libp2p.dart' as libp2p;
import 'package:dcid/dcid.dart';
import 'package:logging/logging.dart';

import '../proto/bitswap_message.pb.dart' as pb;

final _log = Logger('bitswap.client.wantlist');

/// What we have sent to, and heard from, one peer about one want.
class PeerWantState {
  bool sentBlock = false; // We sent WANT_BLOCK (otherwise WANT_HAVE)
  bool? has; // null: no answer yet; true: HAVE; false: DONT_HAVE
}

/// A block our local node wants from the network.
class LocalWant {
  final CID cid;
  final int priority;
  pb.Message_Wantlist_WantType wantType; // Upgraded in place from Have to Block
  final bool sendDontHave;
  final Map<libp2p.PeerId, PeerWantState> peers =
      {}; // Peers this want was sent to
  final Completer<Uint8List> completer =
      Completer<Uint8List>(); // Completes when the block arrives
  int waiters = 0; // getBlock callers currently awaiting [completer]

  LocalWant({
    required this.cid,
    this.priority = 1,
    this.wantType = pb.Message_Wantlist_WantType.Block,
    this.sendDontHave = false,
  });

  /// Peers this want was sent to (and so must be CANCELled with).
  Set<libp2p.PeerId> get peersSentTo => peers.keys.toSet();

  /// The peer holding our outstanding WANT_BLOCK, if any. A peer that
  /// answered DONT_HAVE no longer counts.
  libp2p.PeerId? get blockPeer {
    for (final MapEntry(key: peer, value: state) in peers.entries) {
      if (state.sentBlock && state.has != false) return peer;
    }
    return null;
  }

  /// Peers that said HAVE but have not been sent WANT_BLOCK yet.
  Iterable<libp2p.PeerId> get haveCandidates => peers.entries
      .where((e) => e.value.has == true && !e.value.sentBlock)
      .map((e) => e.key);

  @override
  String toString() {
    return 'LocalWant(cid: $cid, priority: $priority, type: $wantType, sendDontHave: $sendDontHave, peers: ${peers.length})';
  }
}

/// The blocks our node wants, and who is waiting for them.
class LocalWantlist {
  // Key: CID as String
  final Map<String, LocalWant> _wants = {};

  /// Adds [cid] to the wantlist. Returns the want if it is new or was
  /// upgraded (and so needs broadcasting), or null if nothing changed.
  LocalWant? add(
    CID cid, {
    int priority = 1,
    pb.Message_Wantlist_WantType wantType = pb.Message_Wantlist_WantType.Block,
    bool sendDontHave = false,
  }) {
    final cidStr = cid.toString();
    final existing = _wants[cidStr];
    if (existing != null) {
      // Never replace an existing entry: callers may be awaiting its completer.
      // WANT_BLOCK subsumes WANT_HAVE, so the only meaningful change is an upgrade.
      if (existing.wantType == pb.Message_Wantlist_WantType.Have &&
          wantType == pb.Message_Wantlist_WantType.Block) {
        existing.wantType = wantType;
        _log.fine('Upgraded local want for $cidStr from Have to Block.');
        return existing;
      }
      return null;
    }
    final entry = LocalWant(
      cid: cid,
      priority: priority,
      wantType: wantType,
      sendDontHave: sendDontHave,
    );
    _wants[cidStr] = entry;
    _log.fine('Added local want: $entry');
    return entry;
  }

  /// Registers a waiter on an existing want and returns the future that
  /// completes with the block. Each call must be balanced by [releaseWaiter].
  Future<Uint8List> addWaiter(CID cid) {
    final entry = _wants[cid.toString()];
    if (entry == null) {
      return Future.error(
          StateError('Block $cid is not in the local wantlist.'));
    }
    entry.waiters++;
    return entry.completer.future;
  }

  /// Releases a waiter registered with [addWaiter]. If it was the last waiter
  /// and the block has not arrived, the want is removed and returned so the
  /// caller can CANCEL it with the peers in [LocalWant.peersSentTo].
  LocalWant? releaseWaiter(CID cid) {
    final entry = _wants[cid.toString()];
    if (entry == null) return null; // Already fulfilled or removed
    entry.waiters--;
    if (entry.waiters > 0 || entry.completer.isCompleted) return null;
    return remove(cid);
  }

  /// Removes the want for [cid], failing its waiters with [error].
  /// Returns the removed want, or null if there was none.
  LocalWant? cancel(CID cid, Object error) {
    final entry = remove(cid);
    if (entry != null) _fail(entry, error);
    return entry;
  }

  /// Fails every outstanding want with [error] and clears the wantlist.
  void failAll(Object error) {
    for (final entry in _wants.values) {
      _fail(entry, error);
    }
    _wants.clear();
  }

  void _fail(LocalWant entry, Object error) {
    if (entry.completer.isCompleted) return;
    entry.completer.future
        .ignore(); // Wants with no waiters must not surface as unhandled
    entry.completer.completeError(error);
  }

  /// Records that the want for [cid] was sent to [peerId], as WANT_BLOCK if
  /// [block] is true and WANT_HAVE otherwise.
  void recordSentTo(CID cid, libp2p.PeerId peerId, {bool block = false}) {
    final want = _wants[cid.toString()];
    if (want == null) return;
    final state = want.peers.putIfAbsent(peerId, PeerWantState.new);
    if (block) state.sentBlock = true;
  }

  /// Records a HAVE ([has] true) or DONT_HAVE from [peerId] and returns the
  /// want, or null if [cid] is not wanted.
  LocalWant? recordPresence(CID cid, libp2p.PeerId peerId,
      {required bool has}) {
    final want = _wants[cid.toString()];
    if (want == null) return null;
    want.peers.putIfAbsent(peerId, PeerWantState.new).has = has;
    return want;
  }

  /// Forgets [peerId] (it disconnected) and returns the wants for which it
  /// held our outstanding WANT_BLOCK.
  List<LocalWant> forgetPeer(libp2p.PeerId peerId) {
    final orphaned = <LocalWant>[];
    for (final want in _wants.values) {
      final wasBlockPeer = want.blockPeer == peerId;
      want.peers.remove(peerId);
      if (wasBlockPeer) orphaned.add(want);
    }
    return orphaned;
  }

  /// Peers the want for [cid] was sent to.
  Set<libp2p.PeerId> peersSentTo(CID cid) {
    return Set.unmodifiable(_wants[cid.toString()]?.peersSentTo ?? const {});
  }

  /// Removes [cid] without notifying waiters.
  LocalWant? remove(CID cid) => _wants.remove(cid.toString());

  bool contains(CID cid) => _wants.containsKey(cid.toString());

  LocalWant? operator [](CID cid) => _wants[cid.toString()];

  List<LocalWant> get all => _wants.values.toList();

  bool get isEmpty => _wants.isEmpty;

  /// Completes the want for [cid] with [data] and removes it. No-op if the
  /// block was not wanted.
  void blockReceived(CID cid, List<int> data) {
    final entry = _wants.remove(cid.toString());
    if (entry == null) return;
    if (!entry.completer.isCompleted) {
      entry.completer.complete(Uint8List.fromList(data));
    }
    _log.fine('Received wanted block $cid.');
  }
}
