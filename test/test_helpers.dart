import 'dart:typed_data';

import 'package:dart_libp2p/dart_libp2p.dart' as libp2p;
import 'package:mockito/mockito.dart';

import 'bitswap_test.mocks.dart';

/// Makes [bus].subscribe() return a subscription that delivers [events],
/// which is what Bitswap.start() listens to for peer connectedness changes.
void stubPeerEvents(MockEventBus bus, Stream<dynamic> events) {
  final subscription = MockTestEventSubscription<dynamic>();
  when(subscription.stream).thenAnswer((_) => events);
  when(subscription.close()).thenAnswer((_) async {});
  when(bus.subscribe(any, opts: anyNamed('opts'))).thenReturn(subscription);
}

/// A real, value-comparable PeerId (identity-style multihash) for tests.
libp2p.PeerId testPeer(int n) =>
    libp2p.PeerId.fromBytes(Uint8List.fromList([0x12, 0x20, ...List.filled(32, n)]));
