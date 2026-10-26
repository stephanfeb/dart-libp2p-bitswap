import 'dart:async';
import 'dart:typed_data';

import 'package:dart_libp2p/dart_libp2p.dart' as libp2p;
import 'package:mockito/mockito.dart';

/// Makes mock [stream] look like a live outgoing stream: open, with reads
/// waiting forever because the remote never writes to it. Unstubbed, a mock
/// read returns EOF, which Bitswap takes as the remote having closed it.
void stubLiveStream(libp2p.P2PStream stream) {
  when(stream.isClosed).thenReturn(false);
  when(stream.read(any)).thenAnswer((_) => Completer<Uint8List>().future);
}
