import 'dart:typed_data';

import 'package:dart_libp2p_bitswap/src/server/message_packing.dart';
import 'package:dart_libp2p_bitswap/src/proto/bitswap_message.pb.dart' as pb;
import 'package:test/test.dart';

void main() {
  pb.Message_Block block(int size, [int fill = 0]) => pb.Message_Block()
    ..prefix = [0x01, 0x55, 0x12, 0x20]
    ..data = Uint8List.fromList(List.filled(size, fill));
  pb.Message_BlockPresence presence(int i) => pb.Message_BlockPresence()
    ..cid = List.filled(36, i)
    ..type = pb.Message_BlockPresenceType.Have;

  group('packResponse', () {
    test('returns no messages when there is nothing to send', () {
      expect(packResponse([], []), isEmpty);
    });

    test('fits a small response in one message', () {
      final messages = packResponse([presence(1)], [block(100)]);
      expect(messages, hasLength(1));
      expect(messages.single.blockPresences, hasLength(1));
      expect(messages.single.payload, hasLength(1));
    });

    test('splits blocks so every encoded message stays within the limit', () {
      const limit = 1000;
      final blocks = [for (var i = 0; i < 10; i++) block(300, i)];

      final messages = packResponse([], blocks, maxSize: limit);

      expect(messages.length, greaterThan(1));
      for (final m in messages) {
        expect(m.writeToBuffer().length, lessThanOrEqualTo(limit));
      }
      // Nothing lost and order preserved
      expect(messages.expand((m) => m.payload).map((b) => b.data.first), [for (var i = 0; i < 10; i++) i]);
    });

    test('puts presences ahead of blocks', () {
      final messages = packResponse([presence(1), presence(2)], [block(800), block(800)], maxSize: 1000);
      expect(messages.first.blockPresences, hasLength(2));
      expect(messages.expand((m) => m.payload), hasLength(2));
    });

    test('drops a block that cannot fit in any message', () {
      final messages = packResponse([presence(1)], [block(2000), block(10)], maxSize: 1000);
      final payload = messages.expand((m) => m.payload).toList();
      expect(payload, hasLength(1));
      expect(payload.single.data.length, 10);
    });
  });
}
