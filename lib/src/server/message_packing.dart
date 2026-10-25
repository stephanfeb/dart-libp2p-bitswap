import 'package:logging/logging.dart';

import '../network/network_service.dart' show maxBitswapMessageSize;
import '../proto/bitswap_message.pb.dart' as pb;

final _log = Logger('bitswap.packing');

// Upper bound on the bytes a repeated-field element adds beyond its own
// encoding: one tag byte plus a length varint (at most 5 bytes for int32).
const int _fieldOverhead = 6;

/// Packs [presences] and [blocks] into as few messages as possible, each
/// encoding to at most [maxSize] bytes. Presences come first so peers learn
/// HAVE/DONT_HAVE before large payloads arrive. A block that cannot fit in a
/// message on its own is dropped with a warning (peers would reject it).
List<pb.Message> packResponse(
  List<pb.Message_BlockPresence> presences,
  List<pb.Message_Block> blocks, {
  int maxSize = maxBitswapMessageSize,
}) {
  final messages = <pb.Message>[];
  var current = pb.Message();
  var currentSize = 0;

  void add(int size, void Function(pb.Message) append) {
    if (currentSize + size > maxSize && currentSize > 0) {
      messages.add(current);
      current = pb.Message();
      currentSize = 0;
    }
    append(current);
    currentSize += size;
  }

  for (final presence in presences) {
    add(presence.writeToBuffer().length + _fieldOverhead, (m) => m.blockPresences.add(presence));
  }
  for (final block in blocks) {
    final size = block.writeToBuffer().length + _fieldOverhead;
    if (size > maxSize) {
      _log.warning('Dropping block of ${block.data.length} bytes: exceeds message limit of $maxSize');
      continue;
    }
    add(size, (m) => m.payload.add(block));
  }

  if (currentSize > 0) messages.add(current);
  return messages;
}
