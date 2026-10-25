import 'dart:typed_data';

import 'package:dart_libp2p_bitswap/src/cid_prefix.dart';
import 'package:dcid/dcid.dart' as dart_cid;
import 'package:test/test.dart';

void main() {
  final data = Uint8List.fromList('hello bitswap'.codeUnits);

  group('encodeCidPrefix / cidFromPrefixAndData', () {
    test('CIDv0 prefix uses the four-field layout and round-trips', () {
      // CIDv0 strings are base58 multihashes of the dag-pb bytes
      final v1 = dart_cid.CID.fromData(dart_cid.CID.V1, 'dag-pb', data);
      final v0 = dart_cid.CID(dart_cid.CID.V0, v1.codec, v1.multihash);

      final prefix = encodeCidPrefix(v0);
      expect(prefix, [0x00, 0x70, 0x12, 0x20]);
      expect(cidFromPrefixAndData(prefix, data), v0);
    });

    for (final codec in ['raw', 'dag-pb', 'dag-cbor']) {
      test('CIDv1 $codec round-trips', () {
        final cid = dart_cid.CID.fromData(dart_cid.CID.V1, codec, data);
        expect(cidFromPrefixAndData(encodeCidPrefix(cid), data), cid);
      });
    }

    test('accepts the prefix go-bitswap sends for CIDv0', () {
      final cid = cidFromPrefixAndData(Uint8List.fromList([0x00, 0x70, 0x12, 0x20]), data);
      expect(cid.version, dart_cid.CID.V0);
      expect(cid.toString(), startsWith('Qm'));
    });

    test('rejects CIDv0 prefix with non dag-pb codec', () {
      expect(() => cidFromPrefixAndData(Uint8List.fromList([0x00, 0x55, 0x12, 0x20]), data),
          throwsFormatException);
    });

    test('rejects truncated prefix', () {
      expect(() => cidFromPrefixAndData(Uint8List.fromList([0x01, 0x55, 0x12]), data),
          throwsFormatException);
    });

    test('rejects trailing bytes', () {
      expect(() => cidFromPrefixAndData(Uint8List.fromList([0x01, 0x55, 0x12, 0x20, 0x00]), data),
          throwsFormatException);
    });

    test('rejects digest length mismatch', () {
      expect(() => cidFromPrefixAndData(Uint8List.fromList([0x01, 0x55, 0x12, 0x10]), data),
          throwsFormatException);
    });

    test('rejects unknown CID version', () {
      expect(() => cidFromPrefixAndData(Uint8List.fromList([0x02, 0x55, 0x12, 0x20]), data),
          throwsFormatException);
    });

    test('rejects unsupported hash function', () {
      // 0x00 = identity multihash
      expect(() => cidFromPrefixAndData(Uint8List.fromList([0x01, 0x55, 0x00, 0x0d]), data),
          throwsFormatException);
    });
  });
}
