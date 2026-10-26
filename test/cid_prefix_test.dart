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
      // 0xd5 = md5, deliberately not accepted
      expect(() => cidFromPrefixAndData(Uint8List.fromList([0x01, 0x55, 0xd5, 0x01, 0x10]), data),
          throwsFormatException);
    });
  });

  group('multihash functions', () {
    final bytes = Uint8List.fromList(List.generate(200, (i) => i));
    Uint8List hex(String s) =>
        Uint8List.fromList([for (var i = 0; i < s.length; i += 2) int.parse(s.substring(i, i + 2), radix: 16)]);

    // Digests of bytes 0..199, from Python's hashlib (an independent implementation)
    final vectors = {
      (0x13, 'sha2-512'): '986058e9895e2c2ab8f9e8cbdf801db12a44842a56a91d5a4e87b1fc98b293722c4664142e42c3c551ff898646268cd92b84ed230b8c94bed7798d4f27cd7465',
      (0x17, 'sha3-224'): '8bcd90dbc5379549b5e78a1fbe24ae120d92caef17750461262b1e97',
      (0x16, 'sha3-256'): '5f728f63bf5ee48c77f453c0490398fa645b8d4c4e56be9a41cfec344d6ca899',
      (0x15, 'sha3-384'): 'b13febb1b3c54a7c6b69367f693a1d1f3145709b6ddef23ff15874133ea1fb9cfa48ee7ff4ec9aa987dea641e33ccdf7',
      (0x14, 'sha3-512'): 'ea5d05f19348dd589793354793a15f37a73b4c0bb4e750b9a00757dfce2f8b65a64191bb9b137de00feef6474cfd47abf7880efbc51614a5715df12cfe0caee3',
      (0xb220, 'blake2b-256'): '63c3d97a9f8894d5e043a707b0fee7f7ec4c049a23bbf1079df20b4165f9e22d',
      (0xb240, 'blake2b-512'): 'fb3c1f0f56a56f8e316fdf5d853c8c872c39635d083634c3904fc3ac07d1b578e85ff0e480e92d44ade33b62e893ee32343e79ddf6ef292e89b582d312502314',
    };

    Uint8List prefixFor(int mhCode, int digestLength) => Uint8List.fromList([
          ...dart_cid.encodeVarint(1),
          ...dart_cid.encodeVarint(0x55), // raw
          ...dart_cid.encodeVarint(mhCode),
          ...dart_cid.encodeVarint(digestLength),
        ]);

    Uint8List digestOf(dart_cid.CID cid) {
      final mh = cid.multihash;
      final code = dart_cid.decodeVarint(mh);
      final length = dart_cid.decodeVarint(mh, code.value);
      return mh.sublist(code.value + length.value);
    }

    for (final MapEntry(key: (code, name), value: expected) in vectors.entries) {
      test('$name verifies and round-trips its prefix', () {
        final prefix = prefixFor(code, expected.length ~/ 2);
        final cid = cidFromPrefixAndData(prefix, bytes);
        expect(digestOf(cid), hex(expected));
        expect(encodeCidPrefix(cid), prefix);
        expect(dart_cid.CID.fromBytes(cid.toBytes()).toBytes(), cid.toBytes());
      });
    }

    test('keccak-256 matches the published vectors', () {
      final prefix = prefixFor(0x1b, 32);
      expect(digestOf(cidFromPrefixAndData(prefix, Uint8List(0))),
          hex('c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470'));
      expect(digestOf(cidFromPrefixAndData(prefix, Uint8List.fromList('abc'.codeUnits))),
          hex('4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45'));
    });

    test('identity carries the block itself, up to $maxIdentityDigestLength bytes', () {
      final small = Uint8List.fromList([1, 2, 3, 4]);
      expect(digestOf(cidFromPrefixAndData(prefixFor(0x00, 4), small)), small);

      final big = Uint8List(maxIdentityDigestLength + 1);
      expect(() => cidFromPrefixAndData(prefixFor(0x00, big.length), big), throwsFormatException);
    });

    test('a digest length that does not match the function is rejected', () {
      expect(() => cidFromPrefixAndData(prefixFor(0xb220, 64), bytes), throwsFormatException);
      expect(() => cidFromPrefixAndData(prefixFor(0x00, 3), Uint8List.fromList([1, 2, 3, 4])), throwsFormatException);
    });

    test('supportedMultihashes lists them all', () {
      expect(supportedMultihashes, containsAll(['identity', 'sha2-256', 'keccak-512', ...vectors.keys.map((k) => k.$2)]));
    });
  });
}
