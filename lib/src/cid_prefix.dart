import 'dart:typed_data';

import 'package:blake3_dart/blake3_dart.dart' as blake3;
import 'package:crypto/crypto.dart' as crypto;
import 'package:dart_multihash/dart_multihash.dart' as dart_multihash;
import 'package:dcid/dcid.dart' as dart_cid;
import 'package:pointycastle/digests/blake2b.dart';
import 'package:pointycastle/digests/keccak.dart';
import 'package:pointycastle/digests/sha3.dart';

/// Largest identity "digest" (the block itself) we accept, as in boxo's
/// verifcid: identity CIDs are meant for tiny inline blocks.
const maxIdentityDigestLength = 128;

/// Largest BLAKE3 digest accepted, as in go-multihash. BLAKE3 can produce
/// any output length, so its multihash digest length is variable.
const maxBlake3DigestLength = 128;

// [hash] gets the digest length the prefix declares; only variable-length
// functions (BLAKE3) use it, the rest are checked against it afterwards
typedef _Hasher = ({String name, Uint8List Function(Uint8List data, int digestLength) hash});

// Hash functions we can verify received blocks with, by multihash code
final Map<int, _Hasher> _hashers = {
  0x00: (name: 'identity', hash: (data, _) => data),
  0x12: (name: 'sha2-256', hash: (data, _) => Uint8List.fromList(crypto.sha256.convert(data).bytes)),
  0x13: (name: 'sha2-512', hash: (data, _) => Uint8List.fromList(crypto.sha512.convert(data).bytes)),
  0x14: (name: 'sha3-512', hash: (data, _) => SHA3Digest(512).process(data)),
  0x15: (name: 'sha3-384', hash: (data, _) => SHA3Digest(384).process(data)),
  0x16: (name: 'sha3-256', hash: (data, _) => SHA3Digest(256).process(data)),
  0x17: (name: 'sha3-224', hash: (data, _) => SHA3Digest(224).process(data)),
  0x1b: (name: 'keccak-256', hash: (data, _) => KeccakDigest(256).process(data)),
  0x1d: (name: 'keccak-512', hash: (data, _) => KeccakDigest(512).process(data)),
  0x1e: (name: 'blake3', hash: blake3.blake3),
  0xb220: (name: 'blake2b-256', hash: (data, _) => Blake2bDigest(digestSize: 32).process(data)),
  0xb240: (name: 'blake2b-512', hash: (data, _) => Blake2bDigest(digestSize: 64).process(data)),
};

/// Names of the multihash functions [cidFromPrefixAndData] can verify.
Iterable<String> get supportedMultihashes => _hashers.values.map((h) => h.name);

/// Encodes the Bitswap block prefix for [cid]: every CID component except the
/// digest, i.e. `<version><codec><multihash-code><digest-length>` as varints.
///
/// CIDv0 uses the same four-field layout (`0x00 0x70 0x12 0x20`), matching
/// go-bitswap / boxo.
Uint8List encodeCidPrefix(dart_cid.CID cid) {
  final decodedMh = dart_multihash.Multihash.decode(cid.multihash);
  return (BytesBuilder()
        ..add(dart_cid.encodeVarint(cid.version))
        ..add(dart_cid.encodeVarint(cid.codec))
        ..add(dart_cid.encodeVarint(decodedMh.code))
        ..add(dart_cid.encodeVarint(decodedMh.digest.length)))
      .toBytes();
}

/// Reconstructs the CID of a received block by hashing [data] according to
/// [prefix] (as produced by [encodeCidPrefix]).
///
/// Because the digest is computed locally, the returned CID is always correct
/// for [data]. Throws [FormatException] if the prefix is malformed,
/// inconsistent, or names an unsupported hash function.
dart_cid.CID cidFromPrefixAndData(Uint8List prefix, Uint8List data) {
  var offset = 0;
  int next() {
    if (offset >= prefix.length) {
      throw const FormatException('Truncated CID prefix');
    }
    final entry = dart_cid.decodeVarint(prefix, offset);
    offset += entry.value;
    return entry.key;
  }

  final version = next();
  final codec = next();
  final mhCode = next();
  final digestLength = next();
  if (offset != prefix.length) {
    throw FormatException('CID prefix has ${prefix.length - offset} trailing bytes');
  }

  if (version == dart_cid.CID.V0) {
    if (codec != dart_cid.codecNameToCode['dag-pb'] ||
        mhCode != dart_cid.codecNameToCode['sha2-256'] ||
        digestLength != 32) {
      throw FormatException(
          'Invalid CIDv0 prefix (codec: $codec, mhCode: $mhCode, digestLen: $digestLength)');
    }
  } else if (version != dart_cid.CID.V1) {
    throw FormatException('Unknown CID version $version');
  }

  final hasher = _hashers[mhCode];
  if (hasher == null) {
    throw FormatException('Unsupported multihash code 0x${mhCode.toRadixString(16)}');
  }
  if (hasher.name == 'identity' && data.length > maxIdentityDigestLength) {
    throw FormatException('Identity block of ${data.length} bytes exceeds $maxIdentityDigestLength');
  }
  if (hasher.name == 'blake3' && (digestLength < 1 || digestLength > maxBlake3DigestLength)) {
    throw FormatException('BLAKE3 digest length $digestLength outside 1..$maxBlake3DigestLength');
  }
  final digest = hasher.hash(data, digestLength);
  if (digest.length != digestLength) {
    throw FormatException(
        'Digest length $digestLength in prefix does not match ${hasher.name} (${digest.length})');
  }

  final multihash = dart_multihash.Multihash.encode(hasher.name, digest).toBytes();
  return dart_cid.CID(version, codec, multihash);
}
