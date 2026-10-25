import 'dart:typed_data';

import 'package:crypto/crypto.dart' as crypto;
import 'package:dart_multihash/dart_multihash.dart' as dart_multihash;
import 'package:dcid/dcid.dart' as dart_cid;

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

  final mhName = dart_cid.codecCodeToName[mhCode];
  final Uint8List digest;
  switch (mhName) {
    case 'sha2-256':
      digest = Uint8List.fromList(crypto.sha256.convert(data).bytes);
    case 'sha2-512':
      digest = Uint8List.fromList(crypto.sha512.convert(data).bytes);
    default:
      throw FormatException('Unsupported multihash code $mhCode ($mhName)');
  }
  if (digest.length != digestLength) {
    throw FormatException(
        'Digest length $digestLength in prefix does not match $mhName (${digest.length})');
  }

  final multihash = dart_multihash.Multihash.encode(mhName!, digest).toBytes();
  return dart_cid.CID(version, codec, multihash);
}
