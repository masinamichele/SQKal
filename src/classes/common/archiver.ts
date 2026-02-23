import { Buffer } from 'node:buffer';
import { brotliCompressSync as zip, brotliDecompressSync as unzip } from 'node:zlib';

export class Archiver {
  static compress(buffer: Buffer) {
    return zip(buffer);
  }

  static decompress(buffer: Buffer) {
    return unzip(buffer);
  }
}
