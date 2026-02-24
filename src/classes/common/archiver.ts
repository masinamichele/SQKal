import { Buffer } from 'node:buffer';
import { brotliCompressSync, brotliDecompressSync, gzipSync, gunzipSync } from 'node:zlib';

export class Archiver {
  constructor(private readonly algo: 'gzip' | 'brotli' | null) {}

  compress(buffer: Buffer) {
    if (!this.algo) return buffer;
    switch (this.algo) {
      case 'gzip':
        return gzipSync(buffer);
      case 'brotli':
        return brotliCompressSync(buffer);
    }
  }

  decompress(buffer: Buffer) {
    if (!this.algo) return buffer;
    switch (this.algo) {
      case 'gzip':
        return gunzipSync(buffer);
      case 'brotli':
        return brotliDecompressSync(buffer);
    }
  }
}
