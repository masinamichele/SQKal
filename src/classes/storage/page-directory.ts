import { Buffer } from 'node:buffer';
import { PAGE_SIZE, sizeof_uint32 } from '../../const.js';

export type PageLocation = {
  offset: number;
  length: number;
};

export class PageDirectory extends Map<number, PageLocation> {
  serialize() {
    const buffer = Buffer.alloc(PAGE_SIZE);
    let offset = 0;

    buffer.writeUint32BE(this.size, offset);
    offset += sizeof_uint32;

    for (const [pageId, location] of this.entries()) {
      if (offset + 3 * sizeof_uint32 > PAGE_SIZE) {
        throw new Error('PageDirectory overflow');
      }
      buffer.writeUint32BE(pageId, offset);
      offset += sizeof_uint32;
      buffer.writeUint32BE(location.offset, offset);
      offset += sizeof_uint32;
      buffer.writeUint32BE(location.length, offset);
      offset += sizeof_uint32;
    }

    return buffer;
  }

  deserialize(buffer: Buffer) {
    this.clear();
    let offset = 0;

    const entryCount = buffer.readUint32BE(offset);
    offset += sizeof_uint32;

    for (let i = 0; i < entryCount; i++) {
      const pageId = buffer.readUint32BE(offset);
      offset += sizeof_uint32;
      const pageOffset = buffer.readUint32BE(offset);
      offset += sizeof_uint32;
      const pageLength = buffer.readUint32BE(offset);
      offset += sizeof_uint32;

      this.set(pageId, { offset: pageOffset, length: pageLength });
    }
  }
}
