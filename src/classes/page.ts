import { Buffer } from 'node:buffer';
import { HEADER_SIZE, LAST_PAGE_ID, PAGE_SIZE, ROW_HEADER_SIZE, uint32 } from '../const.js';

export class Page {
  constructor(private readonly buffer: Buffer, readonly id: number) {}

  get rowCount() {
    return this.buffer.readUint32BE(0);
  }
  private set rowCount(value: number) {
    this.buffer.writeUint32BE(value, 0);
  }

  get freeSpaceOffset() {
    return this.buffer.readUint32BE(uint32);
  }
  private set freeSpaceOffset(value: number) {
    this.buffer.writeUint32BE(value, uint32);
  }

  get nextPageId() {
    return this.buffer.readUint32BE(2 * uint32);
  }
  set nextPageId(value: number) {
    this.buffer.writeUint32BE(value, 2 * uint32);
  }

  static initialize(buffer: Buffer, id: number) {
    const page = new this(buffer, id);
    page.rowCount = 0;
    page.freeSpaceOffset = HEADER_SIZE;
    page.nextPageId = LAST_PAGE_ID;
    return page;
  }

  insertRow(row: Buffer) {
    const size = row.length;

    const maxRowSize = PAGE_SIZE - HEADER_SIZE - ROW_HEADER_SIZE;
    if (size > maxRowSize) {
      throw new RangeError(`Row of size ${size} exceeds the maximum allowed size of ${maxRowSize}`);
    }

    const totalSize = size + ROW_HEADER_SIZE;
    if (totalSize > PAGE_SIZE - this.freeSpaceOffset) {
      return null;
    }

    this.buffer.writeUint32BE(size, this.freeSpaceOffset);
    row.copy(this.buffer, this.freeSpaceOffset + ROW_HEADER_SIZE);
    this.freeSpaceOffset += totalSize;
    this.rowCount += 1;
    return this.rowCount;
  }

  getBuffer() {
    return this.buffer;
  }

  getRow(index: number) {
    if (index < 0 || index > this.rowCount) {
      throw new RangeError(`Row index ${index} out of bounds`);
    }
    let offset = HEADER_SIZE;
    for (let i = 0; i < index; i++) {
      const rowSize = this.buffer.readUint32BE(offset);
      offset += rowSize + uint32;
    }
    const targetRowSize = this.buffer.readUint32BE(offset);
    const rowStart = offset + uint32;
    return this.buffer.subarray(rowStart, rowStart + targetRowSize);
  }
}
