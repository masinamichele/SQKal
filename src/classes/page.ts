import { Buffer } from 'node:buffer';
import { PAGE_HEADER_SIZE, LAST_PAGE_ID, PAGE_SIZE, sizeof_uint32, PAGE_SLOT_SIZE } from '../const.js';

export class Page {
  constructor(
    private readonly buffer: Buffer,
    readonly id: number,
  ) {}

  get rowCount() {
    return this.buffer.readUint32BE(0);
  }
  private set rowCount(value: number) {
    this.buffer.writeUint32BE(value, 0);
  }

  get freeSpacePointer() {
    return this.buffer.readUint32BE(sizeof_uint32);
  }
  private set freeSpacePointer(value: number) {
    this.buffer.writeUint32BE(value, sizeof_uint32);
  }

  get nextPageId() {
    return this.buffer.readUint32BE(2 * sizeof_uint32);
  }
  set nextPageId(value: number) {
    this.buffer.writeUint32BE(value, 2 * sizeof_uint32);
  }

  static initialize(buffer: Buffer, id: number) {
    const page = new this(buffer, id);
    page.rowCount = 0;
    page.freeSpacePointer = PAGE_SIZE;
    page.nextPageId = LAST_PAGE_ID;
    return page;
  }

  getBuffer() {
    return this.buffer;
  }

  private getSlot(index: number) {
    const slotOffset = PAGE_HEADER_SIZE + index * PAGE_SLOT_SIZE;
    const offset = this.buffer.readUint32BE(slotOffset);
    const length = this.buffer.readUint32BE(slotOffset + sizeof_uint32);
    return { offset, length };
  }

  private setSlot(index: number, offset: number, length: number) {
    const slotOffset = PAGE_HEADER_SIZE + index * PAGE_SLOT_SIZE;
    this.buffer.writeUint32BE(offset, slotOffset);
    this.buffer.writeUint32BE(length, slotOffset + sizeof_uint32);
  }

  insertRow(row: Buffer) {
    const size = row.length;

    const maxRowSize = PAGE_SIZE - PAGE_HEADER_SIZE - PAGE_SLOT_SIZE;
    if (size > maxRowSize) {
      throw new RangeError(`Row of size ${size} exceeds the maximum allowed size of ${maxRowSize}`);
    }

    const requiredSpace = size + PAGE_SLOT_SIZE;
    const endOfSlots = PAGE_HEADER_SIZE + this.rowCount * PAGE_SLOT_SIZE;

    if (requiredSpace > this.freeSpacePointer - endOfSlots) {
      return null;
    }

    const newRowOffset = this.freeSpacePointer - size;
    row.copy(this.buffer, newRowOffset);
    this.setSlot(this.rowCount, newRowOffset, size);
    this.freeSpacePointer = newRowOffset;
    this.rowCount += 1;
    return this.rowCount;
  }

  getRow(index: number) {
    if (index < 0 || index >= this.rowCount) {
      throw new RangeError(`Row index ${index} out of bounds`);
    }
    const { offset, length } = this.getSlot(index);
    if (!length) return null;

    return this.buffer.subarray(offset, offset + length);
  }

  deleteRow(index: number) {
    if (index < 0 || index >= this.rowCount) {
      throw new RangeError(`Row index ${index} out of bounds`);
    }
    for (let i = index; i < this.rowCount - 1; i++) {
      const nextSlot = this.getSlot(i + 1);
      this.setSlot(i, nextSlot.offset, nextSlot.length);
    }
    this.rowCount -= 1;
  }

  defragment() {
    const tempBuffer = Buffer.alloc(PAGE_SIZE);
    const tempPage = Page.initialize(tempBuffer, this.id);
    tempPage.nextPageId = this.nextPageId;

    for (let i = this.rowCount - 1; i >= 0; i--) {
      const row = this.getRow(i);
      if (row) tempPage.insertRow(row);
    }

    tempPage.getBuffer().copy(this.buffer, 0);
  }
}
