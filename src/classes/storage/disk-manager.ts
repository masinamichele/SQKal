import { FileHandle, open } from 'node:fs/promises';
import assert from 'node:assert/strict';
import { Buffer } from 'node:buffer';
import { PAGE_SIZE } from '../../const.js';

export class DiskManager {
  private handle: FileHandle;

  constructor(private readonly path: string) {}

  async size() {
    const stats = await this.handle.stat();
    return stats.size;
  }

  async open() {
    try {
      this.handle = await open(this.path, 'r+');
    } catch (error: any) {
      if (error.code === 'ENOENT') {
        this.handle = await open(this.path, 'w+');
      } else {
        throw error;
      }
    }
  }

  async close() {
    if (this.handle) await this.handle.close();
  }

  async writePage(id: number, data: Buffer) {
    assert(data.length === PAGE_SIZE, `Data buffer must be ${PAGE_SIZE} bytes`);
    const offset = id * PAGE_SIZE;
    await this.handle.write(data, 0, PAGE_SIZE, offset);
  }

  async readPage(id: number, buffer: Buffer = Buffer.alloc(PAGE_SIZE)) {
    const offset = id * PAGE_SIZE;
    await this.handle.read(buffer, 0, PAGE_SIZE, offset);
    return buffer;
  }

  async allocatePage() {
    const stats = await this.handle.stat();
    const pageId = stats.size / PAGE_SIZE;
    const buffer = Buffer.alloc(PAGE_SIZE);
    return { pageId, buffer };
  }
}
