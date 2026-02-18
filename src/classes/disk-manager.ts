import { FileHandle, open } from 'node:fs/promises';
import assert from 'node:assert/strict';
import { Buffer } from 'node:buffer';
import { PAGE_SIZE } from '../const.js';

export class DiskManager {
  private handle: FileHandle;

  private logHandle: FileHandle;
  private readonly logPath: string;

  constructor(private readonly path: string) {
    this.logPath = `${this.path}.log`;
  }

  async log(event: string) {
    await this.logHandle.write(`${event}\n`);
  }

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
    this.logHandle = await open(this.logPath, 'a');
  }

  async close() {
    if (this.handle) await this.handle.close();
    if (this.logHandle) await this.logHandle.close();
  }

  async writePage(id: number, data: Buffer) {
    assert(data.length === PAGE_SIZE, `Data buffer must be ${PAGE_SIZE} bytes`);
    const offset = id * PAGE_SIZE;
    await this.handle.write(data, 0, PAGE_SIZE, offset);
    await this.log(`[W] Page ${id}`);
  }

  async readPage(id: number, buffer: Buffer = Buffer.alloc(PAGE_SIZE)) {
    const offset = id * PAGE_SIZE;
    await this.handle.read(buffer, 0, PAGE_SIZE, offset);
    await this.log(`[R] Page ${id}`);
    return buffer;
  }

  async allocatePage() {
    const stats = await this.handle.stat();
    const pageId = stats.size / PAGE_SIZE;
    await this.log(`[A] Page ${pageId}`);
    const buffer = Buffer.alloc(PAGE_SIZE);
    return { pageId, buffer };
  }
}
