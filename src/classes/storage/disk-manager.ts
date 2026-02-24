import { FileHandle, open } from 'node:fs/promises';
import assert from 'node:assert/strict';
import { Buffer } from 'node:buffer';
import { CATALOG, FSM, PAGE_DIRECTORY, PAGE_SIZE } from '../../const.js';
import { PageDirectory, PageLocation } from './page-directory.js';
import { Archiver } from '../common/archiver.js';
import { Injector } from '../injector.js';
import { Exception } from '../common/errors.js';

const RESERVED_PAGES = new Set([CATALOG, FSM, PAGE_DIRECTORY]);

export class DiskManager {
  private handle: FileHandle;
  private readonly injector = Injector.getInstance();
  readonly pageDirectory = new PageDirectory();

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

    const directoryBuffer = await this.readPage(PAGE_DIRECTORY);
    this.pageDirectory.deserialize(directoryBuffer);
  }

  async close() {
    if (this.handle) await this.handle.close();
  }

  async writePage(id: number, data: Buffer) {
    assert(data.length === PAGE_SIZE, `Data buffer must be ${PAGE_SIZE} bytes`);

    const location: PageLocation = { offset: 0, length: 0 };
    if (RESERVED_PAGES.has(id)) {
      location.offset = id * PAGE_SIZE;
      location.length = PAGE_SIZE;
      await this.handle.write(data, 0, location.length, location.offset);
    } else {
      const compressedData = this.injector.resolve(Archiver).compress(data);
      const stats = await this.handle.stat();
      location.offset = stats.size;
      location.length = compressedData.length;
      await this.handle.write(compressedData, 0, location.length, location.offset);
    }

    this.pageDirectory.set(id, location);
    const directoryBuffer = this.pageDirectory.serialize();
    await this.handle.write(directoryBuffer, 0, PAGE_SIZE, PAGE_DIRECTORY * PAGE_SIZE);
  }

  async readPage(id: number, buffer: Buffer = Buffer.alloc(PAGE_SIZE)) {
    if (RESERVED_PAGES.has(id)) {
      const offset = id * PAGE_SIZE;
      const stats = await this.handle.stat();
      if (offset < stats.size) {
        await this.handle.read(buffer, 0, PAGE_SIZE, offset);
      }
    } else {
      const location = this.pageDirectory.get(id);
      if (!location) throw new Exception('E400', id);

      const compressedBuffer = Buffer.alloc(location.length);
      await this.handle.read(compressedBuffer, 0, location.length, location.offset);

      const decompressedData = this.injector.resolve(Archiver).decompress(compressedBuffer);
      decompressedData.copy(buffer);
    }

    return buffer;
  }

  async allocatePage() {
    const pageId = this.pageDirectory.size;
    const buffer = Buffer.alloc(PAGE_SIZE);
    return { pageId, buffer };
  }
}
