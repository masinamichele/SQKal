import { BufferPoolManager } from './buffer-pool-manager.js';
import { Buffer } from 'node:buffer';
import { Page } from './page.js';
import { FSM, LAST_PAGE_ID, sizeof_uint16 } from '../../const.js';
import { Injector } from '../injector.js';

export class FreeSpaceMap {
  private readonly injector = Injector.getInstance();
  private readonly bpm = this.injector.resolve(BufferPoolManager);

  async initialize(buffer: Buffer) {
    Page.initialize(buffer, FSM);
    await this.bpm.flushPage(FSM);
    this.bpm.unpin(FSM, false);
  }

  async get(pageId: number) {
    const fsmBuffer = await this.bpm.fetchPage(FSM);
    try {
      const offset = pageId * sizeof_uint16;
      if (offset + sizeof_uint16 > fsmBuffer.length) {
        return 0;
      }
      return fsmBuffer.readUint16BE(offset);
    } finally {
      this.bpm.unpin(FSM, false);
    }
  }

  async update(pageId: number, bytes: number) {
    const fsmBuffer = await this.bpm.fetchPage(FSM);
    try {
      const offset = pageId * sizeof_uint16;
      if (offset + sizeof_uint16 > fsmBuffer.length) {
        return 0;
      }
      return fsmBuffer.writeUint16BE(bytes, offset);
    } finally {
      this.bpm.unpin(FSM, true);
    }
  }

  async findPage(requiredBytes: number, startPageId?: number) {
    const fsmBuffer = await this.bpm.fetchPage(FSM);
    try {
      if (startPageId == null) {
        for (let offset = 0; offset < fsmBuffer.length; offset += sizeof_uint16) {
          if (fsmBuffer.readUint16BE(offset) >= requiredBytes) {
            return offset / sizeof_uint16;
          }
        }
      } else {
        let currentPageId = startPageId;
        while (currentPageId !== LAST_PAGE_ID) {
          const offset = currentPageId * sizeof_uint16;
          if (offset + sizeof_uint16 <= fsmBuffer.length && fsmBuffer.readUint16BE(offset) >= requiredBytes) {
            return currentPageId;
          }
          const pageBuffer = await this.bpm.fetchPage(currentPageId);
          const page = new Page(pageBuffer, currentPageId);
          this.bpm.unpin(currentPageId, false);
          currentPageId = page.nextPageId;
        }
      }
    } finally {
      this.bpm.unpin(FSM, false);
    }
  }
}
