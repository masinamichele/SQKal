import { BufferPoolManager } from './buffer-pool-manager.js';
import { Buffer } from 'node:buffer';
import { Page } from './page.js';
import { FSM, sizeof_uint16 } from '../const.js';

export class FreeSpaceMap {
  constructor(private readonly bpm: BufferPoolManager) {}

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

  async findPage(requiredBytes: number) {
    const fsmBuffer = await this.bpm.fetchPage(FSM);
    try {
      for (let offset = 0; offset < fsmBuffer.length; offset += sizeof_uint16) {
        if (fsmBuffer.readUint16BE(offset) >= requiredBytes) {
          return offset / sizeof_uint16;
        }
      }
    } finally {
      this.bpm.unpin(FSM, false);
    }
  }
}
