import { Buffer } from 'node:buffer';
import { DiskManager } from './disk-manager.js';
import { PAGE_SIZE } from '../../const.js';
import { DoublyLinkedList } from '../common/doubly-linked-list.js';
import { Injector } from '../injector.js';
import { Exception } from '../common/errors.js';

export class BufferPoolManager {
  private readonly pool: Buffer[] = [];
  private readonly pageToFrame = new Map<number, number>();
  private readonly frameToPage = new Map<number, number>();
  private readonly freeList: number[] = [];
  private readonly lruReplacer = new DoublyLinkedList<number>();
  private readonly pinCount = new Map<number, number>();
  private readonly isDirty = new Map<number, boolean>();

  private readonly injector = Injector.getInstance();
  private readonly diskManager = this.injector.resolve(DiskManager);

  constructor(private readonly poolSize: number) {
    for (let i = 0; i < this.poolSize; i++) {
      this.pool.push(Buffer.alloc(PAGE_SIZE));
      this.freeList.push(i);
    }
  }

  private async findVictimFrame() {
    if (this.freeList.length > 0) {
      return this.freeList.pop();
    }

    let victimFrameId: number = null;
    for (const frameId of this.lruReplacer.reverseValues()) {
      if (this.pinCount.get(frameId) === 0) {
        victimFrameId = frameId;
        break;
      }
    }

    if (victimFrameId == null) {
      throw new Exception('E402');
    }

    if (this.frameToPage.has(victimFrameId)) {
      const evictedPageId = this.frameToPage.get(victimFrameId);
      if (this.isDirty.get(victimFrameId)) {
        await this.diskManager.writePage(evictedPageId, this.pool[victimFrameId]);
      }

      this.pageToFrame.delete(evictedPageId);
      this.frameToPage.delete(victimFrameId);
      this.pinCount.delete(victimFrameId);
      this.isDirty.delete(victimFrameId);
      this.lruReplacer.remove(this.lruReplacer.getNode(victimFrameId));
    }

    return victimFrameId;
  }

  private setupFrameMetadata(pageId: number, frameId: number) {
    this.pinCount.set(frameId, 1);
    this.isDirty.set(frameId, false);
    this.pageToFrame.set(pageId, frameId);
    this.frameToPage.set(frameId, pageId);
    this.lruReplacer.insertAtHead(frameId);
  }

  async fetchPage(pageId: number) {
    if (this.pageToFrame.has(pageId)) {
      const frameId = this.pageToFrame.get(pageId);
      const node = this.lruReplacer.getNode(frameId);
      this.lruReplacer.moveToHead(node);
      this.pinCount.set(frameId, (this.pinCount.get(frameId) || 0) + 1);
      return this.pool[frameId];
    }

    const frameId = await this.findVictimFrame();

    await this.diskManager.readPage(pageId, this.pool[frameId]);
    this.setupFrameMetadata(pageId, frameId);

    return this.pool[frameId];
  }

  unpin(pageId: number, dirty: boolean) {
    if (!this.pageToFrame.has(pageId)) return;
    const frameId = this.pageToFrame.get(pageId);
    if (this.pinCount.get(frameId) > 0) {
      this.pinCount.set(frameId, this.pinCount.get(frameId) - 1);
      if (dirty) this.isDirty.set(frameId, true);
    }
  }

  async newPage() {
    const { pageId } = await this.diskManager.allocatePage();
    const frameId = await this.findVictimFrame();
    const pageBuffer = this.pool[frameId];
    pageBuffer.fill(0);
    this.setupFrameMetadata(pageId, frameId);
    return { pageId, buffer: pageBuffer };
  }

  async flushPage(pageId: number) {
    if (!this.pageToFrame.has(pageId)) return;
    const frameId = this.pageToFrame.get(pageId);
    await this.diskManager.writePage(pageId, this.pool[frameId]);
    this.isDirty.set(frameId, false);
  }

  async flush() {
    for (const [pageId, frameId] of this.pageToFrame.entries()) {
      if (this.isDirty.get(frameId)) {
        await this.diskManager.writePage(pageId, this.pool[frameId]);
        this.isDirty.set(frameId, false);
      }
    }
  }
}
