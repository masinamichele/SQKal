import { Buffer } from 'node:buffer';
import { DiskManager } from './disk-manager.js';
import { PAGE_SIZE } from '../const.js';
import { DoublyLinkedList } from './doubly-linked-list.js';

export class BufferPoolManager {
  private readonly pool: Buffer[] = [];
  private readonly pageToFrame = new Map<number, number>();
  private readonly frameToPage = new Map<number, number>();
  private readonly freeList: number[] = [];
  private readonly lruReplacer = new DoublyLinkedList<number>();
  private readonly pinCount = new Map<number, number>();
  private readonly isDirty = new Map<number, boolean>();

  constructor(
    private readonly diskManager: DiskManager,
    private readonly poolSize: number,
  ) {
    for (let i = 0; i < this.poolSize; i++) {
      this.pool.push(Buffer.alloc(PAGE_SIZE));
      this.freeList.push(i);
    }
  }

  async fetchPage(pageId: number) {
    if (this.pageToFrame.has(pageId)) {
      const frameId = this.pageToFrame.get(pageId);
      const node = this.lruReplacer.getNode(frameId);
      this.lruReplacer.moveToHead(node);
      this.pinCount.set(frameId, (this.pinCount.get(frameId) || 0) + 1);
      return this.pool[frameId];
    }

    let frameId: number;
    if (this.freeList.length > 0) {
      frameId = this.freeList.pop();
    } else {
      for (const fid of this.lruReplacer.reverseValues()) {
        if (this.pinCount.get(fid) === 0) {
          frameId = fid;
          break;
        }
      }

      if (frameId == null) return null;

      if (this.frameToPage.has(frameId)) {
        const evictedPageId = this.frameToPage.get(frameId);
        if (this.isDirty.get(frameId)) {
          await this.diskManager.writePage(evictedPageId, this.pool[frameId]);
        }

        this.pageToFrame.delete(evictedPageId);
        this.frameToPage.delete(frameId);
        this.pinCount.delete(frameId);
        this.isDirty.delete(frameId);
        this.lruReplacer.remove(this.lruReplacer.getNode(frameId));
      }
    }

    await this.diskManager.readPage(pageId, this.pool[frameId]);
    this.pinCount.set(frameId, 1);
    this.isDirty.set(frameId, false);
    this.pageToFrame.set(pageId, frameId);
    this.frameToPage.set(frameId, pageId);
    this.lruReplacer.insertAtHead(frameId);
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
    const pageBuffer = await this.fetchPage(pageId);
    if (pageBuffer == null) return null;
    return { pageId, buffer: pageBuffer };
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
