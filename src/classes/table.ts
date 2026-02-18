import { Buffer } from 'node:buffer';
import { LAST_PAGE_ID } from '../const.js';
import { Page } from './page.js';
import { Entity } from './entity.js';
import { getEntityClass } from './decorators/registry.js';
import { BufferPoolManager } from './buffer-pool-manager.js';

export class Table<T extends Entity> {
  private readonly entityClass: { new (...data: any[]): T; deserialize: (buffer: Buffer) => T };

  constructor(
    private readonly bufferPoolManager: BufferPoolManager,
    private readonly firstPageId: number,
    private readonly name: string,
  ) {
    const entityClass = getEntityClass(this.name);
    if (!entityClass) {
      throw new Error(`No entity model has been registered for table '${this.name}'`);
    }
    this.entityClass = entityClass as any;
  }

  async insert(entity: T) {
    const row = entity.serialize();
    let currentPageId = this.firstPageId;
    while (true) {
      const pageBuffer = await this.bufferPoolManager.fetchPage(currentPageId);
      const page = new Page(pageBuffer, currentPageId);
      const rowId = page.insertRow(row);
      if (rowId !== null) {
        this.bufferPoolManager.unpin(page.id, true);
        return;
      }

      const nextPageId = page.nextPageId;
      if (nextPageId === LAST_PAGE_ID) {
        const { pageId, buffer } = await this.bufferPoolManager.newPage();
        page.nextPageId = pageId;
        this.bufferPoolManager.unpin(page.id, true);
        const newPage = Page.initialize(buffer, pageId);
        newPage.insertRow(row);
        this.bufferPoolManager.unpin(pageId, true);
        return;
      }
      currentPageId = nextPageId;
    }
  }

  private async *scanWithLocation() {
    let id = this.firstPageId;
    while (id !== LAST_PAGE_ID) {
      const pageBuffer = await this.bufferPoolManager.fetchPage(id);
      const page = new Page(pageBuffer, id);
      try {
        for (let i = 0; i < page.rowCount; i++) {
          const buffer = page.getRow(i);
          yield { buffer, pageId: id, rowIndex: i };
        }
      } finally {
        this.bufferPoolManager.unpin(id, false);
      }
      id = page.nextPageId;
    }
  }

  async *scan() {
    for await (const { buffer } of this.scanWithLocation()) {
      yield this.entityClass.deserialize(buffer);
    }
  }

  async vacuum() {
    let id = this.firstPageId;
    while (id !== LAST_PAGE_ID) {
      const pageBuffer = await this.bufferPoolManager.fetchPage(id);
      const page = new Page(pageBuffer, id);
      page.defragment();
      this.bufferPoolManager.unpin(page.id, true);
      id = page.nextPageId;
    }
  }

  async select() {
    const rows: T[] = [];
    for await (const row of this.scan()) {
      rows.push(row);
    }
    return rows;
  }

  async delete(entity: T) {
    const targetBuffer = entity.serialize();
    for await (const { buffer, rowIndex, pageId } of this.scanWithLocation()) {
      if (targetBuffer.equals(buffer)) {
        const pageBuffer = await this.bufferPoolManager.fetchPage(pageId);
        const page = new Page(pageBuffer, pageId);
        page.deleteRow(rowIndex);
        this.bufferPoolManager.unpin(pageId, true);
        return true;
      }
    }
    return false;
  }
}
