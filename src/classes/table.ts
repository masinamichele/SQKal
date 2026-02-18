import { DiskManager } from './disk-manager.js';
import { Buffer } from 'node:buffer';
import { LAST_PAGE_ID } from '../const.js';
import { Page } from './page.js';
import { Entity } from './entity.js';
import { getEntityClass } from './decorators/registry.js';

export class Table<T extends Entity> {
  private readonly entityClass: { new (...data: any[]): T; deserialize: (buffer: Buffer) => T };

  constructor(
    private readonly diskManager: DiskManager,
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
      const pageBuffer = await this.diskManager.readPage(currentPageId);
      const page = new Page(pageBuffer, currentPageId);
      const rowId = page.insertRow(row);
      if (rowId !== null) {
        await this.diskManager.writePage(page.id, page.getBuffer());
        return;
      }

      const nextPageId = page.nextPageId;
      if (nextPageId === LAST_PAGE_ID) {
        const { pageId, buffer } = await this.diskManager.allocatePage();
        page.nextPageId = pageId;
        await this.diskManager.writePage(page.id, page.getBuffer());
        const newPage = Page.initialize(buffer, pageId);
        newPage.insertRow(row);
        await this.diskManager.writePage(pageId, newPage.getBuffer());
        return;
      }
      currentPageId = nextPageId;
    }
  }

  async *scan() {
    let id = this.firstPageId;
    while (id !== LAST_PAGE_ID) {
      const pageBuffer = await this.diskManager.readPage(id);
      const page = new Page(pageBuffer, id);
      for (let i = 0; i < page.rowCount; i++) {
        const buffer = page.getRow(i);
        yield this.entityClass.deserialize(buffer);
      }
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
}
