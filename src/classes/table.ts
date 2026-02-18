import { DiskManager } from './disk-manager.js';
import { Buffer } from 'node:buffer';
import { LAST_PAGE_ID } from '../const.js';
import { Page } from './page.js';

export class Table {
  constructor(
    private readonly diskManager: DiskManager,
    private readonly firstPageId: number,
  ) {}

  async insert(row: Buffer) {
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
        yield page.getRow(i);
      }
      id = page.nextPageId;
    }
  }

  async select() {
    const rows: Buffer[] = [];
    for await (const row of this.scan()) {
      rows.push(row);
    }
    return rows;
  }
}
