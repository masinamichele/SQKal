import { Buffer } from 'node:buffer';
import { LAST_PAGE_ID, PAGE_SLOT_SIZE } from '../../const.js';
import { Page } from '../storage/page.js';
import { BufferPoolManager } from '../storage/buffer-pool-manager.js';
import { FreeSpaceMap } from '../storage/free-space-map.js';
import { Injector } from '../injector.js';

export class Table {
  private readonly injector = Injector.getInstance();
  private readonly bpm = this.injector.resolve(BufferPoolManager);
  private readonly fsm = this.injector.resolve(FreeSpaceMap);

  constructor(private readonly firstPageId: number) {}

  async *scanWithLocation() {
    let id = this.firstPageId;
    while (id !== LAST_PAGE_ID) {
      const pageBuffer = await this.bpm.fetchPage(id);
      const page = new Page(pageBuffer, id);
      try {
        for (let i = 0; i < page.rowCount; i++) {
          const buffer = page.getRow(i);
          if (!buffer) continue;
          yield { buffer, pageId: id, rowIndex: i };
        }
      } finally {
        this.bpm.unpin(id, false);
      }
      id = page.nextPageId;
    }
  }

  async *scan() {
    for await (const { buffer } of this.scanWithLocation()) {
      yield buffer;
    }
  }

  async insert(row: Buffer) {
    const requiredSpace = row.length + PAGE_SLOT_SIZE;

    const pageIdFromFsm = await this.fsm.findPage(requiredSpace);

    if (pageIdFromFsm != null) {
      const pageBuffer = await this.bpm.fetchPage(pageIdFromFsm);
      if (pageBuffer) {
        const page = new Page(pageBuffer, pageIdFromFsm);
        if (page.insertRow(row) !== null) {
          await this.fsm.update(page.id, page.totalFreeSpace);
          this.bpm.unpin(pageIdFromFsm, true);
          return;
        }

        page.defragment();
        if (page.insertRow(row) !== null) {
          await this.fsm.update(page.id, page.totalFreeSpace);
          this.bpm.unpin(pageIdFromFsm, true);
          return;
        }

        this.bpm.unpin(pageIdFromFsm, false);
      }
    }

    let currentPageId = this.firstPageId;
    let lastPage: Page = null;
    while (currentPageId !== LAST_PAGE_ID) {
      const currentPageBuffer = await this.bpm.fetchPage(currentPageId);
      lastPage = new Page(currentPageBuffer, currentPageId);
      const nextPageId = lastPage.nextPageId;
      if (nextPageId === LAST_PAGE_ID) break;
      this.bpm.unpin(currentPageId, false);
      currentPageId = nextPageId;
    }

    const { pageId: newPageId, buffer: newPageBuffer } = await this.bpm.newPage();
    lastPage.nextPageId = newPageId;
    await this.fsm.update(lastPage.id, lastPage.totalFreeSpace);
    this.bpm.unpin(lastPage.id, true);

    const newPage = Page.initialize(newPageBuffer, newPageId);
    await this.fsm.update(newPage.id, newPage.totalFreeSpace);
    newPage.insertRow(row);
    await this.fsm.update(newPage.id, newPage.totalFreeSpace);
    this.bpm.unpin(newPage.id, true);
  }

  async delete(row: Buffer) {
    for await (const { buffer, rowIndex, pageId } of this.scanWithLocation()) {
      if (row.equals(buffer)) {
        const pageBuffer = await this.bpm.fetchPage(pageId);
        const page = new Page(pageBuffer, pageId);
        page.deleteRow(rowIndex);
        await this.fsm.update(page.id, page.totalFreeSpace);
        this.bpm.unpin(pageId, true);
        return true;
      }
    }
    return false;
  }

  async vacuum() {
    let id = this.firstPageId;
    while (id !== LAST_PAGE_ID) {
      const pageBuffer = await this.bpm.fetchPage(id);
      const page = new Page(pageBuffer, id);
      page.defragment();
      this.bpm.unpin(page.id, true);
      id = page.nextPageId;
    }
  }
}
