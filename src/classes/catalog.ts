import { DiskManager } from './disk-manager.js';
import { Buffer } from 'node:buffer';
import { CATALOG, uint32 } from '../const.js';
import { Page } from './page.js';
import { Table } from './table.js';
import { Entity } from './entity.js';

export class Catalog {
  constructor(private readonly diskManager: DiskManager) {}

  async initialize(buffer: Buffer) {
    const page = Page.initialize(buffer, CATALOG);
    await this.diskManager.writePage(CATALOG, page.getBuffer());
  }

  async getTable<T extends Entity>(tableName: string) {
    const catalogBuffer = await this.diskManager.readPage(CATALOG);
    const catalogPage = new Page(catalogBuffer, CATALOG);
    for (let i = 0; i < catalogPage.rowCount; i++) {
      const row = catalogPage.getRow(i);
      const nameLength = row.readUint32BE(0);
      const name = row.toString('utf8', uint32, uint32 + nameLength);
      if (name === tableName) {
        const pageId = row.readUint32BE(uint32 + nameLength);
        return new Table<T>(this.diskManager, pageId, tableName);
      }
    }
    return null;
  }

  async createTable<T extends Entity>(tableName: string) {
    if ((await this.getTable(tableName)) !== null) {
      throw new Error(`Table '${tableName}' already exists`);
    }

    const { pageId, buffer } = await this.diskManager.allocatePage();
    const page = Page.initialize(buffer, pageId);
    await this.diskManager.writePage(pageId, page.getBuffer());

    const nameBuffer = Buffer.from(tableName, 'utf8');
    const nameLength = nameBuffer.length;
    const catalogRow = Buffer.alloc(uint32 + nameLength + uint32);
    catalogRow.writeUint32BE(nameLength, 0);
    nameBuffer.copy(catalogRow, uint32);
    catalogRow.writeUint32BE(pageId, uint32 + nameLength);

    const catalogBuffer = await this.diskManager.readPage(CATALOG);
    const catalogPage = new Page(catalogBuffer, CATALOG);
    catalogPage.insertRow(catalogRow);
    await this.diskManager.writePage(CATALOG, catalogPage.getBuffer());

    return new Table<T>(this.diskManager, pageId, tableName);
  }
}
