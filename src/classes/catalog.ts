import { Buffer } from 'node:buffer';
import { CATALOG, sizeof_uint32 } from '../const.js';
import { Page } from './page.js';
import { Table } from './table.js';
import { Entity } from './entity.js';
import { BufferPoolManager } from './buffer-pool-manager.js';

export class Catalog {
  constructor(private readonly bufferPoolManager: BufferPoolManager) {}

  async initialize(buffer: Buffer) {
    Page.initialize(buffer, CATALOG);
    await this.bufferPoolManager.flushPage(CATALOG);
    this.bufferPoolManager.unpin(CATALOG, false);
  }

  async getTable<T extends Entity>(tableName: string) {
    const catalogBuffer = await this.bufferPoolManager.fetchPage(CATALOG);
    const catalogPage = new Page(catalogBuffer, CATALOG);
    for (let i = 0; i < catalogPage.rowCount; i++) {
      const row = catalogPage.getRow(i);
      const nameLength = row.readUint32BE(0);
      const name = row.toString('utf8', sizeof_uint32, sizeof_uint32 + nameLength);
      if (name === tableName) {
        this.bufferPoolManager.unpin(CATALOG, false);
        const pageId = row.readUint32BE(sizeof_uint32 + nameLength);
        return new Table<T>(this.bufferPoolManager, pageId, tableName);
      }
    }
    this.bufferPoolManager.unpin(CATALOG, false);
    return null;
  }

  async createTable<T extends Entity>(tableName: string) {
    if ((await this.getTable(tableName)) !== null) {
      throw new Error(`Table '${tableName}' already exists`);
    }

    const { pageId, buffer } = await this.bufferPoolManager.newPage();
    Page.initialize(buffer, pageId);

    await this.bufferPoolManager.flushPage(pageId);
    this.bufferPoolManager.unpin(pageId, false);

    const nameBuffer = Buffer.from(tableName, 'utf8');
    const nameLength = nameBuffer.length;
    const catalogRow = Buffer.alloc(sizeof_uint32 + nameLength + sizeof_uint32);
    catalogRow.writeUint32BE(nameLength, 0);
    nameBuffer.copy(catalogRow, sizeof_uint32);
    catalogRow.writeUint32BE(pageId, sizeof_uint32 + nameLength);

    const catalogBuffer = await this.bufferPoolManager.fetchPage(CATALOG);
    const catalogPage = new Page(catalogBuffer, CATALOG);
    catalogPage.insertRow(catalogRow);
    this.bufferPoolManager.unpin(CATALOG, true);

    return new Table<T>(this.bufferPoolManager, pageId, tableName);
  }
}
