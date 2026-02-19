import { Buffer } from 'node:buffer';
import { CATALOG, sizeof_uint32, sizeof_uint8 } from '../const.js';
import { Page } from './page.js';
import { Table } from './table.js';
import { BufferPoolManager } from './buffer-pool-manager.js';

export enum DataType {
  NUMBER = 0x01,
  STRING = 0x02,
}

export type Column = {
  name: string;
  type: DataType;
};

export type Schema = Column[];

export class Catalog {
  constructor(private readonly bufferPoolManager: BufferPoolManager) {}

  async initialize(buffer: Buffer) {
    Page.initialize(buffer, CATALOG);
    await this.bufferPoolManager.flushPage(CATALOG);
    this.bufferPoolManager.unpin(CATALOG, false);
  }

  private async findTableMetadata(tableName: string) {
    const catalogBuffer = await this.bufferPoolManager.fetchPage(CATALOG);
    try {
      const catalogPage = new Page(catalogBuffer, CATALOG);
      for (let i = 0; i < catalogPage.rowCount; i++) {
        const row = catalogPage.getRow(i);
        if (!row) continue;
        let offset = 0;
        const nameLength = row.readUint32BE(offset);
        offset += sizeof_uint32;
        const name = row.toString('utf8', offset, offset + nameLength);
        if (name === tableName) {
          return row;
        }
      }
    } finally {
      this.bufferPoolManager.unpin(CATALOG, false);
    }
  }

  async getSchema(tableName: string) {
    const row = await this.findTableMetadata(tableName);
    if (!row) return null;
    const nameLength = row.readUint32BE(0);
    let offset = sizeof_uint32 + nameLength + sizeof_uint32;
    const columnCount = row.readUInt8(offset);
    offset += sizeof_uint8;
    const schema: Schema = [];
    for (let i = 0; i < columnCount; i++) {
      const columnNameLength = row.readUInt8(offset);
      offset += sizeof_uint8;
      const columnName = row.toString('utf8', offset, offset + columnNameLength);
      offset += columnNameLength;
      const columnType = row.readUInt8(offset);
      offset += sizeof_uint8;
      schema.push({ name: columnName, type: columnType });
    }
    return schema;
  }

  async getTable(tableName: string) {
    const row = await this.findTableMetadata(tableName);
    if (!row) return null;
    const nameLength = row.readUint32BE(0);
    const pageId = row.readUint32BE(sizeof_uint32 + nameLength);
    return new Table(this.bufferPoolManager, pageId);
  }

  async createTable(tableName: string, schema: Schema) {
    if ((await this.getTable(tableName)) !== null) {
      throw new Error(`Table '${tableName}' already exists`);
    }

    const { pageId, buffer } = await this.bufferPoolManager.newPage();
    Page.initialize(buffer, pageId);

    await this.bufferPoolManager.flushPage(pageId);
    this.bufferPoolManager.unpin(pageId, false);

    const columnBuffers = schema.map((column) => {
      const nameBuffer = Buffer.from(column.name, 'utf8');
      const typeBuf = Buffer.alloc(sizeof_uint8);
      typeBuf.writeUInt8(column.type);
      const nameLengthBuffer = Buffer.alloc(sizeof_uint8);
      nameLengthBuffer.writeUInt8(nameBuffer.length);
      return Buffer.concat([nameLengthBuffer, nameBuffer, typeBuf]);
    });
    const columnsBuffer = Buffer.concat(columnBuffers);

    const columnCountBuffer = Buffer.alloc(sizeof_uint8);
    columnCountBuffer.writeUInt8(schema.length);

    const firstPageIdBuffer = Buffer.alloc(sizeof_uint32);
    firstPageIdBuffer.writeUint32BE(pageId);

    const nameBuffer = Buffer.from(tableName, 'utf8');
    const nameLengthBuffer = Buffer.alloc(sizeof_uint32);
    nameLengthBuffer.writeUint32BE(nameBuffer.length);

    const catalogRow = Buffer.concat([
      nameLengthBuffer,
      nameBuffer,
      firstPageIdBuffer,
      columnCountBuffer,
      columnsBuffer,
    ]);

    const catalogBuffer = await this.bufferPoolManager.fetchPage(CATALOG);
    const catalogPage = new Page(catalogBuffer, CATALOG);
    catalogPage.insertRow(catalogRow);
    this.bufferPoolManager.unpin(CATALOG, true);

    return new Table(this.bufferPoolManager, pageId);
  }
}
