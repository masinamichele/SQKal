import { DiskManager } from './disk-manager.js';
import { Catalog } from './catalog.js';
import { BUFFER_POOL_SIZE, CATALOG } from '../const.js';
import { Entity } from './entity.js';
import { BufferPoolManager } from './buffer-pool-manager.js';

export class Database {
  private readonly diskManager: DiskManager;
  private readonly bufferPoolManager: BufferPoolManager;
  private readonly catalog: Catalog;

  constructor(private readonly path: string) {
    this.diskManager = new DiskManager(this.path);
    this.bufferPoolManager = new BufferPoolManager(this.diskManager, BUFFER_POOL_SIZE);
    this.catalog = new Catalog(this.bufferPoolManager);
  }

  async open() {
    await this.diskManager.open();
    if ((await this.diskManager.size()) === 0) {
      const { pageId, buffer } = await this.bufferPoolManager.newPage();
      if (pageId !== CATALOG) {
        throw new Error(`Database initialization failed: first page was ${pageId} instead of ${CATALOG}`);
      }
      await this.catalog.initialize(buffer);
    }
  }

  async close() {
    await this.bufferPoolManager.flush();
    await this.diskManager.close();
  }

  async createTable<T extends Entity>(tableName: string) {
    return this.catalog.createTable<T>(tableName);
  }

  async getTable<T extends Entity>(tableName: string) {
    return this.catalog.getTable<T>(tableName);
  }
}
