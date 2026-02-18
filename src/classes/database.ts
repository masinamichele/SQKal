import { DiskManager } from './disk-manager.js';
import { Catalog } from './catalog.js';
import { CATALOG } from '../const.js';
import { Entity } from './entity.js';

export class Database {
  private readonly diskManager: DiskManager;
  private readonly catalog: Catalog;

  constructor(private readonly path: string) {
    this.diskManager = new DiskManager(path);
    this.catalog = new Catalog(this.diskManager);
  }

  async open() {
    await this.diskManager.open();
    if ((await this.diskManager.size()) === 0) {
      const { pageId, buffer } = await this.diskManager.allocatePage();
      if (pageId !== CATALOG) {
        throw new Error(`Database initialization failed: first page was ${pageId} instead of ${CATALOG}`);
      }
      await this.catalog.initialize(buffer);
    }
  }

  async close() {
    await this.diskManager.close();
  }

  async createTable<T extends Entity>(tableName: string) {
    return this.catalog.createTable<T>(tableName);
  }

  async getTable<T extends Entity>(tableName: string) {
    return this.catalog.getTable<T>(tableName);
  }
}
