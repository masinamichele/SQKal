import { DiskManager } from './disk-manager.js';
import { Catalog } from './catalog.js';
import { BUFFER_POOL_SIZE, CATALOG } from '../const.js';
import { Entity } from './entity.js';
import { BufferPoolManager } from './buffer-pool-manager.js';
import { QueryParser } from './query/query-parser.js';
import { QueryRunner } from './query/query-runner.js';

export class Database {
  private readonly diskManager: DiskManager;
  private readonly bufferPoolManager: BufferPoolManager;
  private readonly catalog: Catalog;
  private readonly queryParser: QueryParser;
  private readonly queryRunner: QueryRunner;

  constructor(private readonly path: string) {
    this.diskManager = new DiskManager(this.path);
    this.bufferPoolManager = new BufferPoolManager(this.diskManager, BUFFER_POOL_SIZE);
    this.catalog = new Catalog(this.bufferPoolManager);
    this.queryParser = new QueryParser();
    this.queryRunner = new QueryRunner(this);
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

  async query(query: string) {
    const command = this.queryParser.parse(query);
    if (!command) {
      throw new Error(`Invalid query: '${query}'`);
    }
    return this.queryRunner.run(command);
  }
}
