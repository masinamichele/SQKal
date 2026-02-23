import { DiskManager } from './storage/disk-manager.js';
import { Catalog, Schema } from './table/catalog.js';
import { BUFFER_POOL_SIZE, CATALOG, FSM, PAGE_DIRECTORY, PAGE_SIZE } from '../const.js';
import { BufferPoolManager } from './storage/buffer-pool-manager.js';
import { QueryParser } from './query/parser.js';
import { QueryRunner } from './query/runner.js';
import { FreeSpaceMap } from './storage/free-space-map.js';
import { Injector } from './injector.js';
import { Buffer } from 'node:buffer';

export class Database {
  private static instance: Database;
  static getInstance(path: string) {
    if (!this.instance) this.instance = new Database(path);
    return this.instance;
  }

  private readonly injector = Injector.getInstance();

  private readonly diskManager: DiskManager;
  private readonly bpm: BufferPoolManager;
  private readonly catalog: Catalog;
  private readonly fsm: FreeSpaceMap;
  private readonly queryParser: QueryParser;
  private readonly queryRunner: QueryRunner;

  private constructor(private readonly path: string) {
    this.diskManager = this.injector.register(DiskManager, [this.path]);
    this.bpm = this.injector.register(BufferPoolManager, [BUFFER_POOL_SIZE]);
    this.fsm = this.injector.register(FreeSpaceMap, []);
    this.catalog = this.injector.register(Catalog, []);
    this.queryParser = this.injector.register(QueryParser, []);
    this.queryRunner = this.injector.register(QueryRunner, [this]);
  }

  async open() {
    await this.diskManager.open();

    if ((await this.diskManager.size()) === 0) {
      const catalog = Buffer.alloc(PAGE_SIZE);
      await this.catalog.initialize(catalog);
      await this.diskManager.writePage(CATALOG, catalog);
      this.diskManager.pageDirectory.set(CATALOG, { offset: CATALOG * PAGE_SIZE, length: PAGE_SIZE });

      const fsm = Buffer.alloc(PAGE_SIZE);
      await this.fsm.initialize(fsm);
      await this.diskManager.writePage(FSM, fsm);
      this.diskManager.pageDirectory.set(CATALOG, { offset: CATALOG * PAGE_SIZE, length: PAGE_SIZE });
    }
  }

  async close() {
    await this.bpm.flush();
    await this.diskManager.close();
  }

  async createTable(tableName: string, schema: Schema) {
    return this.catalog.createTable(tableName, schema);
  }

  async getTable(tableName: string) {
    return this.catalog.getTable(tableName);
  }

  async getSchema(tableName: string) {
    return this.catalog.getSchema(tableName);
  }

  async exec([query]: TemplateStringsArray) {
    const command = this.queryParser.parse(query);
    if (!command) {
      throw new Error(`Invalid query: '${query}'`);
    }
    return this.queryRunner.run(command);
  }
}
