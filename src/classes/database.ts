import { DiskManager } from './storage/disk-manager.js';
import { Catalog, Schema } from './table/catalog.js';
import { BUFFER_POOL_SIZE, CATALOG, FSM, PAGE_DIRECTORY, PAGE_SIZE } from '../const.js';
import { BufferPoolManager } from './storage/buffer-pool-manager.js';
import { QueryParser } from './query/parser.js';
import { QueryRunner } from './query/runner.js';
import { FreeSpaceMap } from './storage/free-space-map.js';
import { Injector } from './injector.js';
import { Buffer } from 'node:buffer';
import { isAbsolute, join, dirname } from 'node:path';
import { rmSync } from 'node:fs';
import { Archiver } from './common/archiver.js';
import { Exception } from './common/errors.js';

type DatabaseOptions = Partial<{
  clean: boolean;
  compression: 'gzip' | 'brotli' | null;
}>;

let databaseCreated = false;

export class Database {
  private readonly injector = Injector.getInstance();

  private readonly diskManager: DiskManager;
  private readonly bpm: BufferPoolManager;
  private readonly catalog: Catalog;
  private readonly fsm: FreeSpaceMap;
  private readonly queryParser: QueryParser;
  private readonly queryRunner: QueryRunner;

  constructor(
    private readonly path: string,
    { clean = false, compression = 'brotli' }: DatabaseOptions,
  ) {
    if (databaseCreated) {
      throw new Exception('E410');
    }
    if (!isAbsolute(this.path)) {
      this.path = join(dirname(process.env.npm_package_json), 'db', this.path);
    }
    if (clean) {
      try {
        rmSync(this.path);
        console.log('[debug] Previous database deleted');
      } catch {}
    }
    this.injector.register(Archiver, [compression]);
    this.diskManager = this.injector.register(DiskManager, [this.path]);
    this.bpm = this.injector.register(BufferPoolManager, [BUFFER_POOL_SIZE]);
    this.fsm = this.injector.register(FreeSpaceMap, []);
    this.catalog = this.injector.register(Catalog, []);
    this.queryParser = this.injector.register(QueryParser, []);
    this.queryRunner = this.injector.register(QueryRunner, [this]);
    databaseCreated = true;
  }

  async open() {
    await this.diskManager.open();

    if ((await this.diskManager.size()) === 0) {
      const directory = Buffer.alloc(PAGE_SIZE);
      await this.diskManager.writePage(PAGE_DIRECTORY, directory);

      const catalog = Buffer.alloc(PAGE_SIZE);
      await this.catalog.initialize(catalog);
      await this.diskManager.writePage(CATALOG, catalog);

      const fsm = Buffer.alloc(PAGE_SIZE);
      await this.fsm.initialize(fsm);
      await this.diskManager.writePage(FSM, fsm);
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
      throw new Exception('E113', query);
    }
    console.log('>', query);
    return this.queryRunner.run(command);
  }
}
