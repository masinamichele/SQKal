import { DiskManager } from './disk-manager.js';
import { Catalog, Schema } from './catalog.js';
import { BUFFER_POOL_SIZE, CATALOG, FSM } from '../const.js';
import { BufferPoolManager } from './buffer-pool-manager.js';
import { QueryParser } from './query/query-parser.js';
import { QueryRunner } from './query/query-runner.js';
import { FreeSpaceMap } from './free-space-map.js';

export class Database {
  private readonly diskManager: DiskManager;
  private readonly bpm: BufferPoolManager;
  private readonly catalog: Catalog;
  private readonly fsm: FreeSpaceMap;
  private readonly queryParser: QueryParser;
  private readonly queryRunner: QueryRunner;

  constructor(private readonly path: string) {
    this.diskManager = new DiskManager(this.path);
    this.bpm = new BufferPoolManager(this.diskManager, BUFFER_POOL_SIZE);
    this.fsm = new FreeSpaceMap(this.bpm);
    this.catalog = new Catalog(this.bpm, this.fsm);
    this.queryParser = new QueryParser();
    this.queryRunner = new QueryRunner(this, this.bpm);
  }

  async open() {
    await this.diskManager.open();

    const setupReservedPage = async (reservedId: number) => {
      const { pageId, buffer } = await this.bpm.newPage();
      if (pageId !== reservedId) {
        throw new Error(`Database initialization failed: first page was ${pageId} instead of ${CATALOG}`);
      }
      return buffer;
    };

    if ((await this.diskManager.size()) === 0) {
      const catalog = await setupReservedPage(CATALOG);
      await this.catalog.initialize(catalog);
      const fsm = await setupReservedPage(FSM);
      await this.fsm.initialize(fsm);
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

  async query(query: string) {
    const command = this.queryParser.parse(query);
    if (!command) {
      throw new Error(`Invalid query: '${query}'`);
    }
    return this.queryRunner.run(command);
  }
}
