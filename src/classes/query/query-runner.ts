import { Database } from '../database.js';
import { Command, CreateTableCommand, DeleteCommand, InsertCommand, SelectCommand } from './query-types.js';
import { Serializer } from '../serializer.js';
import { BufferPoolManager } from '../buffer-pool-manager.js';
import { Page } from '../page.js';
import { DataType, Schema } from '../catalog.js';

export class QueryRunner {
  constructor(
    private readonly database: Database,
    private readonly bufferPoolManager: BufferPoolManager,
  ) {}

  async run(command: Command): Promise<any[]> {
    switch (command.type) {
      case 'INSERT':
        return this.handleInsert(command);
      case 'SELECT':
        return this.handleSelect(command);
      case 'DELETE':
        return this.handleDelete(command);
      case 'CREATE_TABLE':
        return this.handleCreateTable(command);
      default:
        throw new Error(`Unknown command '${(command as any)?.type}'`);
    }
  }

  private async getCommandEntities(command: Command) {
    const [table, schema] = await Promise.all([
      this.database.getTable(command.tableName),
      this.database.getSchema(command.tableName),
    ]);
    if (!table) {
      throw new Error(`Table '${command.tableName}' not found.`);
    }
    if (!schema) {
      throw new Error(`Schema for table '${command.tableName}' not found.`);
    }
    return { table, schema };
  }

  private pick(item: any, fields: string[]) {
    return fields.reduce<any>((obj, field) => {
      obj[field] = item[field];
      return obj;
    }, {});
  }

  private async handleInsert(command: InsertCommand) {
    const { table, schema } = await this.getCommandEntities(command);

    const data: Record<string, any> = {};
    schema.forEach((column, index) => {
      data[column.name] = command.values[index];
    });

    const rowBuffer = Serializer.serialize(data, schema);
    await table.insert(rowBuffer);

    return [1];
  }

  private async handleSelect(command: SelectCommand) {
    const { table, schema } = await this.getCommandEntities(command);

    const results: Record<string, any>[] = [];

    for await (const rowBuffer of table.scan()) {
      const rowObject = Serializer.deserialize(rowBuffer, schema);

      if (command.where) {
        const { field, operator, value } = command.where;
        let matches = false;
        if (operator === '=') {
          matches = rowObject[field] === value;
        }
        if (!matches) continue;
      }

      if (command.fields === '*') {
        results.push(rowObject);
      } else {
        results.push(this.pick(rowObject, command.fields));
      }
    }

    return results;
  }

  private async handleDelete(command: DeleteCommand) {
    const { table, schema } = await this.getCommandEntities(command);

    const { field, operator, value } = command.where;
    const locationToDelete: { pageId: number; rowIndex: number }[] = [];

    for await (const { buffer, pageId, rowIndex } of table.scanWithLocation()) {
      const rowObject = Serializer.deserialize(buffer, schema);
      let matches = false;
      if (operator === '=') {
        matches = rowObject[field] === value;
      }
      if (matches) {
        locationToDelete.push({ pageId, rowIndex });
      }
    }

    const deletionByPage = locationToDelete.reduce<Record<number, number[]>>((acc, val) => {
      if (!acc[val.pageId]) acc[val.pageId] = [];
      acc[val.pageId].push(val.rowIndex);
      return acc;
    }, {});

    const entries: [number, number[]][] = Object.entries(deletionByPage) as any;
    for (const [pageId, rowIndices] of entries) {
      const pageBuffer = await this.bufferPoolManager.fetchPage(pageId);
      if (!pageBuffer) continue;
      const page = new Page(pageBuffer, pageId);
      rowIndices.sort((a, b) => b - a);
      for (const rowIndex of rowIndices) {
        page.deleteRow(rowIndex);
      }
      this.bufferPoolManager.unpin(pageId, true);
    }

    return [locationToDelete.length];
  }

  private async handleCreateTable(command: CreateTableCommand) {
    await this.database.createTable(command.tableName, command.schema);
    return [1];
  }
}
