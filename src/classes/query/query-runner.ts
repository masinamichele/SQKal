import { Database } from '../database.js';
import {
  Command,
  CreateTableCommand,
  DeleteCommand,
  InsertCommand,
  SelectCommand,
  UpdateCommand,
  WhereClause,
} from './query-types.js';
import { Serializer } from '../serializer.js';
import { BufferPoolManager } from '../buffer-pool-manager.js';
import { Page } from '../page.js';
import { Injector } from '../injector.js';
import { Buffer } from 'node:buffer';

export class QueryRunner {
  private readonly injector = Injector.getInstance();
  private readonly bpm = this.injector.resolve(BufferPoolManager);

  constructor(private readonly database: Database) {}

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
      case 'UPDATE':
        return this.handleUpdate(command);
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

  private matches(rowObject: Record<string, any>, { operator, field, value }: WhereClause) {
    const rowValue = rowObject[field];
    switch (operator) {
      case '=':
        return rowValue === value;
      case '<>':
        return rowValue !== value;
      case '<':
        return rowValue < value;
      case '>':
        return rowValue > value;
      case '<=':
        return rowValue <= value;
      case '>=':
        return rowValue >= value;
      case 'LIKE': {
        if (rowValue == null) return false;
        if (typeof value === 'number') return rowValue === value;
        const _value = value.toLowerCase();
        const _rowValue = rowValue.toLowerCase();
        const globBegin = _value.startsWith('%');
        const globEnd = _value.endsWith('%');
        if (globBegin && globEnd) return _rowValue.includes(_value.slice(1, -1));
        if (globBegin) return _rowValue.endsWith(_value.slice(1));
        if (globEnd) return _rowValue.startsWith(_value.slice(0, -1));
        return _rowValue === _value;
      }
      case 'IS':
        return rowValue == null;
      case 'IS NOT':
        return rowValue != null;
      default:
        throw new Error(`Unknown operator '${operator}' in WHERE clause`);
    }
  }

  private async handleInsert(command: InsertCommand) {
    const { table, schema } = await this.getCommandEntities(command);

    const buffersToInsert: Buffer[] = [];

    for (const values of command.values) {
      const data: Record<string, any> = {};
      schema.forEach((column, index) => {
        const value = values[index];
        if (!column.nullable && value == null) {
          throw new Error(`Constraint violation: column '${column.name}' cannot be null`);
        }
        data[column.name] = value;
      });
      const rowBuffer = Serializer.serialize(data, schema);
      buffersToInsert.push(rowBuffer);
    }

    for (const buffer of buffersToInsert) {
      await table.insert(buffer);
    }

    return [buffersToInsert.length];
  }

  private async handleSelect(command: SelectCommand) {
    const { table, schema } = await this.getCommandEntities(command);

    let results: Record<string, any>[] = [];

    for await (const rowBuffer of table.scan()) {
      const rowObject = Serializer.deserialize(rowBuffer, schema);

      if (command.where) {
        if (!this.matches(rowObject, command.where)) continue;
      }

      if (command.fields === '*') {
        results.push(rowObject);
      } else {
        results.push(this.pick(rowObject, command.fields));
      }
    }

    if (command.order) {
      const { field, direction } = command.order;
      results = results.toSorted((a, b) => {
        if (a[field].toLocaleCompare) {
          return a[field].toLocaleCompare(b[field]);
        }
        return a[field] - b[field];
      });
      if (direction === 'DESC') {
        results = results.toReversed();
      }
    }

    if (command.limit) {
      const { limit, offset } = command.limit;
      results = results.slice(offset ?? 0, offset + limit);
    }

    return results;
  }

  private async handleDelete(command: DeleteCommand) {
    const { table, schema } = await this.getCommandEntities(command);

    const locationToDelete: { pageId: number; rowIndex: number }[] = [];

    for await (const { buffer, pageId, rowIndex } of table.scanWithLocation()) {
      const rowObject = Serializer.deserialize(buffer, schema);
      if (this.matches(rowObject, command.where)) {
        locationToDelete.push({ pageId, rowIndex });
      }
    }

    const deletionByPage = locationToDelete.reduce<Record<number, number[]>>((acc, val) => {
      if (!acc[val.pageId]) acc[val.pageId] = [];
      acc[val.pageId].push(val.rowIndex);
      return acc;
    }, {});

    for (const [pageIdString, rowIndices] of Object.entries(deletionByPage)) {
      const pageId = Number(pageIdString);
      const pageBuffer = await this.bpm.fetchPage(pageId);
      if (!pageBuffer) continue;
      const page = new Page(pageBuffer, pageId);
      rowIndices.sort((a, b) => b - a);
      for (const rowIndex of rowIndices) {
        page.deleteRow(rowIndex);
      }
      this.bpm.unpin(pageId, true);
    }

    return [locationToDelete.length];
  }

  private async handleCreateTable(command: CreateTableCommand) {
    await this.database.createTable(command.tableName, command.schema);
    return [1];
  }

  private async handleUpdate(command: UpdateCommand) {
    const { table, schema } = await this.getCommandEntities(command);
    const rowsToUpdate: Record<string, any>[] = [];
    for await (const rowBuffer of table.scan()) {
      const rowObject = Serializer.deserialize(rowBuffer, schema);
      if (this.matches(rowObject, command.where)) {
        rowsToUpdate.push(rowObject);
      }
    }
    const updatableRows: [old: Buffer, new: Buffer][] = [];
    for (const oldRow of rowsToUpdate) {
      const oldRowBuffer = Serializer.serialize(oldRow, schema);
      const newRow = { ...oldRow, ...command.set };

      for (const column of schema) {
        const value = newRow[column.name];
        if (!column.nullable && value == null) {
          throw new Error(`Constraint violation: Column '${column.name}' cannot be null.`);
        }
      }

      const newRowBuffer = Serializer.serialize(newRow, schema);

      updatableRows.push([oldRowBuffer, newRowBuffer]);
    }

    for (const [old, row] of updatableRows) {
      await table.delete(old);
      await table.insert(row);
    }

    return [updatableRows.length];
  }
}
