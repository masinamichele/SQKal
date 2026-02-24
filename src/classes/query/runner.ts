import { Database } from '../database.js';
import {
  Command,
  ConditionNode,
  CreateTableCommand,
  DeleteCommand,
  InsertCommand,
  LogicalNode,
  SelectCommand,
  UpdateCommand,
  WhereClause,
} from './types.js';
import { Serializer } from '../common/serializer.js';
import { BufferPoolManager } from '../storage/buffer-pool-manager.js';
import { Page } from '../storage/page.js';
import { Injector } from '../injector.js';
import { Buffer } from 'node:buffer';
import { Table } from '../table/table.js';
import { Schema } from '../table/catalog.js';
import { Exception } from '../common/errors.js';

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
        throw new Exception('E203', (command as any)?.type);
    }
  }

  private async getCommandEntities(command: Command) {
    const [table, schema] = await Promise.all([
      this.database.getTable(command.tableName),
      this.database.getSchema(command.tableName),
    ]);
    if (!table) {
      throw new Exception('E200', command.tableName);
    }
    if (!schema) {
      throw new Exception('E202', command.tableName);
    }
    return { table, schema };
  }

  private pick(item: any, fields: string[]) {
    return fields.reduce<any>((obj, field) => {
      obj[field] = item[field];
      return obj;
    }, {});
  }

  private _evaluateConditionNode(rowObject: Record<string, any>, { field, operator, value }: ConditionNode) {
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
        throw new Exception('E111', `'${operator}' in WHERE clause`);
    }
  }

  private _evaluateLogicalNode(rowObject: Record<string, any>, { operator, left, right }: LogicalNode) {
    const leftResult = this.matches(rowObject, left);
    if (operator === 'OR' && leftResult) return true;
    if (operator === 'AND' && !leftResult) return false;
    const rightResult = this.matches(rowObject, right);
    if (operator === 'OR') return leftResult || rightResult;
    if (operator === 'AND') return leftResult && rightResult;
  }

  private async _checkUniqueness(table: Table, schema: Schema, columnName: string, value: any) {
    for await (const { buffer } of table.scan()) {
      const rowObject = Serializer.deserialize(buffer, schema);
      if (rowObject[columnName] === value) {
        throw new Exception('E301', `${columnName}: ${value}`);
      }
    }
  }

  private matches(rowObject: Record<string, any>, where: WhereClause): boolean {
    if (where.type === 'CONDITION') {
      return this._evaluateConditionNode(rowObject, where);
    }
    if (where.type === 'LOGICAL') {
      return this._evaluateLogicalNode(rowObject, where);
    }
    throw new Exception('E112');
  }

  private async handleInsert(command: InsertCommand) {
    const { table, schema } = await this.getCommandEntities(command);

    const autoIncrementColumn = schema.find((col) => col.autoIncrement);
    let nextAutoIncrementId = 1;
    if (autoIncrementColumn) {
      let maxId = 0;
      for await (const { buffer } of table.scan()) {
        const rowObject = Serializer.deserialize(buffer, schema);
        const id = rowObject[autoIncrementColumn.name];
        if (id > maxId) maxId = id;
      }
      nextAutoIncrementId = maxId + 1;
    }

    const rowsToInsert: Record<string, any>[] = [];
    for (const values of command.values) {
      const data: Record<string, any> = {};
      for (const [index, column] of schema.entries()) {
        let value = values[index];
        if (column.autoIncrement && value == null) {
          value = nextAutoIncrementId++;
        }
        data[column.name] = value;
      }
      rowsToInsert.push(data);
    }

    for (const data of rowsToInsert) {
      for (const column of schema) {
        const value = data[column.name];

        if (!column.nullable && value == null) {
          throw new Exception('E300', column.name);
        }
        if (value != null && column.unique) {
          await this._checkUniqueness(table, schema, column.name, value);
          const duplicatesInBatch = rowsToInsert.filter((r) => r[column.name] === value);
          if (duplicatesInBatch.length > 1) {
            throw new Exception('E202', `${column.name}: ${value}`);
          }
        }
      }
    }

    for (const row of rowsToInsert) {
      const buffer = Serializer.serialize(row, schema);
      await table.insert(buffer);
    }

    return [rowsToInsert.length];
  }

  private async handleSelect(command: SelectCommand) {
    const { table, schema } = await this.getCommandEntities(command);

    let results: Record<string, any>[] = [];

    for await (const { buffer } of table.scan()) {
      const rowObject = Serializer.deserialize(buffer, schema);

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

    for await (const { buffer, pageId, rowIndex } of table.scan()) {
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

    // TODO: enforce column constraints

    const rowsToUpdate: Record<string, any>[] = [];
    for await (const { buffer } of table.scan()) {
      const rowObject = Serializer.deserialize(buffer, schema);
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
          throw new Exception('E200', column.name);
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
