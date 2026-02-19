import { Database } from '../database.js';
import { Command, InsertCommand, SelectCommand } from './types.js';
import { ReflectMetadata } from '../reflect-metadata.js';
import { METADATA_KEY_COLUMNS } from '../decorators/keys.js';

export class QueryRunner {
  constructor(private readonly database: Database) {}

  async run(command: Command): Promise<any[]> {
    switch (command.type) {
      case 'INSERT':
        return this.handleInsert(command);
      case 'SELECT':
        return this.handleSelect(command);
      default:
        throw new Error(`Unknown command '${(command as any)?.type}'`);
    }
  }

  private pick(item: any, fields: string[]) {
    return fields.reduce<any>((obj, field) => {
      obj[field] = item[field];
      return obj;
    }, {});
  }

  private async handleInsert(command: InsertCommand) {
    const table = await this.database.getTable(command.tableName);
    if (!table) {
      throw new Error(`Table '${command.tableName}' not found.`);
    }
    const entityClass = table.getEntityClass();
    const columns = ReflectMetadata.get<any[]>(METADATA_KEY_COLUMNS, entityClass);
    const data: Record<string, any> = {};
    columns.forEach((col, index) => {
      data[col.propertyKey] = command.values[index];
    });
    const entity = entityClass.create(data);
    await table.insert(entity);
    return [1];
  }

  private async handleSelect(command: SelectCommand) {
    const table = await this.database.getTable(command.tableName);
    if (!table) {
      throw new Error(`Table '${command.tableName}' not found.`);
    }

    let rows = await table.select();

    if (command.where) {
      const { field, operator, value } = command.where;
      if (operator === '=') {
        rows = rows.filter((row) => (row as any)[field] === value);
      }
    }

    if (command.fields === '*') {
      rows = rows.map((row) => row.raw());
    } else {
      rows = rows.map((row) => this.pick(row, command.fields as string[]));
    }

    return rows;
  }
}
