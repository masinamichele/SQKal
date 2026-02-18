import { Database } from './database.js';
import { ReflectMetadata } from './reflect-metadata.js';
import { METADATA_KEY_COLUMNS } from './decorators/keys.js';

export interface InsertCommand {
  type: 'INSERT';
  tableName: string;
  values: (string | number)[];
}

export type Command = InsertCommand;

export class QueryParser {
  private parseValues(valuesString: string): (string | number)[] {
    return valuesString.split(/, ?/).map((value) => {
      const num = Number.parseInt(value);
      if (!Number.isNaN(num)) return num;
      return value.replaceAll(/['"]/g, '');
    });
  }

  parse(query: string): Command {
    query = query.trim().replace(/;$/, '');
    const insertRegex = /^INSERT INTO (\w+) VALUES \((.+)\)$/i;
    const match = insertRegex.exec(query);
    if (!match) return null;

    const [, tableName, valuesString] = match;
    const values = this.parseValues(valuesString);

    return {
      type: 'INSERT',
      tableName,
      values,
    };
  }
}

export class QueryRunner {
  constructor(private readonly database: Database) {}

  async run(command: Command) {
    switch (command.type) {
      case 'INSERT':
        return this.handleInsert(command);
      default:
        throw new Error(`Unknown command '${command.type}'`);
    }
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
    return 1;
  }
}
