import {
  Command,
  CreateTableCommand,
  DeleteCommand,
  InsertCommand,
  LimitClause,
  OrderByClause,
  SelectCommand,
  SetClause,
  Token,
  UpdateCommand,
  ValueType,
  WhereClause,
} from './query-types.js';
import { QueryTokenizer } from './query-tokenizer.js';
import { Column, DataType, Schema } from '../catalog.js';

export class QueryParser {
  tokens: Token[] = [];
  cursor = 0;

  parse(query: string): Command {
    this.tokens = QueryTokenizer.tokenize(query);
    return this.buildCommand();
  }

  private peek() {
    return this.tokens[this.cursor] || null;
  }

  private consume(expectedType?: Token['type'], expectedValue?: string) {
    const token = this.peek();
    if (!token) throw new Error(`Unexpected end of query`);
    if (expectedType && token.type !== expectedType) {
      throw new Error(`Unexpected token type: ${token.type}(${token.value}) instead of ${expectedType}(_)`);
    }
    if (expectedValue && token.value !== expectedValue) {
      throw new Error(`Unexpected token value: ${token.type}(${token.value}) instead of _(${expectedValue})`);
    }
    this.cursor++;
    return token;
  }

  private buildCommand() {
    this.cursor = 0;
    const firstToken = this.peek();
    if (firstToken?.type === 'KEYWORD') {
      switch (firstToken.value) {
        case 'INSERT INTO':
          return this.parseInsertStatement();
        case 'SELECT':
          return this.parseSelectStatement();
        case 'DELETE FROM':
          return this.parseDeleteStatement();
        case 'CREATE TABLE':
          return this.parseCreateTableStatement();
        case 'UPDATE':
          return this.parseUpdateStatement();
      }
    }
  }

  private _parseIdentifiersList() {
    const identifiers: string[] = [];

    do {
      identifiers.push(this.consume('IDENTIFIER').value);
      if (this.peek()?.value === ',') {
        this.consume('PUNCTUATION', ',');
      } else break;
    } while (true);

    return identifiers;
  }

  private _parseTypedValue() {
    const token = this.consume();
    if (token.type === 'NUMBER') {
      return Number(token.value);
    } else if (token.type === 'STRING') {
      return token.value.slice(1, -1);
    } else if (token.type === 'KEYWORD' && token.value === 'NULL') {
      return null;
    } else {
      throw new Error(`Expected NUMBER or STRING in value list, but got ${token.type} (${token.value})`);
    }
  }

  private _parseValuesList() {
    const values: ValueType[] = [];

    do {
      values.push(this._parseTypedValue());
      if (this.peek()?.value === ',') {
        this.consume('PUNCTUATION', ',');
      } else break;
    } while (true);

    return values;
  }

  private _parseColumnDefinition(): Column {
    const name = this.consume('IDENTIFIER').value;
    const typeToken = this.consume('KEYWORD').value;

    let type: DataType;
    switch (typeToken) {
      case 'INT':
        type = DataType.NUMBER;
        break;
      case 'VARCHAR':
        type = DataType.STRING;
        break;
      default:
        throw new Error(`Unknown column type: ${typeToken}`);
    }

    let nullable = true;
    const nextToken = this.peek();
    if (nextToken?.type === 'KEYWORD') {
      if (nextToken.value === 'NULL') {
        this.consume('KEYWORD', 'NULL');
      } else if (nextToken.value === 'NOT NULL') {
        this.consume('KEYWORD', 'NOT NULL');
        nullable = false;
      }
    }

    return { name, type, nullable };
  }

  private _parseWhereClause(): WhereClause {
    this.consume('KEYWORD', 'WHERE');
    const field = this.consume('IDENTIFIER').value;
    const operator = this.consume('OPERATOR').value;
    const value = this._parseTypedValue();
    if (value != null && (operator === 'IS' || operator === 'IS NOT')) {
      throw new Error(`Operator ${operator} only supports NULL`);
    } else if (value == null && !(operator === 'IS' || operator === 'IS NOT')) {
      throw new Error(`Operator ${operator} cannot be used with NULL`);
    }

    return { field, operator, value };
  }

  private _parseSetClause(): SetClause {
    this.consume('KEYWORD', 'SET');
    const setClause: SetClause = {};

    do {
      const field = this.consume('IDENTIFIER').value;
      this.consume('OPERATOR', '=');
      setClause[field] = this._parseTypedValue();

      if (this.peek()?.value === ',') {
        this.consume('PUNCTUATION', ',');
      } else break;
    } while (true);

    return setClause;
  }

  private _parseOrderByClause(): OrderByClause {
    this.consume('KEYWORD', 'ORDER BY');
    const field = this.consume('IDENTIFIER').value;
    let direction: 'ASC' | 'DESC' = 'ASC';
    if (['ASC', 'DESC'].includes(this.peek()?.value)) {
      direction = this.consume('KEYWORD').value as 'ASC' | 'DESC';
    }
    return { field, direction };
  }

  private _parseLimitClause(): LimitClause {
    this.consume('KEYWORD', 'LIMIT');
    const limit = Number(this.consume('NUMBER').value);
    let offset: number = 0;
    if (this.peek()?.value === 'OFFSET') {
      this.consume('KEYWORD', 'OFFSET');
      offset = Number(this.consume('NUMBER').value);
    }
    return { limit, offset };
  }

  private parseInsertStatement(): InsertCommand {
    this.consume('KEYWORD', 'INSERT INTO');
    const tableName = this.consume('IDENTIFIER').value;
    this.consume('KEYWORD', 'VALUES');

    const values: ValueType[][] = [];
    do {
      this.consume('PUNCTUATION', '(');
      const currentValues = this._parseValuesList();
      this.consume('PUNCTUATION', ')');
      values.push(currentValues);

      if (this.peek()?.value === ',') {
        this.consume('PUNCTUATION', ',');
      } else break;
    } while (true);

    return { type: 'INSERT', tableName, values };
  }

  private parseSelectStatement(): SelectCommand {
    this.consume('KEYWORD', 'SELECT');
    let fields: '*' | string[];
    if (this.peek()?.value === '*') {
      this.consume('OPERATOR', '*');
      fields = '*';
    } else {
      fields = this._parseIdentifiersList();
    }

    this.consume('KEYWORD', 'FROM');
    const tableName = this.consume('IDENTIFIER').value;

    let where: WhereClause;
    if (this.peek()?.value === 'WHERE') {
      where = this._parseWhereClause();
    }

    let order: OrderByClause;
    if (this.peek()?.value === 'ORDER BY') {
      order = this._parseOrderByClause();
    }

    let limit: LimitClause;
    if (this.peek()?.value === 'LIMIT') {
      limit = this._parseLimitClause();
    }

    return { type: 'SELECT', tableName, fields, where, order, limit };
  }

  private parseDeleteStatement(): DeleteCommand {
    this.consume('KEYWORD', 'DELETE FROM');
    const tableName = this.consume('IDENTIFIER').value;
    const where = this._parseWhereClause();
    return { type: 'DELETE', tableName, where };
  }

  private parseCreateTableStatement(): CreateTableCommand {
    this.consume('KEYWORD', 'CREATE TABLE');
    const tableName = this.consume('IDENTIFIER').value;
    this.consume('PUNCTUATION', '(');

    const schema: Schema = [];
    do {
      schema.push(this._parseColumnDefinition());
      if (this.peek()?.value === ',') {
        this.consume('PUNCTUATION', ',');
      } else break;
    } while (true);

    this.consume('PUNCTUATION', ')');

    return { type: 'CREATE_TABLE', tableName, schema };
  }

  private parseUpdateStatement(): UpdateCommand {
    this.consume('KEYWORD', 'UPDATE');
    const tableName = this.consume('IDENTIFIER').value;
    const set = this._parseSetClause();
    const where = this._parseWhereClause();
    return { type: 'UPDATE', tableName, set, where };
  }
}
