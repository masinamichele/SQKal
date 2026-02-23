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
  TokenType,
  UpdateCommand,
  ValueType,
  WhereClause,
} from './query-types.js';
import { QueryScanner } from './query-scanner.js';
import { Column, DataType, Schema } from '../catalog.js';

export class QueryParser {
  private scanner: Generator<Token>;
  private lookahead: Token = null;

  parse(query: string): Command {
    this.scanner = new QueryScanner(query).iterator();
    this.lookahead = this.scanner.next().value;
    return this.buildCommand();
  }

  private peek() {
    return this.lookahead;
  }

  private consume({ type, value, next }: { type?: TokenType; value?: string; next?: TokenType } = {}) {
    const token = this.lookahead;
    if (!token) throw new Error(`Unexpected end of query`);
    if (type && token.type !== type) {
      throw new Error(`Unexpected token type: ${token.type}(${token.value}) instead of ${type}(_)`);
    }
    if (value && token.value !== value) {
      throw new Error(`Unexpected token value: ${token.type}(${token.value}) instead of _(${value})`);
    }
    this.lookahead = this.scanner.next(next).value;
    return token;
  }

  private buildCommand() {
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
      identifiers.push(this.consume({ type: 'IDENTIFIER' }).value);
      if (this.peek()?.value === ',') {
        this.consume({ type: 'PUNCTUATION', value: ',', next: 'IDENTIFIER' });
      } else break;
    } while (true);

    return identifiers;
  }

  private _parseTypedValue() {
    const token = this.consume();
    if (token.type === 'NUMBER') {
      return Number(token.value);
    } else if (token.type === 'STRING') {
      return token.value;
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
        this.consume({ type: 'PUNCTUATION', value: ',' });
      } else break;
    } while (true);

    return values;
  }

  private _parseColumnDefinition(): Column {
    const name = this.consume({ type: 'IDENTIFIER' }).value;
    const typeToken = this.consume({ type: 'KEYWORD' }).value;

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
        this.consume({ type: 'KEYWORD', value: 'NULL' });
      } else if (nextToken.value === 'NOT') {
        this.consume({ type: 'KEYWORD', value: 'NOT' });
        this.consume({ type: 'KEYWORD', value: 'NULL' });
        nullable = false;
      }
    }

    return { name, type, nullable };
  }

  private _parseWhereClause(): WhereClause {
    this.consume({ type: 'KEYWORD', value: 'WHERE', next: 'IDENTIFIER' });
    const field = this.consume({ type: 'IDENTIFIER', next: 'OPERATOR' }).value;
    let operator = this.consume({ type: 'OPERATOR' }).value;
    if (operator === 'IS' && this.peek()?.value === 'NOT') {
      this.consume({ type: 'KEYWORD', value: 'NOT' });
      operator = 'IS NOT';
    }
    const value = this._parseTypedValue();
    if (value != null && (operator === 'IS' || operator === 'IS NOT')) {
      throw new Error(`Operator ${operator} only supports NULL`);
    } else if (value == null && !(operator === 'IS' || operator === 'IS NOT')) {
      throw new Error(`Operator ${operator} cannot be used with NULL`);
    }

    return { field, operator, value };
  }

  private _parseSetClause(): SetClause {
    this.consume({ type: 'KEYWORD', value: 'SET', next: 'IDENTIFIER' });
    const setClause: SetClause = {};

    do {
      const field = this.consume({ type: 'IDENTIFIER', next: 'OPERATOR' }).value;
      this.consume({ type: 'OPERATOR', value: '=' });
      setClause[field] = this._parseTypedValue();

      if (this.peek()?.value === ',') {
        this.consume({ type: 'PUNCTUATION', value: ',', next: 'IDENTIFIER' });
      } else break;
    } while (true);

    return setClause;
  }

  private _parseOrderByClause(): OrderByClause {
    this.consume({ type: 'KEYWORD', value: 'ORDER BY', next: 'IDENTIFIER' });
    const field = this.consume({ type: 'IDENTIFIER' }).value;
    let direction: 'ASC' | 'DESC' = 'ASC';
    if (['ASC', 'DESC'].includes(this.peek()?.value)) {
      direction = this.consume({ type: 'KEYWORD' }).value as 'ASC' | 'DESC';
    }
    return { field, direction };
  }

  private _parseLimitClause(): LimitClause {
    this.consume({ type: 'KEYWORD', value: 'LIMIT', next: 'NUMBER' });
    const limit = Number(this.consume({ type: 'NUMBER' }).value);
    let offset: number = 0;
    if (this.peek()?.value === 'OFFSET') {
      this.consume({ type: 'KEYWORD', value: 'OFFSET', next: 'NUMBER' });
      offset = Number(this.consume({ type: 'NUMBER' }).value);
    }
    return { limit, offset };
  }

  private parseInsertStatement(): InsertCommand {
    this.consume({ type: 'KEYWORD', value: 'INSERT INTO', next: 'IDENTIFIER' });
    const tableName = this.consume({ type: 'IDENTIFIER', next: 'KEYWORD' }).value;
    this.consume({ type: 'KEYWORD', value: 'VALUES', next: 'PUNCTUATION' });

    const values: ValueType[][] = [];
    do {
      this.consume({ type: 'PUNCTUATION', value: '(' });
      const currentValues = this._parseValuesList();
      this.consume({ type: 'PUNCTUATION', value: ')' });
      values.push(currentValues);

      if (this.peek()?.value === ',') {
        this.consume({ type: 'PUNCTUATION', value: ',', next: 'PUNCTUATION' });
      } else break;
    } while (true);

    return { type: 'INSERT', tableName, values };
  }

  private parseSelectStatement(): SelectCommand {
    this.consume({ type: 'KEYWORD', value: 'SELECT' });
    let fields: '*' | string[];
    if (this.peek()?.value === '*') {
      this.consume({ type: 'OPERATOR', value: '*' });
      fields = '*';
    } else {
      fields = this._parseIdentifiersList();
    }

    this.consume({ type: 'KEYWORD', value: 'FROM', next: 'IDENTIFIER' });
    const tableName = this.consume({ type: 'IDENTIFIER' }).value;

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
    this.consume({ type: 'KEYWORD', value: 'DELETE FROM', next: 'IDENTIFIER' });
    const tableName = this.consume({ type: 'IDENTIFIER', next: 'KEYWORD' }).value;
    const where = this._parseWhereClause();
    return { type: 'DELETE', tableName, where };
  }

  private parseCreateTableStatement(): CreateTableCommand {
    this.consume({ type: 'KEYWORD', value: 'CREATE TABLE', next: 'IDENTIFIER' });
    const tableName = this.consume({ type: 'IDENTIFIER', next: 'PUNCTUATION' }).value;
    this.consume({ type: 'PUNCTUATION', value: '(' });

    const schema: Schema = [];
    do {
      schema.push(this._parseColumnDefinition());
      if (this.peek()?.value === ',') {
        this.consume({ type: 'PUNCTUATION', value: ',', next: 'IDENTIFIER' });
      } else break;
    } while (true);

    this.consume({ type: 'PUNCTUATION', value: ')' });

    return { type: 'CREATE_TABLE', tableName, schema };
  }

  private parseUpdateStatement(): UpdateCommand {
    this.consume({ type: 'KEYWORD', value: 'UPDATE', next: 'IDENTIFIER' });
    const tableName = this.consume({ type: 'IDENTIFIER', next: 'KEYWORD' }).value;
    const set = this._parseSetClause();
    const where = this._parseWhereClause();
    return { type: 'UPDATE', tableName, set, where };
  }
}
