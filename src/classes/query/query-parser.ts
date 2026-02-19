import {
  Command,
  CreateTableCommand,
  DeleteCommand,
  InsertCommand,
  SelectCommand,
  SetClause,
  Token,
  UpdateCommand,
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
    } else {
      throw new Error(`Expected NUMBER or STRING in value list, but got ${token.type} (${token.value})`);
    }
  }

  private _parseValuesList() {
    const values: (string | number)[] = [];

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
    switch (typeToken.toUpperCase()) {
      case 'INT':
        type = DataType.NUMBER;
        break;
      case 'VARCHAR':
        type = DataType.STRING;
        break;
      default:
        throw new Error(`Unknown column type: ${typeToken}`);
    }
    return { name, type };
  }

  private _parseWhereClause(): WhereClause {
    this.consume('KEYWORD', 'WHERE');
    const field = this.consume('IDENTIFIER').value;
    const operator = this.consume('OPERATOR').value;
    const value = this._parseTypedValue();
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

  private parseInsertStatement(): InsertCommand {
    this.consume('KEYWORD', 'INSERT INTO');
    const tableName = this.consume('IDENTIFIER').value;
    this.consume('KEYWORD', 'VALUES');
    this.consume('PUNCTUATION', '(');

    const values = this._parseValuesList();

    this.consume('PUNCTUATION', ')');

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
    if (this.peek()?.value.toUpperCase() === 'WHERE') {
      where = this._parseWhereClause();
    }

    return { type: 'SELECT', tableName, fields, where };
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
