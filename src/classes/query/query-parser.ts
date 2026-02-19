import { Command, InsertCommand, SelectCommand, Token } from './types.js';
import { QueryTokenizer } from './query-tokenizer.js';

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
      throw new Error(`Unexpected token type: ${token.type} instead of ${expectedType}`);
    }
    if (expectedValue && token.value !== expectedValue) {
      throw new Error(`Unexpected token value: ${token.value} instead of ${expectedValue}`);
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
      }
    }
  }

  private parseIdentifiersList() {
    const identifiers: string[] = [];

    do {
      identifiers.push(this.consume('IDENTIFIER').value);
      if (this.peek()?.value === ',') {
        this.consume('PUNCTUATION', ',');
      } else break;
    } while (true);

    return identifiers;
  }

  private parseValuesList() {
    const values: (string | number)[] = [];

    do {
      const token = this.consume();
      if (token.type === 'NUMBER') {
        values.push(Number(token.value));
      } else if (token.type === 'STRING') {
        values.push(token.value.slice(1, -1));
      } else {
        throw new Error(`Expected NUMBER or STRING in value list, but got ${token.type} (${token.value})`);
      }

      if (this.peek()?.value === ',') {
        this.consume('PUNCTUATION', ',');
      } else break;
    } while (true);

    return values;
  }

  private parseInsertStatement(): InsertCommand {
    this.consume('KEYWORD', 'INSERT INTO');
    const tableName = this.consume('IDENTIFIER').value;
    this.consume('KEYWORD', 'VALUES');
    this.consume('PUNCTUATION', '(');

    const values = this.parseValuesList();

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
      fields = this.parseIdentifiersList();
    }

    this.consume('KEYWORD', 'FROM');
    const tableName = this.consume('IDENTIFIER').value;

    return { type: 'SELECT', tableName, fields };
  }
}
