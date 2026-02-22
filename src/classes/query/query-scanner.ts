import { Token, TokenType } from './query-types.js';

export const RESERVED_WORDS = new Map<string, TokenType>([
  // Multi-word Keywords
  ['INSERT INTO', 'KEYWORD'],
  ['DELETE FROM', 'KEYWORD'],
  ['CREATE TABLE', 'KEYWORD'],
  ['ORDER BY', 'KEYWORD'],

  // Single-word Keywords
  ['SELECT', 'KEYWORD'],
  ['VALUES', 'KEYWORD'],
  ['FROM', 'KEYWORD'],
  ['WHERE', 'KEYWORD'],
  ['UPDATE', 'KEYWORD'],
  ['SET', 'KEYWORD'],
  ['INT', 'KEYWORD'],
  ['VARCHAR', 'KEYWORD'],
  ['ASC', 'KEYWORD'],
  ['DESC', 'KEYWORD'],
  ['LIMIT', 'KEYWORD'],
  ['OFFSET', 'KEYWORD'],
  ['NULL', 'KEYWORD'],
  ['NOT', 'KEYWORD'],

  // Operators that are words
  ['LIKE', 'OPERATOR'],
  ['IS', 'OPERATOR'],
  ['AND', 'OPERATOR'],
  ['OR', 'OPERATOR'],
]);

const SORTED_RESERVED_WORDS = Array.from(RESERVED_WORDS.keys()).sort((a, b) => b.length - a.length);

export class QueryScanner {
  private cursor = 0;

  private get char() {
    return this.query[this.cursor];
  }

  private get next() {
    return this.query[this.cursor + 1];
  }

  private get hasMore() {
    return this.cursor < this.query.length;
  }

  constructor(private readonly query: string) {}

  *iterator(): Generator<Token> {
    while (this.hasMore) {
      this._skipWhitespace();

      if (/[a-z]/i.test(this.char)) {
        yield this._handleWord();
        continue;
      }

      if (/\d/.test(this.char)) {
        yield this._handleNumber();
        continue;
      }

      if (this.char === "'") {
        yield this._handleString();
        continue;
      }

      if (/[,;()]/.test(this.char)) {
        yield this._handlePunctuation();
        continue;
      }

      if (/[*=<>]/.test(this.char)) {
        yield this._handleSymbolOperator();
        continue;
      }

      throw new Error(`Syntax error: Unexpected token '${this.char}' at position ${this.cursor}`);
    }
  }

  private _skipWhitespace() {
    while (this.hasMore && /\s/.test(this.char)) {
      this.cursor++;
    }
  }

  private _handleWord(): Token {
    const rest = this.query.slice(this.cursor).toUpperCase();

    for (const word of SORTED_RESERVED_WORDS) {
      if (rest.startsWith(word)) {
        const nextChar = this.query[this.cursor + word.length];
        if (nextChar == null || /\W/.test(nextChar)) {
          this.cursor += word.length;
          return { type: RESERVED_WORDS.get(word), value: word };
        }
      }
    }

    let value = '';
    while (this.hasMore && /\w/.test(this.char)) {
      value += this.char;
      this.cursor++;
    }

    return { type: 'IDENTIFIER', value };
  }

  private _handleNumber(): Token {
    let value = '';
    while (this.hasMore && /\d/.test(this.char)) {
      value += this.char;
      this.cursor++;
    }
    return { type: 'NUMBER', value };
  }

  private _handleString(): Token {
    this.cursor++;
    let value = '';
    while (this.hasMore && this.char !== "'") {
      value += this.char;
      this.cursor++;
    }
    if (this.char !== "'") {
      throw new Error(`Syntax error: Unterminated string literal`);
    }
    this.cursor++;
    return { type: 'STRING', value };
  }

  private _handlePunctuation(): Token {
    const value = this.query[this.cursor];
    this.cursor++;
    return { type: 'PUNCTUATION', value };
  }

  private _handleSymbolOperator(): Token {
    if (this.char === '<' && this.next === '>') {
      this.cursor += 2;
      return { type: 'OPERATOR', value: '<>' };
    }
    if (this.char === '<' && this.next === '=') {
      this.cursor += 2;
      return { type: 'OPERATOR', value: '<=' };
    }
    if (this.char === '>' && this.next === '=') {
      this.cursor += 2;
      return { type: 'OPERATOR', value: '>=' };
    }

    const value = this.char;
    this.cursor++;
    return { type: 'OPERATOR', value };
  }
}
