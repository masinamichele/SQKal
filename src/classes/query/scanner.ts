import { Token, TokenType } from './types.js';
import { Exception } from '../common/errors.js';

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
  ['PRIMARY KEY', 'KEYWORD'],
  ['AUTOINCREMENT', 'KEYWORD'],
  ['UNIQUE', 'KEYWORD'],

  // Operators that are words
  ['LIKE', 'OPERATOR'],
  ['IS', 'OPERATOR'],
  ['AND', 'OPERATOR'],
  ['OR', 'OPERATOR'],
]);

const SORTED_RESERVED_WORDS = Array.from(RESERVED_WORDS.keys()).sort((a, b) => b.length - a.length);

export class Scanner {
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

  private get rest() {
    return this.query.slice(this.cursor).toUpperCase();
  }

  constructor(private readonly query: string) {}

  *iterator(): Generator<Token> {
    let hint: TokenType = null;
    while (this.hasMore) {
      hint = yield this.nextToken(hint);
    }
  }

  private nextToken(hint?: TokenType) {
    this._skipWhitespace();
    if (!this.hasMore) return null;

    const isAskingFor = (type: TokenType) => {
      return !hint || hint === type;
    };

    const reserved = this._handleReservedWord();
    if (reserved && isAskingFor(reserved.type)) return reserved;

    if (isAskingFor('IDENTIFIER') && /[a-z]/i.test(this.char)) {
      return this._handleIdentifier();
    }
    if (isAskingFor('NUMBER') && /\d/.test(this.char)) {
      return this._handleNumber();
    }
    if (isAskingFor('STRING') && this.char === "'") {
      return this._handleString();
    }
    if (isAskingFor('PUNCTUATION') && /[,;()]/.test(this.char)) {
      return this._handlePunctuation();
    }
    if (isAskingFor('OPERATOR') && /[*=<>]/.test(this.char)) {
      return this._handleSymbolOperator();
    }

    const hintMessage = hint ? ` (hint: ${hint})` : '';
    const details = `Syntax error: Unexpected token '${this.char}' at position ${this.cursor}${hintMessage}`;
    throw new Exception('E104', details);
  }

  private _skipWhitespace() {
    while (this.hasMore && /\s/.test(this.char)) {
      this.cursor++;
    }
  }

  private _handleReservedWord(): Token {
    for (const word of SORTED_RESERVED_WORDS) {
      if (this.rest.startsWith(word)) {
        const nextChar = this.query[this.cursor + word.length];
        if (nextChar == null || /\W/.test(nextChar)) {
          this.cursor += word.length;
          return { type: RESERVED_WORDS.get(word), value: word };
        }
      }
    }
    return null;
  }

  private _handleIdentifier(): Token {
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
      throw new Exception('E105');
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
