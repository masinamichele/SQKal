import { Token, TokenType } from './types.js';
import { Exception } from '../common/errors.js';

export abstract class BaseParser {
  protected lookahead: Token = null;

  protected peek() {
    return this.lookahead;
  }

  protected consume({ type, value }: { type?: TokenType; value?: string } = {}) {
    const token = this.lookahead;
    if (!token) throw new Exception('E100');
    if (type && token.type !== type) {
      throw new Exception('E101', `${token.type}(${token.value}) instead of ${type}(_)`);
    }
    if (value && token.value !== value) {
      throw new Exception('E102', `${token.type}(${token.value}) instead of _(${value})`);
    }
    return token;
  }

  protected hasMoreTokens() {
    return this.peek() != null;
  }

  protected _parseTypedValue() {
    const token = this.consume();
    if (token.type === 'NUMBER') {
      return Number(token.value);
    } else if (token.type === 'STRING') {
      return token.value;
    } else if (token.type === 'KEYWORD' && token.value === 'NULL') {
      return null;
    } else {
      throw new Exception('E106', `Got ${token.type} (${token.value})`);
    }
  }
}
