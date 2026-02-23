import { BaseParser } from './base-parser.js';
import { ConditionNode, Token, TokenType, WhereClause } from './types.js';

const PRECEDENCE = <const>{ OR: 1, AND: 2 };

export class ExpressionParser extends BaseParser {
  private cursor = 0;

  constructor(private readonly tokens: Token[]) {
    super();
    this.lookahead = this.tokens[0];
  }

  protected consume({ type, value }: { type?: TokenType; value?: string } = {}) {
    const token = super.consume({ type, value });
    this.cursor++;
    this.lookahead = this.tokens[this.cursor];
    return token;
  }

  parse(): WhereClause {
    return this._parseExpression(0);
  }

  private _parseExpression(precedence: number): WhereClause {
    let left: WhereClause = this._parseAtom();

    while (this.hasMoreTokens()) {
      const operatorToken = this.peek();
      if (operatorToken.type !== 'OPERATOR' || !Object.keys(PRECEDENCE).includes(operatorToken.value)) {
        break;
      }

      const newPrecedence = PRECEDENCE[operatorToken.value as keyof typeof PRECEDENCE];
      if (newPrecedence <= precedence) {
        break;
      }

      this.consume({ type: 'OPERATOR' });
      const right = this._parseExpression(newPrecedence);
      left = {
        type: 'LOGICAL',
        operator: operatorToken.value as keyof typeof PRECEDENCE,
        left,
        right,
      };
    }

    return left;
  }

  private _parseAtom(): WhereClause {
    const nextToken = this.peek();
    if (nextToken?.type === 'PUNCTUATION' && nextToken?.value === '(') {
      this.consume({ type: 'PUNCTUATION', value: '(' });
      const expression = this._parseExpression(0);
      this.consume({ type: 'PUNCTUATION', value: ')' });
      return expression;
    } else {
      return this._parseCondition();
    }
  }

  private _parseCondition(): ConditionNode {
    const field = this.consume({ type: 'IDENTIFIER' }).value;
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

    return { type: 'CONDITION', field, operator, value };
  }
}
