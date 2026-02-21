import { Token } from './query-types.js';

const TOKEN_REGEX: Record<Token['type'], RegExp> = {
  KEYWORD:
    /^(SELECT|INSERT INTO|VALUES|FROM|WHERE|DELETE FROM|CREATE TABLE|UPDATE|SET|INT|VARCHAR|ORDER BY|ASC|DESC|LIMIT|OFFSET)\b/i,
  OPERATOR: /^(<>|<=|>=|LIKE|[*=<>])/,
  NUMBER: /^\d+/,
  STRING: /^'[^']*'/,
  IDENTIFIER: /^\w+/i,
  PUNCTUATION: /^[,;()]/,
};

export class QueryTokenizer {
  static tokenize(query: string) {
    const tokens: Token[] = [];
    let cursor = 0;

    while (cursor < query.length) {
      if (/\s/.test(query[cursor])) {
        cursor++;
        continue;
      }

      let matched = false;
      for (const [type, regex] of Object.entries(TOKEN_REGEX)) {
        const match = regex.exec(query.slice(cursor));
        if (match) {
          tokens.push({ type: type as Token['type'], value: match[0] });
          cursor += match[0].length;
          matched = true;
          break;
        }
      }

      if (!matched) {
        throw new Error(`Syntax error: Unexpected token at position ${cursor}`);
      }
    }

    return tokens;
  }
}
