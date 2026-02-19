interface WhereClause {
  field: string;
  operator: string;
  value: string | number;
}

export interface InsertCommand {
  type: 'INSERT';
  tableName: string;
  values: (string | number)[];
}

export interface SelectCommand {
  type: 'SELECT';
  tableName: string;
  fields: '*' | string[];
  where?: WhereClause;
}

export type Command = InsertCommand | SelectCommand;

export interface Token {
  type: 'KEYWORD' | 'IDENTIFIER' | 'NUMBER' | 'STRING' | 'OPERATOR' | 'PUNCTUATION';
  value: string;
}
