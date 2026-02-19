import { Schema } from '../catalog.js';

export interface WhereClause {
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

export interface CreateTableCommand {
  type: 'CREATE_TABLE';
  tableName: string;
  schema: Schema;
}

export interface DeleteCommand {
  type: 'DELETE';
  tableName: string;
  where: WhereClause;
}

export type Command = InsertCommand | SelectCommand | DeleteCommand | CreateTableCommand;

export interface Token {
  type: 'KEYWORD' | 'IDENTIFIER' | 'NUMBER' | 'STRING' | 'OPERATOR' | 'PUNCTUATION';
  value: string;
}
