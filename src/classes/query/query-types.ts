import { Schema } from '../catalog.js';

export type ValueType = string | number;

export type WhereClause = {
  field: string;
  operator: string;
  value: ValueType;
};

export type OrderByClause = {
  field: string;
  direction: 'ASC' | 'DESC';
};

export type LimitClause = {
  limit: number;
  offset?: number;
};

export type SetClause = Record<string, ValueType>;

type BaseCommand = {
  tableName: string;
};

export type InsertCommand = BaseCommand & {
  type: 'INSERT';
  values: ValueType[][];
};

export type SelectCommand = BaseCommand & {
  type: 'SELECT';
  fields: '*' | string[];
  where?: WhereClause;
  order?: OrderByClause;
  limit?: LimitClause;
};

export type CreateTableCommand = BaseCommand & {
  type: 'CREATE_TABLE';
  schema: Schema;
};

export type DeleteCommand = BaseCommand & {
  type: 'DELETE';
  where: WhereClause;
};

export type UpdateCommand = BaseCommand & {
  type: 'UPDATE';
  set: SetClause;
  where: WhereClause;
};

export type Command = InsertCommand | SelectCommand | DeleteCommand | CreateTableCommand | UpdateCommand;

export type Token = {
  type: 'KEYWORD' | 'IDENTIFIER' | 'NUMBER' | 'STRING' | 'OPERATOR' | 'PUNCTUATION';
  value: string;
};
