import { Schema } from '../table/catalog.js';

export type ValueType = string | number | null;

export type ConditionNode = {
  type: 'CONDITION';
  field: string;
  operator: string;
  value: ValueType;
};

export type LogicalNode = {
  type: 'LOGICAL';
  operator: 'AND' | 'OR';
  left: WhereClause;
  right: WhereClause;
};

export type WhereClause = ConditionNode | LogicalNode;

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

export type TokenType = 'KEYWORD' | 'IDENTIFIER' | 'NUMBER' | 'STRING' | 'OPERATOR' | 'PUNCTUATION';

export type Token = {
  type: TokenType;
  value: string;
};
