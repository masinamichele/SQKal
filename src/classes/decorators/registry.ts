import { Entity } from '../entity.js';

const registry = new Map<string, typeof Entity>();

export function registerEntity(tableName: string, constructor: typeof Entity) {
  if (registry.has(tableName)) {
    throw new Error(`Duplicated table name in entity registry: ${tableName}`);
  }
  registry.set(tableName, constructor);
}

export function getEntityClass(tableName: string) {
  return registry.get(tableName);
}
