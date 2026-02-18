import { Entity } from '../entity.js';
import { registerEntity } from './registry.js';

export function EntityModel(tableName: string) {
  return function <T extends typeof Entity>(constructor: T) {
    registerEntity(tableName, constructor);
  };
}
