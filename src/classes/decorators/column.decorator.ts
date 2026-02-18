import { ReflectMetadata } from '../reflect-metadata.js';
import { METADATA_KEY_COLUMNS } from './keys.js';

export function Column(type: 'number' | 'string') {
  return function (target: any, propertyKey: string) {
    const columns = ReflectMetadata.get(METADATA_KEY_COLUMNS, target.constructor) || [];
    columns.push({ propertyKey, type });
    ReflectMetadata.define(METADATA_KEY_COLUMNS, columns, target.constructor);
  };
}
