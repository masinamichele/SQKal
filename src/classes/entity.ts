import { ReflectMetadata } from './reflect-metadata.js';
import { METADATA_KEY_COLUMNS } from './decorators/keys.js';
import { Buffer } from 'node:buffer';
import { sizeof_uint32 } from '../const.js';

export interface EntityType<T extends Entity> {
  new (): T;
  create(data: Omit<Partial<T>, 'serialize'>): T;
  deserialize(buffer: Buffer): T;
}

export abstract class Entity {
  static create<T extends Entity>(this: new () => T, data: Omit<Partial<T>, 'serialize'>) {
    const entity = new this();
    Object.assign(entity, data);
    return entity;
  }

  serialize(): Buffer {
    const columns = ReflectMetadata.get(METADATA_KEY_COLUMNS, this.constructor);
    if (!columns) return Buffer.alloc(0);

    const chunks: Buffer[] = [];
    for (const col of columns) {
      const value = (this as any)[col.propertyKey];
      switch (col.type) {
        case 'number': {
          const numBuffer = Buffer.alloc(sizeof_uint32);
          numBuffer.writeUint32BE(value);
          chunks.push(numBuffer);
          break;
        }
        case 'string': {
          const strBuffer = Buffer.from(value, 'utf8');
          const strLength = Buffer.alloc(sizeof_uint32);
          strLength.writeUint32BE(strBuffer.length);
          chunks.push(strLength, strBuffer);
          break;
        }
      }
    }
    return Buffer.concat(chunks);
  }

  static deserialize<T extends Entity>(this: new () => T, buffer: Buffer): T {
    const columns = ReflectMetadata.get(METADATA_KEY_COLUMNS, this);
    const data: Record<string, any> = {};
    let offset = 0;
    if (columns) {
      for (const col of columns) {
        switch (col.type) {
          case 'number': {
            data[col.propertyKey] = buffer.readUint32BE(offset);
            offset += sizeof_uint32;
            break;
          }
          case 'string': {
            const len = buffer.readUint32BE(offset);
            offset += sizeof_uint32;
            data[col.propertyKey] = buffer.toString('utf8', offset, offset + len);
            offset += len;
            break;
          }
        }
      }
    }
    const instance = new this();
    Object.assign(instance, data);
    return instance;
  }
}
