import { Buffer } from 'node:buffer';
import { sizeof_uint32, sizeof_uint8 } from '../../const.js';
import { DataType, Schema } from '../table/catalog.js';

export class Serializer {
  static serialize(obj: Record<string, any>, schema: Schema): Buffer {
    const chunks: Buffer[] = [];
    for (const column of schema) {
      const value = obj[column.name];
      const presenceFlag = Buffer.alloc(sizeof_uint8);

      if (value == null) {
        presenceFlag.writeUInt8(0x00);
        chunks.push(presenceFlag);
        continue;
      } else {
        presenceFlag.writeUInt8(0x01);
        chunks.push(presenceFlag);
      }

      switch (column.type) {
        case DataType.NUMBER: {
          const numBuffer = Buffer.alloc(sizeof_uint32);
          numBuffer.writeUint32BE(value);
          chunks.push(numBuffer);
          break;
        }
        case DataType.STRING: {
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

  static deserialize(buffer: Buffer, schema: Schema): Record<string, any> {
    const obj: Record<string, any> = {};
    let offset = 0;
    for (const column of schema) {
      const presenceFlag = buffer.readUInt8(offset);
      offset += sizeof_uint8;
      if (presenceFlag === 0x00) {
        obj[column.name] = null;
        continue;
      }
      switch (column.type) {
        case DataType.NUMBER: {
          obj[column.name] = buffer.readUint32BE(offset);
          offset += sizeof_uint32;
          break;
        }
        case DataType.STRING: {
          const len = buffer.readUint32BE(offset);
          offset += sizeof_uint32;
          obj[column.name] = buffer.toString('utf8', offset, offset + len);
          offset += len;
          break;
        }
      }
    }
    return obj;
  }
}
