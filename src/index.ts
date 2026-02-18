import { Database } from './classes/database.js';
import { join } from 'node:path';
import { Buffer } from 'node:buffer';
import { uint32 } from './const.js';
import { Table } from './classes/table.js';

const db = new Database(join(import.meta.dirname, '../db/main.db'));
await db.open();

const User = {
  serialize(id: number, name: string) {
    const nameBuffer = Buffer.from(name, 'utf8');
    const nameLength = nameBuffer.length;
    const buffer = Buffer.alloc(uint32 + uint32 + nameLength);
    buffer.writeUint32BE(id, 0);
    buffer.writeUint32BE(nameLength, uint32);
    nameBuffer.copy(buffer, uint32 + uint32);
    return buffer;
  },
  deserialize(buffer: Buffer) {
    const id = buffer.readUint32BE(0);
    const nameLength = buffer.readUint32BE(uint32);
    const name = buffer.toString('utf8', uint32 + uint32, uint32 + uint32 + nameLength);
    return { id, name };
  },
};

try {
  console.log('Database opened successfully.');

  let usersTable: Table;
  const existingTable = await db.getTable('users');
  if (existingTable) {
    console.log("Found existing 'users' table");
    usersTable = existingTable;
  } else {
    console.log("Creating new 'users' table");
    usersTable = await db.createTable('users');

    console.log('Inserting sample data...');
    await usersTable.insert(User.serialize(1, 'Alice'));
    await usersTable.insert(User.serialize(2, 'Bob'));
    await usersTable.insert(User.serialize(3, 'Charlie'));
  }

  console.log();
  console.log('Scanning all users:');
  const allUsers = await usersTable.select();
  if (allUsers.length) {
    for (const user of allUsers) {
      const { id, name } = User.deserialize(user);
      console.log(`User ${id}: ${name}`);
    }
  } else console.log('No users found');
} finally {
  console.log('Closing database.');
  await db.close();
}
