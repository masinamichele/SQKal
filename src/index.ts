import { Database } from './classes/database.js';
import { join } from 'node:path';
import { Table } from './classes/table.js';
import { User } from './entities/User.js';

const db = new Database(join(import.meta.dirname, '../db/main.db'));
await db.open();

try {
  console.log('Database opened successfully.');

  let usersTable: Table<User>;
  const existingTable = await db.getTable<User>('users');
  if (existingTable) {
    console.log("Found existing 'users' table");
    usersTable = existingTable;
  } else {
    console.log("Creating new 'users' table");
    usersTable = await db.createTable<User>('users');

    console.log('Inserting sample data...');
    await usersTable.insert(User.create({ id: 1, name: 'Alice' }));
    await usersTable.insert(User.create({ id: 2, name: 'Bob' }));
    await usersTable.insert(User.create({ id: 3, name: 'Charlie' }));
  }

  console.log();
  console.log('Scanning all users:');
  let allUsers = await usersTable.select();
  if (allUsers.length) {
    for (const user of allUsers) {
      console.log(`User ${user.id}: ${user.name}`);
    }
  } else console.log('No users found');

  console.log();
  console.log('Deleting user with ID 2 (should succeed)');
  let deleted = await usersTable.delete(User.create({ id: 2, name: 'Bob' }));
  if (deleted) console.log('Deletion successful.');
  else console.log('User to delete was not found.');

  console.log();
  console.log('Deleting user with ID 4 (should fail)');
  deleted = await usersTable.delete(User.create({ id: 4, name: 'Donkey' }));
  if (deleted) console.log('Deletion successful.');
  else console.log('User to delete was not found.');

  console.log();
  console.log('Scanning all users:');
  allUsers = await usersTable.select();
  if (allUsers.length) {
    for (const user of allUsers) {
      console.log(`User ${user.id}: ${user.name}`);
    }
  } else console.log('No users found');
} finally {
  console.log('Closing database.');
  await db.close();
}
