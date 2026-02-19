import { Database } from './classes/database.js';
import { join } from 'node:path';
import { Table } from './classes/table.js';
import { User } from './entities/User.js';
import { rm } from 'node:fs/promises';

await rm(join(import.meta.dirname, '../db/main.db'), { force: true });

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
    await db.query("INSERT INTO users VALUES (1, 'Alice')");
    await db.query("INSERT INTO users VALUES (2, 'Bob')");
    await db.query("INSERT INTO users VALUES (3, 'Charlie')");
  }

  console.log();
  console.log('Scanning all users:');
  let allUsers = await db.query('SELECT * FROM users');
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
  allUsers = await db.query('SELECT id, name FROM users');
  if (allUsers.length) {
    for (const user of allUsers) {
      console.log(`User ${user.id}: ${user.name}`);
    }
  } else console.log('No users found');

  console.log();
  console.log('Finding specific user:');
  const user = await db.query('SELECT * FROM users WHERE id = 1');
  console.log(user);
} finally {
  console.log('Closing database.');
  await db.close();
}
