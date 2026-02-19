import { Database } from './classes/database.js';
import { join } from 'node:path';
import { rm } from 'node:fs/promises';

await rm(join(import.meta.dirname, '../db/main.db'), { force: true });
console.log('[debug] Previous database deleted.');

const db = new Database(join(import.meta.dirname, '../db/main.db'));
await db.open();
console.log('Database opened successfully.');

try {
  console.log('Creating table...');
  await db.query('CREATE TABLE users (id INT, name VARCHAR)');
  console.log('Inserting sample data...');
  await db.query("INSERT INTO users VALUES (1, 'Alice')");
  await db.query("INSERT INTO users VALUES (2, 'Bob')");
  await db.query("INSERT INTO users VALUES (3, 'Charlie')");

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
  const [delCount1] = await db.query('DELETE FROM users WHERE id = 2');
  if (delCount1) console.log('Deletion successful.');
  else console.log('User to delete was not found.');

  console.log();
  console.log('Deleting user with ID 4 (should fail)');
  const [delCount2] = await db.query('DELETE FROM users WHERE id = 4');
  if (delCount2) console.log('Deletion successful.');
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

  console.log();
  console.log('Updating user:');
  await db.query("UPDATE users SET name = 'Eve' WHERE id = 1");
  const updatedUser = await db.query('SELECT * FROM users WHERE id = 1');
  console.log(updatedUser);

  console.log();
  console.log('Finding specific users:');
  const users = await db.query('SELECT * FROM users WHERE id >= 1');
  console.log(users);
} finally {
  console.log('Closing database.');
  await db.close();
}
