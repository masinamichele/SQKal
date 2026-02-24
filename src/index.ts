import { Database } from './classes/database.js';
import { join } from 'node:path';
import { rm } from 'node:fs/promises';
import { test, describe, before, after } from 'node:test';
import assert from 'node:assert/strict';

describe('Database Engine Integration Tests', () => {
  const dbPath = join(import.meta.dirname, '../db/main.db');
  let db: Database;

  before(async () => {
    await rm(dbPath, { force: true });
    console.log('[debug] Previous database deleted');
    db = Database.getInstance(dbPath);
    await db.open();
    console.log('Database opened successfully');
  });

  after(async () => {
    await db.close();
    console.log('Database closed');
  });

  test('should correctly handle CREATE TABLE statements', async () => {
    const [result] = await db.exec`CREATE TABLE users (id INT NOT NULL, name VARCHAR)`;
    assert.notEqual(result, null, 'CREATE TABLE should return a result');
  });

  test('should correctly handle multi-row INSERT statements', async () => {
    const [insertCount] = await db.exec`INSERT INTO users VALUES (1, 'Alice'),
                                                                 (2, 'Bob'),
                                                                 (3, 'Charlie'),
                                                                 (4, 'David'),
                                                                 (5, 'Eve'),
                                                                 (6, null)`;
    assert.equal(insertCount, 6, 'INSERT should return the number of inserted rows');
  });

  test('should correctly handle SELECT * statements', async () => {
    const allUsers = await db.exec`SELECT * FROM users`;
    assert.equal(allUsers.length, 6, 'SELECT * should return all rows after insertion');
  });

  test('should correctly handle DELETE statements', async () => {
    const [deleteCount] = await db.exec`DELETE FROM users WHERE id = 2`;
    assert.equal(deleteCount, 1, 'DELETE should return the correct number of deleted rows');
    const [deleteCountFail] = await db.exec`DELETE FROM users WHERE id = 999`;
    assert.equal(deleteCountFail, 0, 'DELETE should return the correct number of deleted rows');
    const remainingUsers = await db.exec`SELECT * FROM users`;
    assert.equal(remainingUsers.length, 5, 'DELETE should remove the row from the table');
  });

  test('should correctly handle UPDATE statements', async () => {
    const [updateCount] = await db.exec`UPDATE users SET name = 'Ally' WHERE id = 1`;
    assert.equal(updateCount, 1, 'UPDATE should return the correct number of updated rows');
    const updatedUser = await db.exec`SELECT name FROM users WHERE id = 1`;
    assert.deepStrictEqual(updatedUser, [{ name: 'Ally' }], 'UPDATE should correctly modify the data');
  });

  test('should correctly handle complex WHERE clauses with AND/OR and precedence', async () => {
    const complexResult =
      await db.exec`SELECT id FROM users WHERE id > 2 AND (name LIKE 'C%' OR name = 'David') ORDER BY id`;
    assert.deepStrictEqual(complexResult, [{ id: 3 }, { id: 4 }], 'Complex WHERE should correctly filter and order');
  });

  test('should correctly handle WHERE with IS NULL', async () => {
    const nullResult = await db.exec`SELECT id FROM users WHERE name IS NULL`;
    assert.deepStrictEqual(nullResult, [{ id: 6 }], 'IS NULL should find rows with null values');
  });

  test('should correctly handle WHERE with IS NOT NULL', async () => {
    const notNullResult = await db.exec`SELECT * FROM users WHERE name IS NOT NULL`;
    assert.equal(notNullResult.length, 4, 'IS NOT NULL should find rows with non-null values');
  });

  test('should correctly handle LIMIT and OFFSET clauses', async () => {
    const pagedResult = await db.exec`SELECT id FROM users ORDER BY id ASC LIMIT 2 OFFSET 1`;
    assert.deepStrictEqual(pagedResult, [{ id: 3 }, { id: 4 }], 'LIMIT/OFFSET should return the correct slice of data');
  });

  test('should fail when violating constraints', async () => {
    // await assert.rejects(
    //   db.exec`INSERT INTO users VALUES (1, 'Duplicate')`,
    //   'INSERT should fail on duplicate primary key (or UNIQUE constraint if we add it)',
    // );

    await assert.rejects(
      db.exec`UPDATE users SET id = null WHERE id = 1`,
      'UPDATE should fail when setting a NOT NULL column to NULL',
    );
  });

  describe('SELECT statement', () => {
    test('should correctly filter with all comparison operators', async () => {
      const greaterThan = await db.exec`SELECT id FROM users WHERE id > 3`;
      assert.deepStrictEqual(greaterThan.map((r) => r.id).sort(), [4, 5, 6], 'Operator > failed');

      const lessThan = await db.exec`SELECT id FROM users WHERE id < 3`;
      assert.deepStrictEqual(lessThan.map((r) => r.id).sort(), [1], 'Operator < failed');

      const greaterOrEqual = await db.exec`SELECT id FROM users WHERE id >= 3`;
      assert.deepStrictEqual(greaterOrEqual.map((r) => r.id).sort(), [3, 4, 5, 6], 'Operator >= failed');

      const lessOrEqual = await db.exec`SELECT id FROM users WHERE id <= 3`;
      assert.deepStrictEqual(lessOrEqual.map((r) => r.id).sort(), [1, 3], 'Operator <= failed');

      const notEqual = await db.exec`SELECT id FROM users WHERE id <> 3`;
      assert.deepStrictEqual(notEqual.map((r) => r.id).sort(), [1, 4, 5, 6], 'Operator <> failed');
    });

    test('should correctly sort with ORDER BY', async () => {
      const defaultOrder = await db.exec`SELECT id FROM users WHERE id > 1 ORDER BY id`;
      assert.deepStrictEqual(
        defaultOrder.map((r) => r.id),
        [3, 4, 5, 6],
        'Default ORDER BY should be ASC',
      );

      const ascOrder = await db.exec`SELECT id FROM users WHERE id > 1 ORDER BY id ASC`;
      assert.deepStrictEqual(
        ascOrder.map((r) => r.id),
        [3, 4, 5, 6],
        'Explicit ASC ORDER BY should work correctly',
      );

      const descOrder = await db.exec`SELECT id FROM users WHERE id > 1 ORDER BY id DESC`;
      assert.deepStrictEqual(
        descOrder.map((r) => r.id),
        [6, 5, 4, 3],
        'DESC ORDER BY should work correctly',
      );
    });

    test('should correctly filter with LIKE patterns', async () => {
      const startsWith = await db.exec`SELECT name FROM users WHERE name LIKE 'A%'`;
      assert.deepStrictEqual(startsWith, [{ name: 'Ally' }], "LIKE 'A%' failed");

      const endsWith = await db.exec`SELECT name FROM users WHERE name LIKE '%e'`;
      assert.deepStrictEqual(endsWith, [{ name: 'Charlie' }, { name: 'Eve' }], "LIKE '%e' failed");

      const contains = await db.exec`SELECT name FROM users WHERE name LIKE '%a%'`;
      assert.deepStrictEqual(contains.map((r) => r.name).sort(), ['Ally', 'Charlie', 'David'], "LIKE '%a%' failed");
    });
  });
});
