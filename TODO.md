# Project TODO

This file tracks the implementation progress of our database engine.

## Completed

- [x] **Core Storage Layer (`DiskManager`)**
  - [x] Create/Open database file.
  - [x] Implement page-based I/O (`readPage`, `writePage`).
  - [x] Implement page allocation (`allocatePage`).
  - [x] Add a human-readable debug log.

- [x] **Page Structure (`Page`)**
  - [x] Define page header with `rowCount`, `freeSpaceOffset`, and `nextPageId`.
  - [x] Implement a static `initialize` method for new pages.
  - [x] Refactor to a "Slotted Page" structure with row insertion, deletion, and in-place defragmentation.

- [x] **Higher-Level Abstractions**
  - [x] Create a `Table` class to manage multiple pages.
  - [x] Implement a Catalog to store and retrieve table schemas in a binary format.

- [x] **Buffer Management**
  - [x] Create a `DoublyLinkedList` for the LRU replacer.
  - [x] Implement `fetchPage` in `BufferPoolManager` with LRU eviction logic.
  - [x] Track "dirty" pages (modified pages) and write them to disk before eviction.
  - [x] Implement page "pinning" to prevent active pages from being evicted.
  - [x] Refactor all core classes to use the `BufferPoolManager`.

- [x] **Query Engine**
  - [x] Refactor parser to be a two-stage (tokenizer/parser) process.
  - [x] Implement `INSERT`, `SELECT` (with projection), `DELETE`, and `CREATE TABLE` commands.
  - [x] Implement simple `WHERE` clause support (`=`).

## To Do

- **Architecture**
  - [ ] Implement a custom, numbered error system.
  - [ ] Implement a Dependency Injection (DI) container to manage services.

- **Query Engine**
  - [ ] Implement `UPDATE` command.
  - [ ] Expand `WHERE` clause support (`>`, `<`, `!=`, `AND`, `OR`).
  - [ ] Support multi-row `INSERT` statements.
  - [ ] Create a query planner.

- **Production-Grade Features**
  - [ ] **Concurrency Control:** Implement a locking mechanism (e.g., page-level or row-level locks).
  - [ ] **Transactions:** Implement ACID-compliant transactions with `BEGIN`, `COMMIT`, and `ROLLBACK`.
  - [ ] **Advanced Indexing:** Design and implement a B+ Tree index structure.

- **Vacuum / Space Reclamation**
  - [ ] Implement a global free-page list to track and reuse deleted pages.
  - [ ] Enhance `Table.vacuum()` to merge half-empty pages and return reclaimed pages to the global list.

- **Advanced Features**
  - [ ] Implement data compression for rows.
  - [ ] Implement TOAST (The oversized-Attribute Storage Technique) for handling data larger than a single page.
