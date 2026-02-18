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
  - [x] Implement row insertion logic within a page.
  - [x] Implement row reading logic from a page.

- [x] **Higher-Level Abstractions**
  - [x] Create a `Table` class to manage multiple pages.
  - [x] Implement a Catalog to store metadata about tables.

## To Do

- **Developer Experience**
  - [ ] Make the `Table` class generic to accept a data shape (e.g., `Table<T>`).
  - [ ] Create a base `Entity` class with `serialize` and `deserialize` methods for easier data modeling.

- **Page Structure Enhancements**
  - [ ] Refactor `Page` to a "Slotted Page" structure to handle deletions and space reuse efficiently.

- **Buffer Management**
  - [ ] Create a `BufferPoolManager` to manage a cache of in-memory pages, reducing disk I/O.

- **Query Engine**
  - [ ] Design and implement a simple query language parser (e.g., for `SELECT`, `INSERT`).
  - [ ] Create a query planner and executor.

- **Advanced Features**
  - [ ] Implement data compression for rows.
  - [ ] Implement a Free Space Map (FSM) to optimize finding pages with free space.
  - [ ] Implement TOAST (The Oversized-Attribute Storage Technique) for handling data larger than a single page.
