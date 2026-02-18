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
  - [x] Implement a Catalog to store metadata about tables.

- [x] **Developer Experience**
  - [x] Make the `Table` class generic (`Table<T>`).
  - [x] Create a decorator-based `Entity` system for automatic serialization.

## To Do

- **Buffer Management**
  - [ ] Create a `DoublyLinkedList` for the LRU replacer.
  - [ ] Implement `fetchPage` in `BufferPoolManager` with LRU eviction logic.
  - [ ] Track "dirty" pages (modified pages) and write them to disk before eviction.
  - [ ] Implement page "pinning" to prevent active pages from being evicted.
  - [ ] Refactor `Table`, `Catalog`, etc. to use the `BufferPoolManager` instead of the `DiskManager`.

- **Vacuum / Space Reclamation**
  - [ ] Implement a global free-page list to track and reuse deleted pages.
  - [ ] Enhance `Table.vacuum()` to merge half-empty pages and return reclaimed pages to the global list.

- **Query Engine**
  - [ ] Design and implement a simple query language parser (e.g., for `SELECT`, `INSERT`).
  - [ ] Create a query planner and executor.

- **Advanced Features**
  - [ ] Implement data compression for rows.
  - [ ] Implement a Free Space Map (FSM) to optimize finding pages with free space.
  - [ ] Implement TOAST (The Oversized-Attribute Storage Technique) for handling data larger than a single page.
