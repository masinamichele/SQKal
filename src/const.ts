export const sizeof_uint8 = 1;
export const sizeof_uint16 = 2;
export const sizeof_uint32 = 4;

export const PAGE_SIZE = 4096;

/**
 * ----------------------------------------------------------------
 * PAGE HEADER (12 bytes)
 * ----------------------------------------------------------------
 * | rowCount (4) | freeSpacePointer (4) | nextPageId (4) | totalFreeSpace (2) | reserved (2)
 * ----------------------------------------------------------------
 */
export const PAGE_HEADER_SIZE = 4 * sizeof_uint32;

/**
 * -------------------------------------------------
 * SLOT (8 bytes)
 * -------------------------------------------------
 * | rowOffset (4) | rowLength (4) |
 * -------------------------------------------------
 */
export const PAGE_SLOT_SIZE = 2 * sizeof_uint32;

export const CATALOG = 0;
export const FSM = 1;
export const PAGE_DIRECTORY = 2;

export const LAST_PAGE_ID = 2 ** 32 - 1;

export const BUFFER_POOL_SIZE = 10;

/**
 * -------------------------------------------------------------------------------------
 * CATALOG ROW (variable size)
 * -------------------------------------------------------------------------------------
 * | tableNameLength (4) | tableName (var) | firstPageId (4) | columnCount (1) | columnsData (var) |
 * -------------------------------------------------------------------------------------
 */

/**
 * -----------------------------------------------------------
 * COLUMN DEFINITION (variable size)
 * -----------------------------------------------------------
 * | colNameLength (1) | colName (var) | colType (1) | | nullableFlag (1) |
 * -----------------------------------------------------------
 */

/**
 * ----------------------------------------------------------------------------------
 * USER DATA ROW (example for {id: number, name: string})
 * ----------------------------------------------------------------------------------
 * | id_present (1) | id (4) | name_present (1) | nameLength (4) | name (var) |
 * ----------------------------------------------------------------------------------
 */
