export const sizeof_uint32 = 4;

export const PAGE_SIZE = 4096;
export const HEADER_SIZE = 3 * sizeof_uint32;
export const SLOT_SIZE = 2 * sizeof_uint32;

export const CATALOG = 0;
export const LAST_PAGE_ID = 2 ** 32 - 1;

export const BUFFER_POOL_SIZE = 10;
