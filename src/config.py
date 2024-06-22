# Static constants

# invalid page id
INVALID_PAGE_ID = -1

# invalid transaction id
INVALID_TXN_ID = -1

# invalid log sequence number
INVALID_LSN = -1

# Format for a 4-byte integer (int32_t)
LSN_FORMAT = "i"

# the header page id
HEADER_PAGE_ID = 0

# size of a data page in byte
PAGE_SIZE = 4096

# size of buffer pool
BUFFER_POOL_SIZE = 10


# Type aliases
frame_id_t = int
page_id_t = int
size_t = int
txn_id_t = int
lsn_t = int
slot_offset_t = int
oid_t = int
