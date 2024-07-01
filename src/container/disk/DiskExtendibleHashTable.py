from src.buffer.BufferPoolManager import BufferPoolManager
from src.storage.Page.ExtendibleHTableBucketPage import HTableBucketArraySize
from src.hash_table_page_defs import (
    sizeOfMappingType,
    HTABLE_HEADER_MAX_DEPTH,
    HTABLE_DIRECTORY_MAX_DEPTH,
)
from src.container.disk.hash import HashFunction
from src.latch.ReaderWriterLatch import ReaderWriterLatch
from src.storage.Page.HashTableDirectoryPage import HashTableDirectoryPage
from src.storage.Page.HashTableBucketPage import HashTableBucketPage

"""**
 * Implementation of extendible hash table that is backed by a buffer pool
 * manager. Non-unique keys are supported. Supports insert and delete. The
 * table grows/shrinks dynamically as buckets become full/empty.
 *"""


class DiskExtendibleHashTable:
    def __init__(
        self,
        name: str,
        bpm: BufferPoolManager,
        cmp,
        hash_fn=None,
        header_max_depth=HTABLE_HEADER_MAX_DEPTH,
        directory_max_depth=HTABLE_DIRECTORY_MAX_DEPTH,
    ):
        """**
        * @brief Creates a new DiskExtendibleHashTable.
        *
        * @param name
        * @param bpm buffer pool manager to be used
        * @param cmp comparator for keys
        * @param hash_fn the hash function
        * @param header_max_depth the max depth allowed for the header page
        * @param directory_max_depth the max depth allowed for the directory page
        *"""
        self._name = name
        self._bpm = bpm
        self._cmp = cmp
        self._hash_fn_ = HashFunction()
        self._header_page_id_ = header_max_depth
        self._directory_max_depth_ = directory_max_depth
        self._header_max_depth_ = header_max_depth
        # bucket_max_size the max size allowed for the bucket page array
        self._bucket_max_size_ = HTableBucketArraySize(sizeOfMappingType)
        self._table_latch_ = ReaderWriterLatch()

        initial_directory_page = self._bpm.AllocatePage()
        self._directory = HashTableDirectoryPage()
        self._directory.SetPageId(initial_directory_page)
        bpm.UnpinPage(initial_directory_page)

        # Allocate initial buckets
        for i in range(2 ** self._directory.GetGlobalDepth()):
            page_id = self._bpm.AllocatePage()
            bucket_page = HashTableBucketPage()
            bucket_page._page_id = page_id
            self._directory.SetBucketPageId(i, page_id)
            self._bpm._pages[page_id] = bucket_page

    def _Hash(self):
        """**
        * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
        * for extendible hashing.
        *
        * @param key the key to hash
        * @return the down-casted 32-bit hash
        *"""
        pass

    def getDirectoryIndex(self, key):
        hash_value = self._hash_fn_.get_hash(key)
        mask = self._directory.GetGlobalDepthMask()
        return hash_value & mask

    def Insert(self, key, value, transaction):
        """* TODO(P2): Add implementation
        * Inserts a key-value pair into the hash table.
        *
        * @param key the key to create
        * @param value the value to be associated with the key
        * @param transaction the current transaction
        * @return true if insert succeeded, false otherwise
        *"""
        idx = self.getDirectoryIndex(key)
        bucket_page = self._directory.FetchBucketPage(idx, self._bpm)
        print("founded bucket page is :", bucket_page, "of index :", idx)
        bucket_page.Insert(key, value)


bpm = BufferPoolManager(9, "disk_manager.db")

h = DiskExtendibleHashTable("name", bpm, "bpm")
print("hash res ", h.getDirectoryIndex("dadsdsna"))
print(h._directory._bucket_page_ids_)
h.Insert("key", "val", "trnx")
