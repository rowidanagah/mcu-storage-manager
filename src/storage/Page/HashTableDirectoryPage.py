from src.config import page_id_t, INVALID_PAGE_ID, lsn_t
from src.hash_table_page_defs import DIRECTORY_ARRAY_SIZE
from src.storage.Page import Page
from src.buffer.BufferPoolManager import BufferPoolManager

"""**Notes
* The HashTableDirectoryPage doesn't get parameters directly.
Instead, memory for the HashTableDirectoryPage is allocated by the buffer pool manager 

* Each bucket has a local depth that indicates how many bits of the hash value are significant for the bucket.

* local_depths : This array helps in managing the splitting of buckets and updating the directory pointers correctly

"""


class HashTableDirectoryPage:
    """**
    *
    * Directory Page for extendible hash table.
    *
    * Directory format (size in byte):
    * --------------------------------------------------------------------------------------------
    * | LSN (4) | PageId(4) | GlobalDepth(4) | LocalDepths(512) | BucketPageIds(2048) | Free(1524)
    * --------------------------------------------------------------------------------------------
    """

    def __init__(self) -> None:
        super().__init__()
        self._page_id_: page_id_t = INVALID_PAGE_ID
        self._lsn_: lsn_t = INVALID_PAGE_ID
        self._global_depth_ = 1
        self._pin_count_: int = 0
        self._local_depths_ = [0] * DIRECTORY_ARRAY_SIZE
        self._bucket_page_ids_: page_id_t = [0] * DIRECTORY_ARRAY_SIZE

    def GetPageId(self) -> page_id_t:
        """** @return the page ID of this page*"""
        return self._page_id_

    def SetPageId(self, page_id: page_id_t):
        """* @param page_id the page id to which to set the page_id_ field"""
        self._page_id_ = page_id

    def GetLsn(self) -> lsn_t:
        """* @return the lsn of this page"""
        return self._lsn_

    def SetLsn(self, lsn):
        """**
        * Sets the LSN of this page
        * @param lsn the log sequence number to which to set the lsn field
        *"""
        self._lsn_ = lsn

    def GetBucketPageId(self, bucket_idx) -> page_id_t:
        """**
        * Lookup a bucket page using a directory index
        *
        * @param bucket_idx the index in the directory to lookup
        * @return bucket page_id corresponding to bucket_idx
        *"""
        return self._bucket_page_ids_[bucket_idx]

    def FetchBucketPage(self, bucket_idx, bpm):
        bucket_id = self.GetBucketPageId(bucket_idx)
        if bucket_id != INVALID_PAGE_ID:
            return bpm.FetchPage(bucket_id)
        return None

    def GetLocalDepth(self, bucket_idx):
        """**
        * Gets the local depth of the bucket at bucket_idx
        *
        * @param bucket_idx the bucket index to lookup
        * @return the local depth of the bucket at bucket_idx
        *"""
        return self._local_depths_[bucket_idx]

    def SetLocalDepth(self, bucket_idx, local_depth):
        """**
        * Set the local depth of the bucket at bucket_idx to local_depth
        *
        * @param bucket_idx bucket index to update
        * @param local_depth new local depth
        *"""
        self._local_depths_[bucket_idx] = local_depth

    def SetBucketPageId(self, bucket_idx, bucket_page_id: page_id_t):
        """**
        * Updates the directory index using a bucket index and page_id
        *
        * @param bucket_idx directory index at which to insert page_id
        * @param bucket_page_id page_id to insert
        *"""
        print(
            "Inside the set bucket page id \n ",
            "Index found is :",
            bucket_idx,
            " page id :",
            bucket_page_id,
        )
        self._bucket_page_ids_[bucket_idx] = bucket_page_id

    def UnpinBucket(
        self, bpm: BufferPoolManager, bucket_idx: page_id_t, is_dirty: bool = False
    ):
        page_id = self.GetBucketPageId(bucket_idx)
        bpm.UnpinPage(page_id, is_dirty)

    def GetGlobalDepth(self):
        """**
        * Get the global depth of the hash table directory
        *
        * @return the global depth of the directory
        *"""
        return self._global_depth_

    def GetSplitImageIndex(self, bucket_idx):
        """**
        * Gets the split image of an index
        *
        * @param bucket_idx the directory index for which to find the split image
        * @return the directory index of the split image
        *"""
        pass

    def GetGlobalDepthMask(self):
        """**
        * GetGlobalDepthMask - returns a mask of global_depth 1's and the rest 0's.
        *
        * In Extendible Hashing we map a key to a directory index
        * using the following hash + mask function.
        *
        * DirectoryIndex = Hash(key) & GLOBAL_DEPTH_MASK
        *
        * where GLOBAL_DEPTH_MASK is a mask with exactly GLOBAL_DEPTH 1's from LSB
        * upwards.  For example, global depth 3 corresponds to 0x00000007 in a 32-bit
        * representation.
        *
        * @return mask of global_depth 1's and the rest 0's (with 1's from LSB upwards)
        *"""
        return (1 << self._global_depth_) - 1

    def IncrGlobalDepth(self):
        """** Decrement the global depth of the directory. *"""
        self._global_depth_ += 1

    def DecrLocalDepth(self):
        """**
        * Decrement the local depth of the bucket at bucket_idx
        * @param bucket_idx bucket index to decrement
        *"""
        self._local_depths_ -= 1

    def GetNumBuckets(self):
        """* Returns number of Buckets so far"""
        return 1 << self._global_depth_

    def SplitBucket(self):
        """* Split a bucket if there is no room for insertion. We can split the bucket till it become full."""
        pass

    def __str__(self) -> str:
        return f"This Hash Directory page  with page id equals to:  {self._page_id_} "


directory_page = HashTableDirectoryPage()
directory_page.SetPageId(1)
directory_page.SetLsn(100)
directory_page.SetLocalDepth(2, 9)
directory_page.SetLocalDepth(0, 1)
directory_page.SetBucketPageId(0, 10)

print("Page ID:", directory_page.GetPageId())
print("LSN:", directory_page.GetLsn())
print("Global Depth:", directory_page.GetLocalDepth(1))
print("Local Depth of bucket 0:", directory_page.GetLocalDepth(0))
print("Bucket Page ID of bucket 0:", directory_page.GetBucketPageId(0))

print("Number of Buckets we have so far: ", directory_page.GetNumBuckets())
