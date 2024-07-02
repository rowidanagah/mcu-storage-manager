from src.hash_table_page_defs import BUCKET_ARRAY_SIZE
from typing import List, Callable
from src.config import KeyType, ValueType, INVALID_PAGE_ID
from src.storage.Page import Page

"""
* Store indexed key and and value together within bucket page. Supports
 * non-unique keys.
 *
 * Bucket page format (keys are stored in order):
 *  ----------------------------------------------------------------
 * | KEY(1) + VALUE(1) | KEY(2) + VALUE(2) | ... | KEY(n) + VALUE(n)
 *  ----------------------------------------------------------------
 *
 *  Here '+' means concatenation.
 *  The above format omits the space required for the occupied_ and
 *  readable_ arrays.
 
 * You use two bit arrays: occupied_ and readable_. Each bit in these arrays corresponds to a slot in
  the bucket page.
  
*
 """


class HashTableBucketPage:
    def __init__(self) -> None:
        self._occupied_ = [0] * ((BUCKET_ARRAY_SIZE - 1) // 8 + 1)
        self._readable_ = [0] * ((BUCKET_ARRAY_SIZE - 1) // 8 + 1)
        # Initialize array for key-value pairs
        self._array_: List[dict] = [{} for _ in range(BUCKET_ARRAY_SIZE)]
        self._page_id_ = INVALID_PAGE_ID
        self._pin_count_: int = 0

    def GetValue(
        self,
        key: KeyType,
        cmp: Callable[[dict, KeyType], bool],
        result: List[ValueType],
    ) -> bool:
        """**
        * Scan the bucket and collect values that have the matching key
        * @return true if at least one key matched
        *"""
        for idx in range(BUCKET_ARRAY_SIZE):
            if self.IsReadable(idx) and cmp(key, self._array_[idx]):
                result.append(self._array_[idx])
                return True
        return False

    def Insert(self, key, value) -> bool:
        """**
        * Attempts to insert a key and value in the bucket.  Uses the occupied_
        * and readable_ arrays to keep track of each slot's availability.
        *
        * @param key key to insert
        * @param value value to insert
        * @return true if inserted, false if duplicate KV pair or bucket is full
        *
        """
        for idx in range(BUCKET_ARRAY_SIZE):
            if not self.IsOccupied(idx):
                self._array_[idx] = {key: value}
                self.SetOccupied(idx)
                self.SetReadable(idx)
                return True
            else:
                if self.GetValue(key, key_comparator, []):
                    return False  # Duplicate key-value pair
        return False  # Bucket is full

    def Remove(
        self, key: KeyError, value: ValueType, cmp: Callable[[dict, KeyType], bool]
    ) -> bool:
        """**
        * Removes a key and value.
        * @return true if removed, false if not found
        """
        for idx in range(BUCKET_ARRAY_SIZE):
            if (
                self.IsOccupied(idx)
                and cmp(self._array_[idx])
                and self._array_[idx].get(key) == value
            ):
                self._array_[idx] = {}
                return True

        return False

    def keyAt(self, bucket_idx) -> KeyType:
        """**
        * Gets the key at an index in the bucket.
        *
        * @param bucket_idx the index in the bucket to get the key at
        * @return key at index bucket_idx of the bucket
        *"""
        if bucket_idx < BUCKET_ARRAY_SIZE and self.IsReadable(bucket_idx):
            return [*self._array_[bucket_idx].keys()][0]
        return None

    def valueAt(self, bucket_idx) -> ValueType:
        """**
        * Gets the value at an index in the bucket.
        *
        * @param bucket_idx the index in the bucket to get the value at
        * @return value at index bucket_idx of the bucket
        *"""
        if bucket_idx < BUCKET_ARRAY_SIZE and self.IsReadable(bucket_idx):
            return [*self._array_[bucket_idx].values()][0]
        return None

    def removeAt(self, bucket_idx):
        """* Remove the KV pair at bucket_idx"""
        if bucket_idx < BUCKET_ARRAY_SIZE and self.IsOccupied(bucket_idx):
            self._array_[bucket_idx] = {}
        return None

    def IsReadable(self, bucket_idx):
        """**
        * Returns whether or not an index is readable (valid key/value pair)
        *
        * @param bucket_idx index to lookup
        * @return true if the index is readable, false otherwise
        *"""
        byte_idx = bucket_idx // 8
        bit_idx = bucket_idx % 8
        return (self._readable_[byte_idx] & (1 << bit_idx)) != 0

    def SetReadable(self, bucket_idx):
        """**
        * SetReadable - Updates the bitmap to indicate that the entry at
        * bucket_idx is readable.
        *
        * @param bucket_idx the index to update
        *"""
        byte_idx = bucket_idx // 8
        bit_idx = bucket_idx % 8
        # to set the bit at bit_idx in the byte at byte_idx to 1
        self._readable_[byte_idx] |= 1 << bit_idx

    def IsOccupied(self, bucket_idx):
        """**
        * Returns whether or not an index is occupied (key/value pair or tombstone)
        *
        * @param bucket_idx index to look at
        * @return true if the index is occupied, false otherwise
        *"""
        byte_idx = bucket_idx // 8
        bit_idx = bucket_idx % 8
        return (self._occupied_[byte_idx] & (1 << bit_idx)) != 0

    def SetOccupied(self, bucket_idx):
        """**
        * SetOccupied - Updates the bitmap to indicate that the entry at
        * bucket_idx is occupied.
        *
        * @param bucket_idx the index to update
        *"""
        byte_idx = bucket_idx // 8
        bit_idx = bucket_idx % 8
        # to set the bit at bit_idx in the byte at byte_idx to 1
        self._occupied_[byte_idx] |= 1 << bit_idx

    def __str__(self) -> str:
        return f"This Hash Bucket page with page id equals to:  {self._page_id_} "


# bucket = HashTableBucketPage()


# def key_comparator(key, dic):
#     return key in dic


# # Insert some key-value pairs
# bucket.Insert("key", "val")

# bucket.Insert("key2", "val")

# bucket.SetReadable(2)
# bucket.Insert("key2", "val")
# bucket.Insert("key3", "val")
# print(bucket._array_)
# print("=================check get val=============================================")

# # Get values for a specific key
# key_to_search_no_found, key_to_search = "key3", "key65"
# res = []
# values1 = bucket.GetValue(key_to_search_no_found, key_comparator, res)
# print("==============================================================")
# values = bucket.GetValue(key_to_search, key_comparator, res)
# print(
#     f"Values for key '{key_to_search} and {key_to_search_no_found}': {values} {values1}",
#     res,
# )
# print(bucket.keyAt(2), bucket._array_)
