from src.config import PAGE_SIZE
from sys import getsizeof
from struct import calcsize

"""**
 * DIRECTORY_ARRAY_SIZE is the number of page_ids that can fit in the directory page of an extendible hash index.
 * This is 512 because the directory array must grow in powers of 2, and 1024 page_ids leaves zero room for
 * storage of the other member variables: page_id_, lsn_, global_depth_, and the array local_depths_.
 * Extending the directory implementation to span multiple pages would be a meaningful improvement to the
 * implementation.
*"""

DIRECTORY_ARRAY_SIZE = 512

sizeOfMappingType = getsizeof(
    (None, None)
)  # Example size of std::pair<KeyType, ValueType>, skipped just to avoid the overhead of python obj type


sizeOfMappingType = calcsize("2I")  # # Size of std::pair<KeyType, ValueType> in C++
"""**
 * BUCKET_ARRAY_SIZE is the number of (key, value) pairs that can be stored in an extendible hash index bucket page.
 * The computation is the same as the above BLOCK_ARRAY_SIZE, but blocks and buckets have different implementations
 * of search, insertion, removal, and helper methods.
 *"""
BUCKET_ARRAY_SIZE = 4 * PAGE_SIZE // (4 * sizeOfMappingType + 1)


# Define the size of uint32_t using the struct module
UINT32_SIZE = calcsize("I")

# Define the constant using the size of uint32_t
HTABLE_BUCKET_PAGE_METADATA_SIZE = UINT32_SIZE * 2


HTABLE_HEADER_MAX_DEPTH = 9

HTABLE_DIRECTORY_MAX_DEPTH = 9