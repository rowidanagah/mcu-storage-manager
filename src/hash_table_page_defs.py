from src.config import PAGE_SIZE
from sys import getsizeof


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
)  # Example size of std::pair<KeyType, ValueType>

"""**
 * BUCKET_ARRAY_SIZE is the number of (key, value) pairs that can be stored in an extendible hash index bucket page.
 * The computation is the same as the above BLOCK_ARRAY_SIZE, but blocks and buckets have different implementations
 * of search, insertion, removal, and helper methods.
 *"""
BUCKET_ARRAY_SIZE = 5 #4 * PAGE_SIZE // (4 * sizeOfMappingType + 1)
