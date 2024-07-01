from src.config import PAGE_SIZE
from src.hash_table_page_defs import HTABLE_BUCKET_PAGE_METADATA_SIZE

"""**
 * Bucket page for extendible hash table.
*"""


class ExtendibleHTableBucketPage:
    pass


def HTableBucketArraySize(mapping_type_size):
    return (PAGE_SIZE - HTABLE_BUCKET_PAGE_METADATA_SIZE) / mapping_type_size
