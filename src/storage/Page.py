from src.config import *

# import PAGE_SIZE, page_id_t, INVALID_PAGE_ID, lsn_t, size_t, LSN_FORMAT
from src.latch.ReaderWriterLatch import ReaderWriterLatch
import struct

"""
 * Page is the basic unit of storage within the database system. Page provides a wrapper for actual data pages being
 * held in main memory. Page also contains book-keeping information that is used by the buffer pool manager, e.g.
 * pin count, dirty flag, page id, etc.
"""


class Page:
    """
    There is book-keeping information inside the page that should only be relevant to the buffer pool manager.
    """

    __SIZE_PAGE_HEADER: size_t = 8
    __OFFSET_PAGE_START: size_t = 0
    __OFFSET_LSN: size_t = 4

    def __init__(self) -> None:
        self._data_ = bytearray(PAGE_SIZE)
        self._page_id_: int = INVALID_PAGE_ID
        self._pin_count_: int = 0
        # True if the page is dirty, i.e. it is different from its corresponding page on disk
        self._is_dirty_ = False
        # Page latch.
        self._rwlatch_ = ReaderWriterLatch()
        self.ResetMemory()

    def getData(self):
        """* @return the actual data contained within this page *"""
        return self._data_

    def getPageId(self) -> page_id_t:
        """* @return the page id of this page *"""
        return self._page_id_

    def getPinCount(self):
        """* @return the pin count of this page *"""
        return self._pin_count_

    def WLatch(self):
        """* Acquire the page write latch. *"""
        self._rwlatch_.WLock()

    def WUnLatch(self):
        """* Release the page write latch. *"""
        self._rwlatch_.WUnLock()

    def RLatch(self):
        """* Acquire the page read latch. *"""
        self._rwlatch_.RLock()

    def RUnLatch(self):
        """* Release the page read latch. *"""
        self._rwlatch_.WUnLock()

    def GetLNS(self) -> lsn_t:
        """* @return the page Log Sequence Number LSN. *"""
        return struct.unpack_from(LSN_FORMAT, self._data_, self.__OFFSET_LSN)

    def SetLNS(self, lsn: lsn_t):
        """/** Sets the page LSN. */
        inline void SetLSN(lsn_t lsn) { memcpy(GetData() + OFFSET_LSN, &lsn, sizeof(lsn_t)); }
        """
        struct.pack_into(LSN_FORMAT, self._data_, self.__OFFSET_LSN, lsn)

    def ResetMemory(self):
        """* Zeroes out the data that is held within the page. *"""
        self._data_[self.__OFFSET_PAGE_START :] = bytearray(
            PAGE_SIZE - self.__OFFSET_PAGE_START
        )
