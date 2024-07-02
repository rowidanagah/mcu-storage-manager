from src.config import size_t, page_id_t, INVALID_PAGE_ID
from src.storage.DiskManager import DiskManager
from src.recovery.LogManager import LogManager
from src.storage.Page.Page import Page
from src.storage.BasicPageGuard import BasicPageGuard
from src.storage.WriteBackCache import WriteBackCache
from src.buffer.LRUReplacer import LRUReplacer
from threading import Lock


class BufferPoolManager:
    """"""

    def __init__(
        self,
        pool_size: size_t,
        disk_manager: DiskManager,
        log_manager: LogManager = None,
        replacer: size_t = None,
    ):
        """
        * Creates a new BufferPoolManager.
        * @param pool_size the size of the buffer pool
        * @param disk_manager the disk manager
        * @param log_manager the log manager (for testing only: null = disable logging).
        * @param replacer the LRU replacer
        """
        self.disk_manager = DiskManager(disk_manager)
        # Number of pages in the buffer pool
        self._pool_size = pool_size
        self._log_manager = log_manager
        # Array of buffer pool pages.
        self._pages = [Page() for i in range(pool_size)]
        self._replacer = LRUReplacer(pool_size)
        # Initialize the free list with all frames that are currently not being used to store any page.
        self._free_list = list(range(pool_size))
        self._latch_ = Lock()
        # The next page id to be allocated
        self._next_page_id_ = 0
        # Page table for keeping track of buffer pool pages.
        self._page_table = {}
        # This buffer is to optimize the write requests.
        self._write_back_cache_ = WriteBackCache()

    def GetPoolSize(self) -> size_t:
        """*  Return the size (number of frames) of the buffer pool. *"""
        return self.pool_size

    def GetPages(self):
        """*  Return the pointer to all the pages in the buffer pool. *"""
        return self.pages

    def NewPage(self, page_id: [page_id_t]) -> Page:
        """**
        * TODO(P1): Add implementation
        *
        *  Create a new page in the buffer pool. Set page_id to the new page's id, or null if all frames
        * are currently in use and not evictable (in another word, pinned).
        *
        * You should pick the replacement frame from either the free list or the replacer (always find from the free list
        * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
        * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
        *
        * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
        * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
        * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
        *
        * @param[out] page_id id of created page
        * @return null if no new pages could be created, otherwise pointer to new page
        **"""
        with self._latch_:
            print("check if there is a free list or evictable frame")
            if not self._free_list and not self._replacer.size():
                print("no free list nor evictable frame")
                return None

            allocated_page_id, allocated_frame_id = self.AllocatePage()
            page = Page()
            page_id.append(allocated_page_id)
            page._page_id_, page._pin_count_ = allocated_page_id, 1
            self._replacer.pin(allocated_frame_id)
            self._pages[allocated_frame_id]._page_id_ = allocated_page_id
            print("created page", page)
            return self._pages[allocated_frame_id]

    def NewPageGuarded(self, page_id: page_id_t) -> BasicPageGuard:
        """**
        * TODO(P1): Add implementation
        *
        *  PageGuard wrapper for NewPage
        *
        * Functionality should be the same as NewPage, except that
        * instead of returning a pointer to a page, you return a
        * BasicPageGuard structure.
        *
        * @param[out] page_id, the id of the new page
        * @return BasicPageGuard holding a new page
        *"""
        pass

    def FetchPage(self, page_id: page_id_t) -> Page:
        """**
        * TODO(P1): Add implementation
        * Fetch the requested page from the buffer pool.
        * Return null if page_id needs to be fetched from the disk
        * but all frames are currently in use and not evictable (in another word, pinned).
        * you should return NULL if no page is available in the free list and all other pages are currently pinned
        * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
        * the replacer (always find from the free list first), read the page from disk by scheduling a read DiskRequest with
        * disk_scheduler_->Schedule(), and replace the old page in the frame. Similar to NewPage(), if the old page is dirty,
        * you need to write it back to disk and update the metadata of the new page
        *
        * In addition, remember to disable eviction and record the access history of the frame like you did for NewPage().
        *
        * @param page_id id of page to be fetched
        * @return null if page_id cannot be fetched, otherwise pointer to the requested page
        *"""
        with self._latch_:
            if page_id == INVALID_PAGE_ID:
                return None
            if page_id in self._page_table:
                frame_id = self._page_table[page_id]
                self._replacer.pin(frame_id)
                page = self._pages[frame_id]
                page._pin_count_ += 1
                return page

            # If no free frame and no evictable frame
            if not self._free_list and not self._replacer.size():
                return None

            # Get a free frame from the free list or the replacer
            if self._free_list:
                frame_id = self._free_list.pop(0)
            else:
                victim_frame_id = [None]
                if not self._replacer.victim(victim_frame_id):
                    return None
                frame_id = victim_frame_id[0]
                page = self._pages[frame_id]
                if page._is_dirty_:
                    self.disk_manager.writePage(page._page_id_, page.getData())

                del self._page_table[page._page_id_]

            print(frame_id, "frame is ")
            page = self._pages[frame_id]
            self._page_table[page_id] = frame_id
            print("page id is ==========:", page_id)
            self.disk_manager.readPage(page_id, "")
            page._pin_count_, page._is_dirty_, page._page_id_ = 1, False, page_id
            self._replacer.pin(page._page_id_)
            print("ppppppppppppppppppppppppppppppp", page)
            return page

    def UnpinPage(self, page_id: page_id_t, is_dirty=None) -> bool:
        """**
        * TODO(P1): Add implementation
        *
        * Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
        * 0, return false.
        *
        * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
        * Also, set the dirty flag on the page to indicate if the page was modified.
        *
        * @param page_id id of page to be unpinned
        * @param is_dirty true if the page should be marked as dirty, false otherwise
        * @param access_type type of access to the page, only needed for leaderboard tests.
        * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
        *
        """
        with self._latch_:
            if page_id in self._page_table:
                frame_id = self._page_table[page_id]
                page: Page = self._pages[frame_id]
                if page._pin_count_ <= 0:
                    return False
                self._replacer.unpin(frame_id)
                page._pin_count_ -= 1
                page._is_dirty_ = is_dirty or page._is_dirty_
                if page._pin_count_ == 0:
                    self._replacer.unpin(frame_id)
                return True
            else:
                return False

    def FlushPage(self, page_id: page_id_t) -> bool:
        """**
        * TODO(P1): Add implementation
        *
        * Flush the target page to disk.
        * Writes the target page to disk if it is dirty
        * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
        * Unset the dirty flag of the page after flushing.
        *
        * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
        * @return false if the page could not be found in the page table, true otherwise
        *
        """
        with self._latch_:
            if page_id not in self._page_table or page_id == INVALID_PAGE_ID:
                return False
            frame_id = self._page_table[page_id]
            page = self._pages[frame_id]
            self.disk_manager.writePage(page_id, page.getData())
            page._is_dirty_ = False
            return True

    def FlushAllPages(self):
        """**
        *
        *  Flush all the pages in the buffer pool to disk regardless of their pin status.
        *
        """
        for page in self._pages:
            print(page._page_id_, "flush all")
            self.FetchPage(page._page_id_)

    def DeletePage(self, page_id: page_id_t) -> bool:
        """**
        * TODO(P1): Add implementation
        *
        * Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
        * page is pinned and cannot be deleted, return false immediately.
        *
        * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
        * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
        * imitate freeing the page on the disk.
        *
        * @param page_id id of page to be deleted
        * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
        *"""
        if page_id not in self._page_table:
            return True

        frame_id = self._page_table[page_id]
        page = self._pages[frame_id]
        print("== in delete ", page._pin_count_)

        if page._pin_count_ > 0:
            return False
        print("== in delete ")
        del self._page_table[page_id]
        self._replacer.pin(page_id)
        self._free_list.append(frame_id)
        page.ResetMemory()
        return True

    def _AllocateFrame(self):
        if self._free_list:
            print("1- Checking the free list")
            frame_id = self._free_list.pop(0)
            print("1.1 - Founded frame list from the free list", frame_id)
        else:
            print("2- Checking evictable place")
            victim_frame_id = [None]
            if not self._replacer.victim(victim_frame_id):
                return None
            frame_id = victim_frame_id[0]
            print("2.2- Founded frame id:  ", frame_id)
            page = self._pages[frame_id]
            print("2.3- Founded page : ", page)
            if page._is_dirty_:
                self.disk_manager.writePage(page._page_id_, page.getData())

            del self._page_table[page._page_id_]
        return frame_id

    def AllocatePage(self) -> page_id_t:
        """**
        * Allocate a page on disk. Caller should acquire the latch before calling this function.
        * @return the id of the allocated page and the frame id
        *"""
        allocated_frame_id = self._AllocateFrame()
        allocated_page_id = self._next_page_id_
        self._next_page_id_ += 1
        print("allocated page id is : ", allocated_page_id)
        self._page_table[allocated_page_id] = allocated_frame_id
        return allocated_page_id, allocated_frame_id

    def DeallocatePage(self):
        """This is a no-nop right now without a more complex data structure to track deallocated pages"""
        with self._latch_:
            self._next_page_id_ -= 1


# db_name = "test.db"
# buffer_pool_size = 10

# # disk_manager = DiskManager(db_name)
# bpm = BufferPoolManager(buffer_pool_size, db_name)
# print(bpm._page_table)

# # bpm.NewPage(2)
# # bpm.NewPage(3)
# print(bpm._page_table)


def test_buffer_pool_manager():
    buffer_pool, res = BufferPoolManager(3, db_name), []

    # Create a new page
    new_page = buffer_pool.NewPage(res)
    print(res, " ============== res of new page is")
    assert (
        new_page is not None
    ), "Failed to create a new page when there should be free frames."
    print(f"Created new page with id {new_page._page_id_}.")

    # Fetch the created page
    fetched_page = buffer_pool.FetchPage(new_page._page_id_)
    assert fetched_page is not None, "Failed to fetch the created page."
    assert (
        fetched_page._page_id_ == new_page._page_id_
    ), "Fetched page id does not match the created page id."
    print(f"Fetched page with id {fetched_page._page_id_}.")

    # Unpin the fetched page
    assert (
        buffer_pool.UnpinPage(new_page._page_id_, True) is True
    ), "Failed to unpin the page."
    print(f"Unpinned page with id {new_page._page_id_}.")
    print("===========End Of UnPin================================================")
    # Create another page
    another_new_page = buffer_pool.NewPage([])
    assert (
        another_new_page is not None
    ), "Failed to create another new page when there should be free frames."
    print(f"Created another new page with id {another_new_page._page_id_}.")

    # Fetch the new page
    fetched_page2 = buffer_pool.FetchPage(another_new_page._page_id_)
    assert fetched_page2 is not None, "Failed to fetch the newly created page."
    assert (
        fetched_page2._page_id_ == another_new_page._page_id_
    ), "Fetched page id does not match the newly created page id."
    print(f"Fetched another new page with id {fetched_page2._page_id_}.")

    # Flush the page to disk
    assert (
        buffer_pool.FlushPage(another_new_page._page_id_) is True
    ), "Failed to flush the page to disk."
    print(f"Flushed page with id {another_new_page._page_id_} to disk.")

    # Delete the page
    assert (
        buffer_pool.DeletePage(another_new_page._page_id_) is False
    ), "Failed to delete the page."
    print(f"Deleted page with id {another_new_page._page_id_}.")

    # Flush all pages
    buffer_pool.FlushAllPages()
    print("Flushed all pages to disk.")


# Run the test
# test_buffer_pool_manager()
