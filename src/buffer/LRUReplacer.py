from src.buffer.Replacer import Replacer
from src.config import page_id_t, frame_id_t, size_t
from collections import OrderedDict


class LRUReplacer(Replacer):
    """
    * Create a new LRUReplacer.
    * @param num_pages the maximum number of pages the LRUReplacer will be required to store
    """

    def __init__(self, num_pages: size_t) -> None:
        self.num_pages = num_pages
        self.lru = OrderedDict()

    def victim(self, frame_id) -> bool:
        if not self.lru:
            return False
        frame_id, _ = self.lru.popitem(last=False)
        return True

    def pin(self, frame_id: page_id_t):
        if frame_id in self.lru:
            del self.lru[frame_id]

    def unpin(self, frame_id: page_id_t):
        if frame_id not in self.lru:
            self.lru[frame_id] = {}  # of type frame
            if len(self.lru) > self.num_pages:
                self.lru.popitem(last=False)
        return

    def size(self) -> size_t:
        return len(self.lru)
