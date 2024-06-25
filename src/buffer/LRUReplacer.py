from src.buffer.Replacer import Replacer
from src.config import page_id_t, frame_id_t, size_t
from collections import OrderedDict
from threading import Lock


class LRUReplacer(Replacer):
    """
    * Create a new LRUReplacer.
    * @param num_pages the maximum number of pages the LRUReplacer will be required to store
    """

    def __init__(self, num_pages: size_t) -> None:
        self.num_pages = num_pages
        self.lru = OrderedDict()
        self.lock = Lock()

    def victim(self, frame_id) -> bool:
        with self.lock:
            if not self.lru:
                return False, None
            victim_frame_id, _ = self.lru.popitem(last=False)
            print(_, frame_id)
            frame_id[0] = victim_frame_id
            return True

    def pin(self, frame_id: page_id_t):
        with self.lock:
            if frame_id in self.lru:
                del self.lru[frame_id]

    def unpin(self, frame_id: page_id_t):
        with self.lock:
            if frame_id not in self.lru:
                self.lru[frame_id] = {}  # of type frame
                if len(self.lru) > self.num_pages:
                    self.lru.popitem(last=False)
            else:
                self.lru.move_to_end(frame_id)

    def size(self) -> size_t:
        with self.lock:
            return len(self.lru)
