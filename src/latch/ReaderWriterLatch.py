import threading


class ReaderWriterLatch:
    """Reader-Writer latch using shared mutex"""

    def __init__(self):
        self._readers = 0
        self._writer = False
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

    def WLock(self):
        """* Acquire a write latch"""
        with self._condition:
            while self._writer or self._readers:
                self._condition.wait()
            self._writer = True

    def WUnLock(self):
        """Release a write latch"""
        with self._condition:
            self._writer = False
            self._condition.notify_all()

    def RLock(self):
        """* Acquire a read latch"""
        with self._condition:
            while self._writer:
                self._condition.wait()
            self._readers += 1

    def RUnLock(self):
        """* Release a read latch."""
        with self._condition:
            self._readers -= 1
            if self._readers == 0:
                self._condition.notify_all()
