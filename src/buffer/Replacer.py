from abc import ABC, abstractmethod
from src.config import frame_id_t, size_t


class Replacer(ABC):
    """Replacer is an abstract class that tracks page usage."""

    @abstractmethod
    def victim(frame_id) -> bool:
        """
        * Remove the victim frame as defined by the replacement policy.
        * @param[out] frame_id id of frame that was removed, null if no victim was found
        * @return true if a victim frame was found, false otherwise
        """
        pass

    @abstractmethod
    def pin(frame_id: frame_id_t):
        """
        * Pins a frame, indicating that it should not be victimized until it is unpinned.
        * @param frame_id the id of the frame to pin
        """
        pass

    @abstractmethod
    def unpin(frame_id: frame_id_t):
        """
        * Unpins a frame, indicating that it can now be victimized.
        * @param frame_id the id of the frame to unpin
        """
        pass

    @abstractmethod
    def size() -> size_t:
        """@return the number of elements in the replacer that can be victimized"""
        pass
