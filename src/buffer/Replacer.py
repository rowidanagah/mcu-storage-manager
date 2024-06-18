from abc import ABC, abstractmethod


class Replacer(ABC):
    """Replacer is an abstract class that tracks page usage."""

    @abstractmethod
    def Victim(frame_id):
        """
        * Remove the victim frame as defined by the replacement policy.
        * @param[out] frame_id id of frame that was removed, nullptr if no victim was found
        * @return true if a victim frame was found, false otherwise
        """
        pass

    @abstractmethod
    def pin(frame_id):
        """
        * Pins a frame, indicating that it should not be victimized until it is unpinned.
        * @param frame_id the id of the frame to pin
        """
        pass

    @abstractmethod
    def unpin(frame_id):
        """
        * Unpins a frame, indicating that it can now be victimized.
        * @param frame_id the id of the frame to unpin
        """
        pass

    @abstractmethod
    def size():
        """@return the number of elements in the replacer that can be victimized"""
        pass
