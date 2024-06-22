from src.config import page_id_t

"""
 * DiskManager takes care of the allocation and deallocation of pages within a database. It performs the reading and
 * writing of pages to and from disk, providing a logical file layer within the context of a database management system.
"""


class DiskManager:
    """
    * Creates a new disk manager that writes to the specified database file.
    * @param db_file the file name of the database file to write to
    """

    def __init__(self, db_file) -> None:
        pass

    def shutdown():
        """
        * Shut down the disk manager and close all the file resources.
        """
        pass

    def writePage(page_id: page_id_t, page_data):
        """
        * Write a page to the database file.
        * @param page_id id of the page
        * @param page_data raw page data
        """
        pass

    def readPage(page_id: page_id_t, page_data):
        """
        * Read a page from the database file.
        * @param page_id id of the page
        * @param[out] page_data output buffer
        """
        pass

    def writeLog(log_data, size):
        """
        * Flush the entire log buffer into disk.
        * @param log_data raw log data
        * @param size size of log entry
        """
        pass

    def readLog(log_data, size, offset):
        """
        * Read a log entry from the log file.
        * @param[out] log_data output buffer
        * @param size size of the log entry
        * @param offset offset of the log entry in the file
        * @return true if the read was successful, false otherwise
        """
        pass

    def getNumFlashes():
        """@return the number of disk flushes"""
        pass

    def getFlashesState():
        """@return true iff the in-memory content has not been flushed yet"""
        pass

    def getNumWrites():
        """@return the number of disk writes"""
        pass
