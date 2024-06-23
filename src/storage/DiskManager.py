from src.config import page_id_t, size_type, PAGE_SIZE
import threading

"""
 * DiskManager takes care of the allocation and deallocation of pages within a database. It performs the reading and
 * writing of pages to and from disk, providing a logical file layer within the context of a database management system.
"""


class DiskManager:
    def __init__(self, db_file) -> None:
        """
        * Creates a new disk manager that writes to the specified database file.
        * @param db_file the file name of the database file to write to
        """
        self.file_name = db_file
        n: size_type = db_file.rfind(".")
        if n == -1:
            raise ValueError("wrong file format")

        self.log_name_ = self.file_name[:n] + ".log"
        self._db_io_lock = threading.Lock()
        self._log_io_lock = threading.Lock()

        # Open or create the log file
        self._log_io = open(self.log_name_, "a+b")
        self._db_io = open(self.file_name, "a+b")
        self._buffer_used = None
        self._num_writes_ = 1
        self._num_flushes_ = 0
        self._flush_log_ = False

    def shutdown(self):
        """
        * Shut down the disk manager and close all the file resources.
        """
        with self._db_io_lock:
            self._db_io.close()

        with self._log_io_lock:
            self._log_io.close()

    def writePage(self, page_id: page_id_t, page_data: str):
        """
        * Write a page to the database file.
        * @param page_id id of the page
        * @param page_data raw page data
        """
        if len(page_data) != PAGE_SIZE:
            raise ValueError(f"Data must be exactly {PAGE_SIZE} bytes")

        with self._db_io_lock:
            offset: size_type = page_id * PAGE_SIZE
            self._num_writes_ += 1
            self._db_io.seek(offset)
            self._db_io.write(page_data)
            self._db_io.flush()

    def readPage(self, page_id: page_id_t, page_data: str):
        """
        * Read a page from the database file.
        * @param page_id id of the page
        * @param[out] page_data output buffer
        """
        with self._db_io_lock:
            offset: size_type = page_id * PAGE_SIZE
            self._db_io.seek(offset)
            data = self._db_io.read(PAGE_SIZE)
            # Pad with zeros if read is short, if file ends before reading PAGE_SIZE
            if len(data) < PAGE_SIZE:
                data += bytes(PAGE_SIZE - len(data))

            return data

    def writeLog(self, log_data, size):
        """
        * Flush the entire log buffer into disk.
        * @param log_data raw log data
        * @param size size of log entry
        """
        if log_data == self._buffer_used:
            raise AssertionError("log_data should not be the same as buffer_used")
        self.buffer_used = log_data

        # no effect on num_flushes_ if log buffer is empty
        if not size:
            return
        self._num_flushes_ += 1
        self._flush_log_ = True
        with self._log_io_lock:
            self._log_io.write(log_data[:size])
            # Check for I/O error
            if self._log_io.closed:
                print("I/O error while writing log")
                return

            # Flush to keep disk file in sync
            self._log_io.flush()

        self._flush_log_ = False

    def readLog(self, log_data, size, offset):
        """
        * Read a log entry from the log file.
        * @param[out] log_data output buffer
        * @param size size of the log entry
        * @param offset offset of the log entry in the file
        * @return true if the read was successful, false otherwise
        """
        with self._log_io_lock:
            self._log_io.seek(offset)
            log_data = self._log_io.read(size)
            return len(log_data) == size

    def getNumFlashes(self):
        """@return the number of disk flushes"""
        self._num_flushes_

    def getFlashesState(self):
        """@return true iff the in-memory content has not been flushed yet"""
        self._flush_log_

    def getNumWrites(self):
        """@return the number of disk writes"""
        self._num_writes_


# disk_manager = DiskManager("database.db")

# # Writing a page
# page_data = b"a" * PAGE_SIZE
# disk_manager.writePage(0, page_data)

# # Reading a page
# read_data = disk_manager.readPage(1, 0)
# print(read_data == page_data)  # Should print: True

# # Writing to log
# log_data = b"log entry"
# disk_manager.writeLog(log_data, len(log_data))

# # Reading from log
# log_data_buffer = bytearray(len(log_data))
# success = disk_manager.readLog(log_data_buffer, len(log_data), 0)
# print(log_data_buffer == log_data)  # Should print: True
# print(success)  # Should print: True

# disk_manager.shutdown()
