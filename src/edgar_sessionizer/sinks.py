"""
Contains classes for outputs of the Sessionization class.

Current implementation is limited to output to a CSV file, but further outputs can be generated for inter-
process message passing.

Classes:
    Sink - base class for handling data.
    CsvSink - logs session information to a csv file.
"""
import datetime
from edgar_sessionizer import sessionization
import csv


class Sink:
    """
    Base class for saving or transmitting information from Sessionization class.

    Implementations can be concieved for writing data to disk in multiple formats, to a network socket, or to a database.
    All subclasses should implement a .write() method that adheres to the spec below.

    __exit__ method should be overwritten.
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ overwrite this to use in context manager """
        pass

    def write(self, session: 'sessionization.Session'):
        """
        Skeleton for write method. Implementation depends on how data will be stored.

        :param session: Session object to be logged
        """
        pass


class CsvSink(Sink):
    """
    Writer for Sessionizer data to disk in the form of a CSV file.

    Writes session data with the following format:
    IP_address,session_start_time,session_end_time,duration(sec),n_requests_in_session

    Times are stored as 'yyyy-mm-dd HH:MM:SS' format.

    Example output line:
    10.192.21.xxx,2017-05-03 00:00:00,2017-05-03 00:00:03,4,4
    """

    _time_spec = '%Y-%m-%d %H:%M:%S'

    def __init__(self, path, mode='w'):
        """
        :param path: Path to create/open file.
        :param mode: Mode passed to open() function. File must be opened in a writable mode ('r+' or 'w'). ('w' creates
          new file, 'r+' appends to existing)
        """
        assert mode in ('r+', 'w'), "File must be opened in a writable mode."
        self._path = path
        self._file = open(path, mode)
        # I'm choosing to buffer here for performance, but there is some lag between the writes and changes to the file.
        self._writer = csv.writer(self._file, lineterminator='\n')  # '\n' matches spec from example output.

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._file.close()

    def write(self, session: 'sessionization.Session'):
        """
        Writes data line to CSV file.

        :param session: Session object to be logged
        """

        # convert times to spec:

        start_time_str = self._timestamp_to_str(session.start)
        end_time_str = self._timestamp_to_str(session.latest)
        to_write = (session.ip, start_time_str, end_time_str, int(session.duration()), int(session.txn_count))
        _ = self._writer.writerow(to_write)

    def _timestamp_to_str(self, timestamp: float):
        dt = datetime.datetime.fromtimestamp(timestamp)
        return dt.strftime(self._time_spec)

