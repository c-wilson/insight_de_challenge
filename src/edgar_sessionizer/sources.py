"""
Classes for input to the Sessionization class. These are plugins that allow for different data sources to be
used as inputs to the Sessionization class. These are similar to base IO classes in that they return data
when queried.

Classes:
    DataSource - Base class.
    CsvSource - Read and parses CSV file input from EDGAR site.

DataSource subtypes should adhere to the following convention:

* contain facilities for context management (__enter__ and __exit__ functions)
* each should have a__bool__ functions that return True when data is available and False when the data source
is exhausted.
* new data can be pulled using a public get_next() function that returns a
dictionary containing the following fields:
    1. "ip" - IP address of the client
    4. "timestamp" - float representing UNIX epoch time

    Other fields are not currently used.
"""

import csv
import datetime

from csv import DictReader

class DataSource:
    """
    Skeleton for data source for Sessionization algorithm.

    Data structures must implement methods for:
       __bool__: must return true unless the data source is exhausted
       get_next(): returns data in the form of a RequestRecord object.
    """

    def __init__(self):
        pass

    def __enter__(self):
        """ context manager """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ context manager """
        pass

    def __bool__(self):
        """ Should return true when the source might provide another data point.

        As implemented, returning False will end the thread.

        For a file reader, this would be true until the end of the file has been read.
        For a network connection this might go false when the connection is dropped.
        """
        return False

    def get_next(self) -> 'dict':
        """
        Template for function to return the next data record.

        Implementations should return a tuple of (IP address(str), timestamp(float))
        """
        pass


class CsvSource(DataSource):
    """
    Data source for parsing EDGAR CSV files.

    Public methods:
        get_next() - returns RequestRecord for next transaction in file.
    """
    _reader: DictReader
    _required_fields = ('ip', 'date', 'time', 'zone', 'cik', 'accession', 'extention')

    def __init__(self, file_path: str):
        """
        :param file_path: path to the file.
        """
        super(CsvSource, self).__init__()
        self.file_path = file_path
        self._file = open(file_path, 'r')
        self._reader = csv.DictReader(self._file)
        self._check_header()
        self._data_available = True  # True if more data is available. Set to false by _read_next_line if EOF.
        self._next_line = self._read_next_line()

    def __bool__(self):
        """ returns True until the end of the file has been reached. """
        return self._data_available

    def get_next(self, *args, **kwargs) -> dict:
        """
        Parses data from CSV Reader line object and returns a dictionary including the UNIX timestamp of
        the request.

        This decodes CSVs with the following format:
          IP_address(str),Date(YYYY-MM-DD),Time(HH:MM:SS),TZ(float),cik(str),accession(str),extention(str)
          Other fields are not used. Strings can be of arbitrary length, including IP address.

        :returns dictionary of request information
        """

        line = self._next_line  # type: dict

        # Unpack fields for date time parsing:
        d, t, z = line['date'], line['time'], line['zone']

        # Parse time and make record. Fail gracefully if the time cannot be parsed by raising an exception
        # that will be caught by the parent parsing class and logged:
        try:
            timestamp = self._datetime_to_timestamp(d, t, z)
            line['timestamp'] = timestamp
            return line
        except ValueError:  # datetime strptime raises value error.
            err_str = 'Bad time format at line {}.'.format(self._reader.line_num)
            raise ParsingError(err_str)
        finally:
            # Read the next line now, so that we can set _bool to false if no more lines exist.
            self._next_line = self._read_next_line()

    def _read_next_line(self) -> list:
        """
        Reads the next line from the CSV Reader if it exists.
        If the end of the file is reached, set the class's bool to False and return None.

        :return: next line from Csv Reader object or None if EOF.
        """
        try:
            next_line = next(self._reader)
        except StopIteration:
            self._data_available = False
            next_line = None
        return next_line

    def _datetime_to_timestamp(self, date_str: str, time_str: str, zone: str) -> float:
        """
        Parses date and time strings into unix timestamps, which represents seconds since the unix epoch start.

        :param date_str: Date string expressed as "YYYY-MM-DD"
        :param time_str: Time string expressed as "HH:MM:SS"
        :param zone: Time zone (spec is undefined, but 0. is default).
        :return: Float representing UNIX timestamp.
        """

        # TODO: time zone support.
        if float(zone) != 0.:
            raise NotImplementedError('Time zone support is not implemented.')

        y, month, d = [int(x) for x in date_str.split('-')]
        h, minute, s = [int(x) for x in time_str.split(':')]
        dt = datetime.datetime(y, month, d, h, minute, s)
        return dt.timestamp()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ context manager; closes file """
        self._file.close()

    def _check_header(self):
        """
        Checks that all required fields are contained in the CSV header and raises ValueError if not.

        If the header does not contain relevant fields, it also sets the source_active flag to False indicating
        that the source has no additional data to pull.
        """
        fields = self._reader.fieldnames
        for name in self._required_fields:
            if name not in fields:
                self._data_available = False
                raise ValueError('Malformed CSV input. Missing {} field in header'.format(name))


class ParsingError(Exception):
    pass
