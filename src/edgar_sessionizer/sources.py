"""
Classes for input to the Sessionization class.

Classes:
    DataSource - Base class.
    CsvSource - Read and parses CSV file input from EDGAR site.
    RequestRecord - Data class passed to Sessionization.
"""

import os
from collections import deque
import csv
import datetime
from typing import Tuple


ISO_DATE_FMT = '%Y-%m-%dT%H:%M:%S'


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

    def get_next(self) -> "RequestRecord":
        """
        Template for function to return the next data record.

        Implementations should return a tuple of (IP address(str), timestamp(float))
        """
        return RequestRecord('', -1, '', '', '')


class CsvSource(DataSource):
    """
    Data source for parsing EDGAR CSV files.

    This is a generator object.

    Public methods:
        get_next() - returns RequestRecord for next transaction in file.

    """
    def __init__(self, file_path: str):
        """
        :param file_path: path to the file.
        """
        super(CsvSource, self).__init__()
        self.file_path = file_path
        self._file = open(file_path, 'r')
        self._reader = csv.reader(self._file)
        self._header = next(self._reader)  # not used, but we must iterate through it...
        self._bool = True  # True if more data is available. Set to false by _read_next_line if EOF.
        self._next_line = self._read_next_line()

    def __bool__(self):
        """ returns True until the end of the file has been reached. """
        return self._bool

    def get_next(self, *args, **kwargs) -> "RequestRecord":
        """
        Parses data from CSV Reader line and creates a RequestRecord object.

        This decodes CSVs with the following format:
          IP_address(str),Date(YYYY-MM-DD),Time(HH:MM:SS),TZ(float),cik(str),accession(str),extention(str)
          Other fields are not used. Strings can be of arbitrary length, including IP address.

        :returns RequestRecord object
        """

        line = self._next_line

        # unpack fields and convert time:
        try:
            ip = line[0]  # todo: check that this looks like an IP address.
            d, t, z = line[1], line[2], line[3]  # these will be checked later
            cik, accession, extention = line[4], line[5], line[6]  # these are arbirary strings.
            timestamp = self._datetime_to_timestamp(d, t, z)
            record = RequestRecord(ip, timestamp, cik, accession, extention)
        except IndexError:  # if our line is missing a field.
            err_str = 'Bad record at line {}.'.format(self._reader.line_num)
            raise ParsingError(err_str)
        except ValueError:  # datetime strptime raises value error.
            err_str = 'Bad time format at line {}.'.format(self._reader.line_num)
            raise ParsingError(err_str)
        finally:
            # Read the next line now, so that we can set _bool to false if no more lines exist.
            self._next_line = self._read_next_line()

        return record


    def _read_next_line(self) -> list:
        """
        Reads the next line from the CSV Reader if it exists.
        If the end of the file is reached, set the class's bool to False and return None.

        :return: next line from Csv Reader object or None if EOF.
        """
        try:
            next_line = next(self._reader)
        except StopIteration:
            self._bool = False
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

        dt = datetime.datetime.strptime(date_str + 'T' + time_str, ISO_DATE_FMT)
        return dt.timestamp()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """ context manager; closes file """
        self._file.close()


class RequestRecord:
    """
    Structure for containing information from a HTTP request for EDGAR requests.

    Attributes:
        ip: string representing client IP address.
        timestamp: float representing time in seconds (UNIX timestamp spec)
        cik: string
        accession: string
        extention: string
    """
    __slots__ = ['ip', 'timestamp', 'cik', 'accession', 'extention']  # slots for memory efficiency.

    def __init__(self, ip, timestamp, cik, accession, extention):
        self.ip = ip
        self.timestamp = timestamp
        self.cik = cik
        self.accession = accession
        self.extention = extention


class ParsingError(Exception):
    pass




