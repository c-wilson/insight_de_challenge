"""
Module containing data sources for the sessionization algorithm.

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
       get_next: returns data in the form of a tuple (ip_address, timestamp)

    Datasources

    """
    def __init__(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
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
        self._continue = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._file.close()

    def __bool__(self):
        return self._continue

    def get_next(self, *args, **kwargs) -> "RequestRecord":
        """ Template for function to return the next batch of data records for processing. Number of records
        desired is specifed by the parameter 'n'. If less than n records are available, the function should
        return the number remaining in the file. If no records are available, return an empty deque.

        This decodes CSVs with the following format:
          IP_address, Date(YYYY-MM-DD), Time(HH:MM:SS), TZ(float) ...
          Other fields are not used.

        :returns RequestRecord object
        """
        try:
            line = next(self._reader)
        except StopIteration:
            self._continue = False
            return None
        # unpack:
        ip = line[0]
        d, t, z = line[1], line[2], line[3]
        timestamp = self._datetime_to_timestamp(d, t, z)
        cik, accession, extention = line[4], line [5], line[6]

        return RequestRecord(ip, timestamp, cik, accession, extention)

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


class RequestRecord:
    """
    Structure for containing information from a HTTP request for EDGAR requests.
    """
    __slots__ = ['ip', 'timestamp', 'cik', 'accession', 'extention']  # slots for memory efficiency.

    def __init__(self, ip, timestamp, cik, accession, extention):
        self.ip = ip
        self.timestamp = timestamp
        self.cik = cik
        self.accession = accession
        self.extention = extention









