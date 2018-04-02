"""
Contains classes for outputs of the Sessionization class.

Current implementation is limited to output to a CSV file, but further outputs can be generated for inter-
process message passing.



"""
import os
import datetime


class Sink:
    """
    Base class for saving or transmitting information from Sessionization class.
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def write(self, ip_address: str, session_start: float, session_end: float, duration, n_requests):
        pass


class CsvSink(Sink):
    """

    """
    _time_spec = '%Y-%m-%d %H:%M:%S'
    def __init__(self, path, mode='w'):
        self._path = path
        self._file = open(path, mode)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._file.close()

    def write(self, ip_address: str, session_start: float, session_end:float, duration, n_requests):
        """

        :param ip_address:
        :param session_start:
        :param session_end:
        :param duration:
        :param n_requests:
        :return:
        """
        start_time_str = self._timestamp_to_str(session_start)
        end_time_str = self._timestamp_to_str(session_end)
        save_str = '{},{},{},{:d},{:d}{}'.format(
            ip_address, start_time_str, end_time_str, int(duration), int(n_requests), os.linesep
        )
        self._file.write(save_str)

    def _timestamp_to_str(self, timestamp: float):
        dt = datetime.datetime.fromtimestamp(timestamp)
        return dt.strftime(self._time_spec)

