"""
Class that tracks the start and teardown of user sessions.

Classes:
    Sessionizer - main logic class
    Session - supporting data structure maintaining information about a single user class.
"""

from collections import deque, defaultdict
from itertools import count
from . import sources
from . import sinks
import logging


class Sessionizer:
    """
    Main logic class that requests new data from DataSource objects, tracks open user sessions, and reports
    closed session information to a Sink class for logging or transmission to another process.

    Gets data by calling DataSource.get_next(), which should return a RequestRecord data object (found in
    sources module)
    Writes closed session data by calling Sink.write(), which should accept a Session data object (found in
    this module)

    Session information is sent to Sink immediately on expiration of the session.

    When DataSource is exhausted, all open sessions are closed *in the order they were opened*
    """

    def __init__(self, source: 'sources.DataSource', sink: '.sinks.Sink', session_dur: float):
        """
        :param source: DataSource object.
        :param sink: Sink object.
        :param session_dur: Time in seconds when a session times out from no activity.
        """

        self.source = source
        self.sink = sink
        self._sessions = defaultdict(Session)  # session information keyed by ip address
        self._ips_by_expiration = defaultdict(deque)  # queue containing ip addresses keyed by timeout time.
        self._expiration_times = deque()  # queue maintaining the timeout times that have occured IN ORDER.
        self._session_timeout_dur = session_dur
        self._current_time = -1.

    def run(self):
        """
        Retrieves transactions from source, updates session information, and initiates session expiration handling if
        time has changed since last transaction.

        Continues to run until bool(source) returns false, which indicates that the data source is exhausted.
        """

        while self.source:
            # Get next transaction record, try again if parsing fails:
            try:
                record = self.source.get_next()
            except sources.ParsingError as e:
                print(str(e))
                logging.error(str(e))
                continue

            timestamp = record['timestamp']
            expiration_time = timestamp + self._session_timeout_dur + 1
            ip = record['ip']

            # If time has advanced, service expirations occuring at or before the new time:
            if timestamp > self._current_time:
                self._current_time = timestamp
                self._process_timeouts(timestamp)
                self._expiration_times.append(expiration_time)

            # Update the session hashmap with the transaction.
            self._sessions[ip].add_txn(ip, timestamp)
            # todo: can add more tracking information (ie accession, url)

            # Add the session's IP to the expiration tracker. Expiration time is the timestamp when we will need to
            # service this session again to check if it has timed out.
            self._ips_by_expiration[expiration_time].append(ip)  # using as queue (FIFO). Add right, pop left.

        # If source returns False there are no more transactions, cleanup open sessions now the specification is to log
        # sessions in the order that they were started, so we cannot reuse logic from normal timeouts.
        self._cleanup()

    def _process_timeouts(self, time: float):
        """
        Service all session expirations that occur at or prior to the specified time. Retrieves IP addresses from the
        expiration times dictionary and uses this to check the session instance in

        :param time: process all expirations that occur prior to this time.
        """

        # Process all expiration times that 1) haven't been processed and 2) are less than or equal to the
        # current time, specified by the time parameter.
        while self._expiration_times and self._expiration_times[0] <= time:
            t = self._expiration_times.popleft()
            ips = self._ips_by_expiration.pop(t)  # type: deque
            latest_txn_time = t - self._session_timeout_dur - 1

            # if the session's last transaction occured before latest_txn_time, it is expired.
            while ips:
                ip = ips.popleft()
                if ip in self._sessions:
                    # must explicitly check because if the session is deleted, defaultdict will create on get
                    session = self._sessions[ip]
                    if session.latest <= latest_txn_time:
                        self.sink.write(session)
                        del self._sessions[ip]  # Effectively ends session by removing from data structure.

    def _cleanup(self):
        """ Closes and sends to sink any open sessions in the order they were opened. """

        sess_list = list(self._sessions.values())
        sess_list.sort()
        for session in sess_list:
            self.sink.write(session)


class Session:
    """
    Container for session information.
    """
    __slots__ = ['start', 'txn_count', 'latest', 'ip', '_id']
    _ids = count()  # class var to track the number of sessions that have been instantiated

    def __init__(self):
        self.start = None
        self.txn_count = 0
        self.latest = 0
        self.ip = None
        self._id = next(self._ids)  # only used to tear down the session.

    def add_txn(self, ip: str, timestamp: float):
        assert isinstance(timestamp, float)
        if not self.ip:
            self.ip = ip
        if self.start is None:
            self.start = timestamp
        self.latest = timestamp
        self.txn_count += 1

    def duration(self) -> float:
        """ Returns the (inclusive) duration of the session in seconds."""
        return self.latest - self.start + 1.  # time is inclusive of the latest second.

    def __lt__(self, other: 'Session'):
        """ for sorting based on session open time. """
        return self._id < other._id

    def __str__(self):
        return self.ip
