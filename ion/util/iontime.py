#!/usr/bin/env python

"""
@file ion/util/iontime.py
@author Dave Foster <dfoster@asascience.com>
@brief Time utility class
"""

import time

class IonTime:
    """
    Time utility class. Used for generating times in commonly used "milliseconds since unix epoch" format
    as used in many GPB messages.

    Can be used to generate/convert string based ISO8601 (yyyy-MM-dd'T'HH:mm:ss.sss'Z').

    @TODO: ISO8601 is hard to parse as there's a lot of optional stuff that python cannot really
    handle without a dedicated module. Needs to be able to handle a string as input.
    """

    def __init__(self, time_in_ms=None):
        """
        Constructs an IonTime instance using an optional time parameter. If specified, that time is used.
        If not, the current time is used.

        @param  time_in_ms  Optional, the number of ms since the UNIX epoch (1970).
        """
        if time_in_ms is not None:
            self._time_ms = time_in_ms
        else:
            self._time_ms = self._now()

    def _now(self):
        """
        Gets the current system time and converts to milliseconds from the unix epoch (1970).
        """
        curtime = int(round(time.time() * 1000))
        return curtime

    def _get_time_ms(self):
        """
        Gets the time in milliseconds since the UNIX epoch.
        This is the internal representation of time in this class and is just a passthrough to the
        private member var.
        """
        return self._time_ms

    time_ms = property(_get_time_ms)

    def _get_time_str(self):
        """
        Gets the time as ISO8601 Date Format (yyyy-MM-dd'T'HH:mm:ss.sss'Z').
        """
        # need to shift off the millis as python does not handle it well
        
        (secs, fracsecs) = divmod(self._time_ms, 1000)

        gmtuple = time.gmtime(secs)

        iso_time = time.strftime("%Y-%m-%d'T'%H:%M:%S", gmtuple)

        # append fractional time
        iso_time += '.' + str(fracsecs) + "'Z'"

        return iso_time

    time_str = property(_get_time_str)
