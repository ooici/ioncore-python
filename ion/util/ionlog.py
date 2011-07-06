#!/usr/bin/env python

"""
@file ion/util/ionlog.py
@author Michael Meisinger
@brief Abstracts from any form of logging in ION
"""
import logging
from ion.core import ioninit

class LogFactory(object):
    """
    Factory for producing logger objects with additional handlers.
    A global instance of this factory is declared in this module, and
    is used by the getLogger global used all over ioncore-python.
    """
    def __init__(self):
        """
        Initializer.
        """
        self._handlers = []

    def get_logger(self, loggername):
        """
        Creates an instance of a logger.
        Adds any registered handlers with this factory.

        Note: as this method is called typically on module load, if you haven't
        registered a handler at this time, that instance of a logger will not
        have that handler.
        """
        logger = logging.getLogger(loggername)
        for handler in self._handlers:
            logger.addHandler(handler)

        return logger

    def add_handler(self, handler):
        """
        Adds a handler to be added to the logger requested with get_logger.
        The handler must be derived from logging.Handler.
        """
        self._handlers.append(handler)

    def remove_handler(self, handler):
        """
        Removes a handler.
        """
        self._handlers.remove(handler)

# declare global instance
try:
    log_factory
except NameError:
    log_factory = LogFactory()

def getLogger(loggername=__name__):
    """
    This function is used to assign every module in the code base a separate
    logger instance. Currently it just delegates to Python logging.
    """
    return log_factory.get_logger(loggername)

class ProcessInfo:
    """
    Adds extra parameters to the Python logging loggers, for process identification.
    """

    def __getitem__(self, name):
        from random import choice
        if name == "procid":
            result = "p1"
        elif name == "procname":
            result = "n1"
        else:
            result = self.__dict__.get(name, "?")
        return result

    def __iter__(self):
        """
        To allow iteration over keys, which will be merged into
        the LogRecord dict before formatting and output.
        """
        keys = ["procid", "procname"]
        keys.extend(self.__dict__.keys())
        return keys.__iter__()
