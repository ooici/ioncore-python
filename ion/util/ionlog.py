#!/usr/bin/env python

"""
@file ion/util/ionlog.py
@author Michael Meisinger
@brief Abstracts from any form of logging in ION
"""

def getLogger(loggername=__name__):
    import logging
    return logging.getLogger(loggername)

def IonLogger(object):
    import logging
    def __init__(self, loggername):
        self.logger = logging.getLogger(loggername)

    def debug(self, msg, *args):
        pass

    def info(self, msg, *args):
        pass

    def warn(self, msg, *args):
        pass

    def error(self, msg, *args):
        pass

    def critical(self, msg, *args):
        pass

    def exception(self, msg, *args):
        pass

    warning = warn

class LoggerAdapter:
    def __init__(self, logger, args):
        self.logger = logger
        self.args = args

    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, extra=self.extra())

    def warning(self, msg, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)
    warn = warning

    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)

    def exception(self, msg, *args):
        self.logger.exception(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.logger.critical(msg, *args, **kwargs)
    fatal = critical

    def extra(self):
        extra = {}
        for k in self.args:
            extra[k] = self.args[k]
        return extra

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
