#!/usr/bin/env python

"""
@file ion/services/coi/test/test_logger.py
@author Brian Fox
@brief test logger service
"""

import logging
log = logging.getLogger(__name__)
from twisted.internet import defer

from ion.services.coi.logger import LoggerClient
from ion.test.iontest import IonTestCase


class LoggerServiceTest(IonTestCase):
    """
    Tests Logger service
    """
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_log(self):
        services = [
            {
                'name':'logger_test',
                'module':'ion.services.coi.logger',
                'class':'LoggerService'
            },
        ]
        sup = yield self._spawn_processes(services)

        results = []
        logc = LoggerClient(proc=sup)
        run = yield logc.logmsg('info', 'INFO log message 1', 'X', 'Y')
        results.append(run)
        log.info('INFO logging: '+str(run))

        run = yield logc.logmsg('info', 'DEBUG log message 2', 'X', 'Y')
        results.append(run)
        log.info('DEBUG logging: '+str(run))

        run = yield logc.logmsg('info', 'WARN log message 3', 'X', 'Y')
        results.append(run)
        log.info('WARN logging: '+str(run))

        run = yield logc.logmsg('info', 'ERROR log message 4', 'X', 'Y')
        results.append(run)
        log.info('ERROR logging: '+str(run))

        run = yield logc.logmsg('info', 'CRITICAL log message 5', 'X', 'Y')
        results.append(run)
        log.info('CRITICAL logging: '+str(run))

