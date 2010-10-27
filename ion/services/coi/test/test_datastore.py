#!/usr/bin/env python

"""
@file ion/services/coi/test/test_hello.py
@author David Stuebe
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.services.coi.datastore import DataStoreServiceClient
from ion.test.iontest import IonTestCase

class DataStoreTest(IonTestCase):
    """
    Testing example hello service.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        
        #services = [
        #    {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
        #     'spawnargs':{'servicename':'ps1'}
        #        },
        #   {'name':'ds2','module':'ion.services.coi.datastore','class':'DataStoreService',
        #    'spawnargs':{'servicename':'ps2'}
        #        }
        #]

        services = [
            {'name':'ds1','module':'ion.core.process.process','class':'Process',
             'spawnargs':{'proc-name':'ps1'}
                },
            {'name':'ds2','module':'ion.core.process.process','class':'Process',
             'spawnargs':{'proc-name':'ps2'}
                }
        ]

        self.sup = yield self._spawn_processes(services)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_hello(self):


        child_id = yield self.sup.get_child_id('ds1')
        log.debug('Process ID:' + str(child_id))
        self.proc1 = self._get_procinstance(child_id)

        yield self.proc1.workbench.push('MY TEST STRING!!!','4')
