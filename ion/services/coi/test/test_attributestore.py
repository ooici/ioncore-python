#!/usr/bin/env python

"""
@file ion/services/coi/test/test_attributestore.py
@author Michael Meisinger
@author Matt Rodriguez
@brief test attribute store service

@TODO refactor attribute store test or potentially remove is we are not using it?
Already tested in ion.core.data
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.services.coi.attributestore import AttributeStoreClient
from ion.test.iontest import IonTestCase
from ion.core.data.test import test_store


class AttrStoreServiceTest(test_store.StoreServiceTest):
    """
    Testing service classes of attribute store
    """
    @defer.inlineCallbacks
    def _setup_backend(self):
        """
        Start the service and setup the client to the backend for the test.
        """

        yield self._start_container()
        self.timeout = 30
        services = [
            {'name':'attribute_store','module':'ion.services.coi.attributestore','class':'AttributeStoreService'},
        ]
        sup = yield self._spawn_processes(services)
        client = AttributeStoreClient(proc=sup)

        defer.returnValue(client)
