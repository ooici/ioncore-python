#!/usr/bin/env python

"""
@file ion/services/dm/inventory/test/test_association_service.py
@author David Stuebe
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core import ioninit
CONF = ioninit.config(__name__)

from ion.test.iontest import IonTestCase

from ion.core.object import object_utils

from ion.core.process.process import Process
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient

from ion.core.data.storage_configuration_utility import COMMIT_INDEXED_COLUMNS, COMMIT_CACHE

from ion.services.coi.datastore import ION_DATASETS_CFG, PRELOAD_CFG
# Pick three to test existence
from ion.services.coi.datastore_bootstrap.ion_preload_config import ROOT_USER_ID, HAS_A_ID, IDENTITY_RESOURCE_TYPE_ID

from ion.services.dm.inventory.association_service import AssociationServiceClient
from ion.services.dm.inventory.association_service import PREDICATE_OBJECT_QUERY_TYPE, IDREF_TYPE


association_type = object_utils.create_type_identifier(object_id=13, version=1)


class AssociationServiceTest(IonTestCase):
    """
    Testing example hello service.
    """
    services = [
            {'name':'index_store_service','module':'ion.core.data.index_store_service','class':'IndexStoreService',
                'spawnargs':{'indices':COMMIT_INDEXED_COLUMNS} },

            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{PRELOAD_CFG:{ION_DATASETS_CFG:False},
                          COMMIT_CACHE:'ion.core.data.index_store_service.IndexStoreServiceClient'}
                },
            
            {'name':'association_service',
             'module':'ion.services.dm.inventory.association_service',
             'class':'AssociationService'
              },
        ]


    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()


        self.sup = yield self._spawn_processes(self.services)

        # The resource client will make its own process
        self.rc = ResourceClient()

        id_type = yield self.rc.get_instance(IDENTITY_RESOURCE_TYPE_ID)

        yield self.rc.proc.workbench.pull(self.rc.datastore_service, HAS_A_ID)
        has_a = self.rc.proc.workbench.get_repository(HAS_A_ID)
        has_a.checkout('master')

        root_user = yield  self.rc.get_instance(ROOT_USER_ID)

        assoc = self.rc.create_association(root_user, has_a, id_type)


        yield self.rc.proc.workbench.push(self.rc.datastore_service, assoc)


        # run the tests in a completely separate process.
        self.proc = Process()
        yield self.proc.spawn()
        self.asc = AssociationServiceClient(proc=self.proc)



    @defer.inlineCallbacks
    def tearDown(self):
       log.info('Tearing Down Test Container')
       yield self._shutdown_processes()
       yield self._stop_container()


    @defer.inlineCallbacks
    def test_association(self):

        request = yield self.proc.message_client.create_instance(PREDICATE_OBJECT_QUERY_TYPE)

        pair = request.pairs.add()

        # Set the predicate search term
        idref = request.CreateObject(IDREF_TYPE)
        idref.key = HAS_A_ID

        pair.predicate = idref

        # Set the Object search term

        idref = request.CreateObject(IDREF_TYPE)
        idref.key = IDENTITY_RESOURCE_TYPE_ID

        pair.object = idref



        result = yield self.asc.get_subjects(request)

        print 'RESULT:',result
        print 'ROOT_USER_ID:', ROOT_USER_ID
