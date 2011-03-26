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
from ion.services.coi.datastore_bootstrap.ion_preload_config import ROOT_USER_ID, HAS_A_ID, IDENTITY_RESOURCE_TYPE_ID, TYPE_OF_ID

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
        self.proc = Process()
        self.proc.op_fetch_blobs = self.proc.workbench.op_fetch_blobs
        yield self.proc.spawn()

        yield self.proc.workbench.pull('datastore', IDENTITY_RESOURCE_TYPE_ID)
        id_type = self.proc.workbench.get_repository(IDENTITY_RESOURCE_TYPE_ID)
        id_type.checkout('master')

        yield self.proc.workbench.pull('datastore', TYPE_OF_ID)
        type_of = self.proc.workbench.get_repository(TYPE_OF_ID)
        type_of.checkout('master')

        yield self.proc.workbench.pull('datastore', ROOT_USER_ID)
        root_user = self.proc.workbench.get_repository(ROOT_USER_ID)
        root_user.checkout('master')


        assoc = self.proc.workbench.create_association(root_user, type_of, id_type)


        yield self.proc.workbench.push('datastore', assoc)


        # run the tests in a completely separate process.
        self.asc = AssociationServiceClient()



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
        idref.key = TYPE_OF_ID

        pair.predicate = idref

        # Set the Object search term

        idref = request.CreateObject(IDREF_TYPE)
        idref.key = IDENTITY_RESOURCE_TYPE_ID

        pair.object = idref



        result = yield self.asc.get_subjects(request)

        self.assertEqual(result.idrefs[0].key, ROOT_USER_ID)
