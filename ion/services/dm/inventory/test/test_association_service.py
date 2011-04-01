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

from ion.core.data.storage_configuration_utility import COMMIT_INDEXED_COLUMNS, COMMIT_CACHE

from ion.services.coi.resource_registry_beta import resource_client

from ion.core.data import store
from ion.services.coi.datastore import ION_DATASETS_CFG, PRELOAD_CFG
# Pick three to test existence
from ion.services.coi.datastore_bootstrap.ion_preload_config import ROOT_USER_ID, HAS_A_ID, IDENTITY_RESOURCE_TYPE_ID, TYPE_OF_ID, ANONYMOUS_USER_ID, HAS_LIFE_CYCLE_STATE_ID, OWNED_BY_ID, SAMPLE_PROFILE_DATASET_ID, DATASET_RESOURCE_TYPE_ID, RESOURCE_TYPE_TYPE_ID

from ion.services.dm.inventory.association_service import AssociationServiceClient
from ion.services.dm.inventory.association_service import PREDICATE_OBJECT_QUERY_TYPE, IDREF_TYPE


ASSOCIATION_TYPE = object_utils.create_type_identifier(object_id=13, version=1)
PREDICATE_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=25, version=1)
LCS_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=26, version=1)

class AssociationServiceTest(IonTestCase):
    """
    Testing association service.
    """
    services = [
            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{PRELOAD_CFG:{ION_DATASETS_CFG:True},
                          COMMIT_CACHE:'ion.core.data.store.IndexStore'}
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

        self.proc = Process()
        self.proc.op_fetch_blobs = self.proc.workbench.op_fetch_blobs
        yield self.proc.spawn()

        # run the tests in a completely separate process.
        self.asc = AssociationServiceClient(proc=self.proc)



    @defer.inlineCallbacks
    def tearDown(self):
       log.info('Tearing Down Test Container')

       store.Store.clear()
       store.IndexStore.kvs.clear()
       store.IndexStore.indices.clear()

       yield self._shutdown_processes()
       yield self._stop_container()


    @defer.inlineCallbacks
    def test_association_by_type(self):

        request = yield self.proc.message_client.create_instance(PREDICATE_OBJECT_QUERY_TYPE)

        pair = request.pairs.add()

        # Set the predicate search term
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = TYPE_OF_ID

        pair.predicate = pref

        # Set the Object search term

        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = IDENTITY_RESOURCE_TYPE_ID

        pair.object = type_ref



        result = yield self.asc.get_subjects(request)

        self.assertEqual(len(result.idrefs),2)
        self.assertIn(result.idrefs[0].key, [ANONYMOUS_USER_ID, ROOT_USER_ID])
        self.assertIn(result.idrefs[1].key, [ANONYMOUS_USER_ID, ROOT_USER_ID])


    @defer.inlineCallbacks
    def test_association_by_type_and_lcs(self):

        request = yield self.proc.message_client.create_instance(PREDICATE_OBJECT_QUERY_TYPE)

        pair = request.pairs.add()

        # Set the predicate search term
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = TYPE_OF_ID

        pair.predicate = pref

        # Set the Object search term

        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = IDENTITY_RESOURCE_TYPE_ID

        pair.object = type_ref

        # Add a life cycle state request
        pair = request.pairs.add()

        # Set the predicate search term
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = HAS_LIFE_CYCLE_STATE_ID

        pair.predicate = pref


        # Set the Object search term
        state_ref = request.CreateObject(LCS_REFERENCE_TYPE)
        state_ref.lcs = state_ref.LifeCycleState.ACTIVE
        pair.object = state_ref

        result = yield self.asc.get_subjects(request)

        self.assertEqual(len(result.idrefs),2)
        self.assertIn(result.idrefs[0].key, [ANONYMOUS_USER_ID, ROOT_USER_ID])
        self.assertIn(result.idrefs[1].key, [ANONYMOUS_USER_ID, ROOT_USER_ID])


    @defer.inlineCallbacks
    def test_association_by_type_and_lcs_set_state(self):

        # Change the lcs !
        rc = resource_client.ResourceClient()

        uid = yield rc.get_instance(ANONYMOUS_USER_ID)

        uid.ResourceLifeCycleState = uid.NEW

        yield rc.put_instance(uid)


        request = yield self.proc.message_client.create_instance(PREDICATE_OBJECT_QUERY_TYPE)

        pair = request.pairs.add()

        # Set the predicate search term
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = TYPE_OF_ID

        pair.predicate = pref

        # Set the Object search term

        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = IDENTITY_RESOURCE_TYPE_ID

        pair.object = type_ref

        # Add a life cycle state request
        pair = request.pairs.add()

        # Set the predicate search term
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = HAS_LIFE_CYCLE_STATE_ID

        pair.predicate = pref


        # Set the Object search term
        state_ref = request.CreateObject(LCS_REFERENCE_TYPE)
        state_ref.lcs = state_ref.LifeCycleState.ACTIVE
        pair.object = state_ref

        result = yield self.asc.get_subjects(request)

        self.assertEqual(len(result.idrefs),1)
        self.assertIn(result.idrefs[0].key, ROOT_USER_ID)



    @defer.inlineCallbacks
    def test_association_by_owner(self):

        request = yield self.proc.message_client.create_instance(PREDICATE_OBJECT_QUERY_TYPE)

        pair = request.pairs.add()

        # Set the predicate search term
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = OWNED_BY_ID

        pair.predicate = pref

        # Set the Object search term

        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = ANONYMOUS_USER_ID

        pair.object = type_ref

        result = yield self.asc.get_subjects(request)

        key_list = []
        for idref in result.idrefs:
            key_list.append(idref.key)

        self.assertIn(SAMPLE_PROFILE_DATASET_ID, key_list)


    @defer.inlineCallbacks
    def test_association_by_owner_and_type_find_1(self):

        request = yield self.proc.message_client.create_instance(PREDICATE_OBJECT_QUERY_TYPE)

        pair = request.pairs.add()

        # Set the predicate search term
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = OWNED_BY_ID

        pair.predicate = pref

        # Set the Object search term

        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = ANONYMOUS_USER_ID

        pair.object = type_ref

        # Add search by type
        pair = request.pairs.add()

        # Set the predicate search term
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = TYPE_OF_ID

        pair.predicate = pref

        # Set the Object search term

        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = DATASET_RESOURCE_TYPE_ID

        pair.object = type_ref


        result = yield self.asc.get_subjects(request)

        # Depends on how your preload config is set up - there may be more datasets!
        self.assertEqual(len(result.idrefs)>=1,True)

        key_list = []
        for idref in result.idrefs:
            key_list.append(idref.key)

        self.assertIn(SAMPLE_PROFILE_DATASET_ID, key_list)



    @defer.inlineCallbacks
    def test_association_by_owner_and_type_find_none(self):

        request = yield self.proc.message_client.create_instance(PREDICATE_OBJECT_QUERY_TYPE)

        pair = request.pairs.add()

        # Set the predicate search term
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = OWNED_BY_ID

        pair.predicate = pref

        # Set the Object search term

        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = ANONYMOUS_USER_ID

        pair.object = type_ref

        # Add search by type
        pair = request.pairs.add()

        # Set the predicate search term
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = TYPE_OF_ID

        pair.predicate = pref

        # Set the Object search term

        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = RESOURCE_TYPE_TYPE_ID

        pair.object = type_ref

        result = yield self.asc.get_subjects(request)

        # The anonymous user should never own and resource type resources!
        self.assertEqual(len(result.idrefs),0)


    @defer.inlineCallbacks
    def test_association_by_owner_and_state(self):

        request = yield self.proc.message_client.create_instance(PREDICATE_OBJECT_QUERY_TYPE)

        pair = request.pairs.add()

        # Set the predicate search term
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = OWNED_BY_ID

        pair.predicate = pref

        # Set the Object search term

        type_ref = request.CreateObject(IDREF_TYPE)
        type_ref.key = ANONYMOUS_USER_ID

        pair.object = type_ref

        # Add a life cycle state request
        pair = request.pairs.add()

        # Set the predicate search term
        pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = HAS_LIFE_CYCLE_STATE_ID

        pair.predicate = pref


        # Set the Object search term
        state_ref = request.CreateObject(LCS_REFERENCE_TYPE)
        state_ref.lcs = state_ref.LifeCycleState.ACTIVE
        pair.object = state_ref



        result = yield self.asc.get_subjects(request)

        self.assertEqual(len(result.idrefs)>=1,True)

        key_list = []
        for idref in result.idrefs:
            key_list.append(idref.key)

        self.assertIn(SAMPLE_PROFILE_DATASET_ID, key_list)



