#!/usr/bin/env python

"""
@file ion/services/coi/resource_registry/test/test_resource_client.py
@author David Stuebe
@brief test service for registering resources and client classes
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

from ion.core.exception import ReceivedApplicationError, ReceivedContainerError

from ion.core.process.process import Process

from ion.core.object import gpb_wrapper
from ion.core.object import workbench
from ion.core.object import object_utils

from ion.core.exception import ReceivedError
from ion.services.coi.resource_registry.resource_client import ResourceClient
from ion.services.coi.resource_registry.association_client import AssociationClient, AssociationInstance, AssociationManager
from ion.services.coi.resource_registry.association_client import AssociationClientError

from ion.test.iontest import IonTestCase
from ion.services.coi.datastore_bootstrap.ion_preload_config import ION_RESOURCE_TYPES, ION_IDENTITIES, ID_CFG, PRELOAD_CFG, ION_DATASETS_CFG, ION_DATASETS, NAME_CFG, DEFAULT_RESOURCE_TYPE_ID
from ion.services.coi.datastore_bootstrap.ion_preload_config import SAMPLE_PROFILE_DATASET_ID, ANONYMOUS_USER_ID, HAS_A_ID, RESOURCE_TYPE_TYPE_ID, OWNED_BY_ID



ADDRESSLINK_TYPE = object_utils.create_type_identifier(object_id=20003, version=1)
PERSON_TYPE = object_utils.create_type_identifier(object_id=20001, version=1)
INVALID_TYPE = object_utils.create_type_identifier(object_id=-1, version=1)
UPDATE_TYPE = object_utils.create_type_identifier(object_id=10, version=1)
INSTRUMENT_TYPE = object_utils.create_type_identifier(object_id=20024, version=1)

class AssociationClientTest(IonTestCase):
    """
    Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):

        yield self._start_container()
        services = [
            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{PRELOAD_CFG:{ION_DATASETS_CFG:True}}},

            {'name':'association_service',
             'module':'ion.services.dm.inventory.association_service',
             'class':'AssociationService'
              },
            {'name':'resource_registry1','module':'ion.services.coi.resource_registry.resource_registry','class':'ResourceRegistryService',
             'spawnargs':{'datastore_service':'datastore'}},
            ]
        sup = yield self._spawn_processes(services)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_create_and_get_association(self):
        '''
        Create a nonsense association and test the get methods of the association client
        '''

        proc = Process()
        yield proc.spawn()

        rc = ResourceClient(proc=proc)
        ac = AssociationClient(proc=proc)


        ds_resource = yield rc.get_instance(SAMPLE_PROFILE_DATASET_ID)
        ds_resource.ResourceName = 'Make junk'

        type_resource = yield rc.get_instance(RESOURCE_TYPE_TYPE_ID)
        type_resource.ResourceName = 'Make more junk'

        association = yield ac.create_association(ds_resource, HAS_A_ID, type_resource)

        self.assertIn(association, ds_resource.ResourceAssociationsAsSubject)
        self.assertIn(association, type_resource.ResourceAssociationsAsObject)

        # Put the association and the resources in the datastore
        rc.put_resource_transaction([ds_resource, type_resource])

        ### Now get these from a completely separate process!

        proc = Process()
        yield proc.spawn()

        rc = ResourceClient(proc=proc)
        ac = AssociationClient(proc=proc)


        p_association = yield ac.get_instance(association.AssociationIdentity)

        self.assertEqual(p_association.Association, association.Association)

        p_ds_resource = yield rc.get_associated_resource_subject(p_association)
        self.assertEqual(p_ds_resource.Resource, ds_resource.Resource)

        p_type_resource = yield rc.get_associated_resource_object(p_association)
        self.assertEqual(p_type_resource.Resource, type_resource.Resource)



    @defer.inlineCallbacks
    def test_association_exists(self):
        '''
        Test the interface to check for an association
        '''

        proc = Process()
        yield proc.spawn()

        ac = AssociationClient(proc=proc)

        result = yield ac.association_exists(SAMPLE_PROFILE_DATASET_ID, OWNED_BY_ID, ANONYMOUS_USER_ID)

        self.assertEqual(result, True)

        result = yield ac.association_exists(SAMPLE_PROFILE_DATASET_ID, OWNED_BY_ID, RESOURCE_TYPE_TYPE_ID)

        self.assertEqual(result, False)


    @defer.inlineCallbacks
    def test_find_associations(self):
        '''
        Get an association based on a
        '''

        proc = Process()
        yield proc.spawn()

        ac = AssociationClient(proc=proc)
        rc = ResourceClient(proc=proc)

        ds_resource = yield rc.get_instance(SAMPLE_PROFILE_DATASET_ID)
        ds_resource.ResourceName = 'Make junk'

        anon_resource = yield rc.get_instance(ANONYMOUS_USER_ID)
        anon_resource.ResourceName = 'Make more junk'

        results = yield ac.find_associations(ds_resource, OWNED_BY_ID, anon_resource)

        for association in results:

            self.assertEqual(association.SubjectReference.key, ds_resource.ResourceIdentity)
            self.assertEqual(association.ObjectReference.key, anon_resource.ResourceIdentity)
