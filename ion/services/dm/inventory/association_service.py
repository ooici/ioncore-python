#!/usr/bin/env python

"""
@file ion/services/dm/inventory/association_service.py
@author David Stuebe
@brief A service to provide indexing and search capability of objects in the datastore
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.exception import ApplicationError

import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.core.data.storage_configuration_utility import COMMIT_INDEXED_COLUMNS, PREDICATE_KEY, OBJECT_KEY, BRANCH_NAME, SUBJECT_KEY, SUBJECT_COMMIT, SUBJECT_BRANCH, RESOURCE_OBJECT_TYPE, RESOURCE_LIFE_CYCLE_STATE, REPOSITORY_KEY

from ion.services.coi.datastore_bootstrap.ion_preload_config import HAS_LIFE_CYCLE_STATE_ID, TYPE_OF_ID

from ion.core.data import store

from ion.core.object import object_utils

from ion.core import ioninit
CONF = ioninit.config(__name__)

### Need other objects here

IDREF_TYPE = object_utils.create_type_identifier(object_id=4, version=1)

SUBJECT_PREDICATE_QUERY_TYPE = object_utils.create_type_identifier(object_id=16, version=1)
PREDICATE_OBJECT_QUERY_TYPE = object_utils.create_type_identifier(object_id=15, version=1)
QUERY_RESULT_TYPE = object_utils.create_type_identifier(object_id=22, version=1)

PREDICATE_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=25, version=1)

LifeCycleStateObject = object_utils.create_type_identifier(object_id=26, version=1)

class AssociationServiceError(ApplicationError):
    """
    An exception class for the Association Service
    """


class AssociationService(ServiceProcess):
    """
    The Association Service
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='association_service',
                                             version='0.1.0',
                                             dependencies=[])

    @defer.inlineCallbacks
    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.

        index_store_class_name = self.spawn_args.get('index_store_class', CONF.getValue('index_store_class', default='ion.core.data.store.IndexStore'))

        index_store_class = pu.get_class(index_store_class_name)
        assert store.IIndexStore.implementedBy(index_store_class), \
            'The back end class for the index store passed to the association service does not implement the required IIndexStore interface.'


        self.index_store = yield defer.maybeDeferred(index_store_class, self, **{'indices':COMMIT_INDEXED_COLUMNS})

        log.info('SLC_INIT Association Service')

    @defer.inlineCallbacks
    def op_get_subjects(self, predicate_object_query, headers, msg):
        log.info('op_get_subjects: ')

        if predicate_object_query.MessageType != PREDICATE_OBJECT_QUERY_TYPE:
            raise AssociationServiceError('Unexpected type received \n %s' % str(predicate_object_query), predicate_object_query.ResponseCodes.BAD_REQUEST)

        if len(predicate_object_query.pairs) == 0:
            raise AssociationServiceError('Invalid Predicate Object Query received - zero length pairs!', predicate_object_query.ResponseCodes.BAD_REQUEST)


        life_cycle_pair = None
        type_of_pair = None

        subjects = set()

        first_pair = True

        for pair in predicate_object_query.pairs:


            q = store.Query()
            # Get only the latest version of the association!
            q.add_predicate_gt(BRANCH_NAME,'')

            # Build a query for the predicate of the search
            if pair.predicate.ObjectType != PREDICATE_REFERENCE_TYPE:
                raise AssociationServiceError('Invlalid predicate type in predicate object pairs request to get_subjects.', predicate_object_query.ResponseCodes.BAD_REQUEST)


            # if the predicate is for life cycle state or type - do not use associations to find it.
            if pair.predicate.key == HAS_LIFE_CYCLE_STATE_ID:
                if pair.object.ObjectType != LifeCycleStateObject:
                    raise AssociationServiceError('Invalid object type in predicate object pairs request to get_subjects.', predicate_object_query.ResponseCodes.BAD_REQUEST)

                if not life_cycle_pair:
                    life_cycle_pair = pair
                else:
                    # two life cycle pairs in one request is an error!
                    raise AssociationServiceError('Invalid search by life cycle state - two predicate object pairs in the query specify life cycle. There can be only One!', predicate_object_query.ResponseCodes.BAD_REQUEST)

                continue

            elif pair.predicate.key == TYPE_OF_ID:

                if not type_of_pair:
                    type_of_pair = pair

                else:
                    # two life cycle pairs in one request is an error!
                    raise AssociationServiceError('Invalid search by type - two predicate object pairs in the query specify type_of. There can be only One!', predicate_object_query.ResponseCodes.BAD_REQUEST)
                continue

            q.add_predicate_eq(PREDICATE_KEY, pair.predicate.key)

            q.add_predicate_eq(OBJECT_KEY, pair.object.key)

            rows = yield self.index_store.query(q)

            pair_subjects = set()
            for key, row in rows.items():

                # Invert the index so we can find the associations by subject repository & key
                totalkey = (row[SUBJECT_KEY] , row[SUBJECT_BRANCH], row[SUBJECT_COMMIT])

                # Check to make sure we did not hit an inconsistent state where there appear to be two head commits on the association!
                pair_subjects.add(totalkey)


            # Now - at the end of the loop over the pairs - take the intersection with the current search results!
            if first_pair:
                subjects = pair_subjects
                first_pair = False
            else:
                subjects.intersection_update(pair_subjects)


        # Now apply any search by type or lcs!
        if first_pair:
            # If there was no search by association - only by type and state - type must be true!

            if not type_of_pair:
                raise AssociationServiceError('Illegal request to association service. Can not return all subjects by life cycle - there are too many!')

            # Now we are not searching for associations - we are taking a shortcut - straight to the denormalized row for a resource commit!
            q = store.Query()
            q.add_predicate_gt(BRANCH_NAME,'')


            # This is by definition a search for a Resource
            q.add_predicate_eq(RESOURCE_OBJECT_TYPE, type_of_pair.object.key)


            if life_cycle_pair:
                q.add_predicate_eq(RESOURCE_LIFE_CYCLE_STATE, life_cycle_pair.object.lcs)


            # Get all the results that meet the type / state query
            rows = yield self.index_store.query(q)

            # This is a simple search - just add the results!
            for key, row in rows.items():

                totalkey = (row[REPOSITORY_KEY] , row[BRANCH_NAME], key)

                subjects.add(totalkey)

        elif len(subjects) > 0 and life_cycle_pair or type_of_pair:
            # Now apply search by type and state... if needed.

            new_set=set()

            # Assumption - the number of rows returned by the association search is much smaller than what will come from search by type or state!
            for subject in subjects:

                # There for, for each result - check and see if it meets the criteria by type and state...
                q = store.Query()

                # Test this repository key
                q.add_predicate_eq(REPOSITORY_KEY, subject[0])

                # Latest state
                q.add_predicate_gt(BRANCH_NAME,'')

                if life_cycle_pair:
                    q.add_predicate_eq(RESOURCE_LIFE_CYCLE_STATE, life_cycle_pair.object.lcs)

                if type_of_pair:
                    q.add_predicate_eq(RESOURCE_OBJECT_TYPE, type_of_pair.object.key)

                # Get all the results that meet the type / state query
                rows = yield self.index_store.query(q)

                for key, row in rows.items():

                    totalkey = (row[REPOSITORY_KEY] , row[BRANCH_NAME], key)

                    new_set.add(totalkey)

            # Keep the results from our narrowed search
            subjects = new_set

            
        log.info('Found %s subjects!' % len(subjects))

        # Do we want to check for which current heads of each subject are descendents of the


        list_of_subjects = yield self.message_client.create_instance(QUERY_RESULT_TYPE)

        for subject in subjects:

            link = list_of_subjects.idrefs.add()

            idref= list_of_subjects.CreateObject(IDREF_TYPE)
            idref.key = subject[0]
            idref.branch = subject[1]
            idref.commit = subject[2]

            link.SetLink(idref)

        yield self.reply_ok(msg, list_of_subjects)



    @defer.inlineCallbacks
    def op_get_objects(self, subject_predicate_query, headers, msg):
        log.info('op_get_objects: ')

        if subject_predicate_query.MessageType != PREDICATE_OBJECT_QUERY_TYPE:
            raise AssociationServiceError('Unexpected type received \n %s' % str(subject_predicate_query), subject_predicate_query.ResponseCodes.BAD_REQUEST)



        list_of_objects = yield self.message_client.create_instance(QUERY_RESULT_TYPE)
        yield self.reply_ok(msg, list_of_objects)


    @defer.inlineCallbacks
    def op_object_associations(self, object_reference, headers, msg):
        log.info('op_get_objects: ')

        if object_reference.MessageType != IDREF_TYPE:
            raise AssociationServiceError('Unexpected type received \n %s' % str(object_reference), object_reference.ResponseCodes.BAD_REQUEST)

        list_of_associations = yield self.message_client.create_instance(QUERY_RESULT_TYPE)
        yield self.reply_ok(msg, list_of_associations)



    @defer.inlineCallbacks
    def op_subject_associations(self, subject_reference, headers, msg):
        log.info('op_get_objects: ')

        if subject_reference.MessageType != IDREF_TYPE:
            raise AssociationServiceError('Unexpected type received \n %s' % str(subject_reference), subject_reference.ResponseCodes.BAD_REQUEST)

        list_of_associations = yield self.message_client.create_instance(QUERY_RESULT_TYPE)
        yield self.reply_ok(msg, list_of_associations)




class AssociationServiceClient(ServiceClient):
    """
    Association Service Client
    """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "association_service"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def get_subjects(self, msg):
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('get_subjects', msg)
        
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_objects(self, msg):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('get_objects', msg)

        defer.returnValue(content)


    @defer.inlineCallbacks
    def get_subject_associations(self, msg):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('subject_associations', msg)

        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_object_associations(self, msg):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('object_associations', msg)

        defer.returnValue(content)



# Spawn of the process using the module name
factory = ProcessFactory(AssociationService)


