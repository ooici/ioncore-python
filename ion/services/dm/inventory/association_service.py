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
from ion.core.process.process import ProcessFactory, Process, ProcessClient
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.core.data.storage_configuration_utility import COMMIT_INDEXED_COLUMNS, PREDICATE_KEY, OBJECT_KEY, BRANCH_NAME

from ion.core.data import store

from ion.core.object import object_utils

from ion.core import ioninit
CONF = ioninit.config(__name__)

### Need other objects here

IDREF_TYPE = object_utils.create_type_identifier(object_id=4, version=1)

SUBJECT_PREDICATE_QUERY_TYPE = object_utils.create_type_identifier(object_id=16, version=1)
PREDICATE_OBJECT_QUERY_TYPE = object_utils.create_type_identifier(object_id=15, version=1)
QUERY_RESULT_TYPE = object_utils.create_type_identifier(object_id=22, version=1)

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

        #index_store_class_name = self.spawn_args.get(COMMIT_CACHE, CONF.getValue(COMMIT_CACHE, default='ion.core.data.store.IndexStore'))
        index_store_class_name = self.spawn_args.get('index_store_class', CONF.getValue('index_store_class', default='ion.core.data.index_store_service.IndexStoreServiceClient'))

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


        subjects = set()

        first_pair = True

        for pair in predicate_object_query.pairs:


            q = store.Query()
            # Get only the latest version !
            q.add_predicate_gt(BRANCH_NAME,'')

            # Get only associations to the correct branch & key for the subject and object
            q.add_predicate_eq(PREDICATE_KEY, pair.predicate.key)

            q.add_predicate_eq(OBJECT_KEY, pair.object.key)

            rows = yield self.index_store.query(q)

            print 'PK', pair.predicate.key
            print 'OK', pair.object.key


            print 'ROWS',rows
            pair_subjects = set()
            for row in rows:

                # Invert the index so we can find the associations by subject repository & key
                totalkey = (row['subject_repository'] , row['subject_branch'])

                # Check to make sure we did not hit an inconsistent state where there appear to be two head commits on the association!
                pair_subjects.add(totalkey)


            if first_pair:
                subjects = pair_subjects
                first_pair = False
            else:
                subjects.intersection_update(pair_subjects)

        log.info('Found %s subjects!' % len(subjects))

        list_of_subjects = yield self.message_client.create_instance(QUERY_RESULT_TYPE)

        for subject in subjects:
            idref = list_of_subjects.add()
            idref.key = subject[0]
            idref.branch = subject[1]


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


