#!/usr/bin/env python

"""
@file ion/services/dm/inventory/association_service.py
@author David Stuebe
@author Matt Rodriguez
@brief A service to provide indexing and search capability of objects in the datastore
"""

import ion.util.ionlog
from net.ooici.core.message.ion_message_pb2 import BAD_REQUEST

log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.exception import ApplicationError

import ion.util.procutils as pu
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.core.data import cassandra
from ion.core.data.storage_configuration_utility import COMMIT_INDEXED_COLUMNS, PREDICATE_KEY, OBJECT_KEY, COMMIT_CACHE
from ion.core.data.storage_configuration_utility import  BRANCH_NAME, SUBJECT_KEY,  SUBJECT_BRANCH, RESOURCE_OBJECT_TYPE 
from ion.core.data.storage_configuration_utility import  RESOURCE_LIFE_CYCLE_STATE, REPOSITORY_KEY, OBJECT_BRANCH
from ion.core.data.storage_configuration_utility import get_cassandra_configuration, STORAGE_PROVIDER, PERSISTENT_ARCHIVE

from ion.services.coi.datastore_bootstrap.ion_preload_config import HAS_LIFE_CYCLE_STATE_ID, TYPE_OF_ID

from ion.core.data import store

from ion.core.object import object_utils

from ion.core import ioninit
CONF = ioninit.config(__name__)

### Need other objects here

IDREF_TYPE = object_utils.create_type_identifier(object_id=4, version=1)

SUBJECT_PREDICATE_QUERY_TYPE = object_utils.create_type_identifier(object_id=16, version=1)
PREDICATE_OBJECT_QUERY_TYPE = object_utils.create_type_identifier(object_id=15, version=1)
ASSOCIATION_QUERY_MSG_TYPE = object_utils.create_type_identifier(object_id=27, version=1)
ASSOCIATION_GET_STAR_MSG_TYPE = object_utils.create_type_identifier(object_id=28, version=1)
BOOL_MSG_TYPE = object_utils.create_type_identifier(object_id=30, version=1)


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

    def __init__(self, *args, **kwargs):


        ServiceProcess.__init__(self, *args, **kwargs)

        index_store_class_name = self.spawn_args.get('index_store_class', CONF.getValue('index_store_class', default='ion.core.data.store.IndexStore'))
        self.index_store_class = pu.get_class(index_store_class_name)


        assert store.IIndexStore.implementedBy(self.index_store_class), \
            'The back end class for the index store passed to the association service does not implement the required IIndexStore interface.'
                # Service life cycle state. Initialize service here. Can use yields.
        self._username = self.spawn_args.get("username", CONF.getValue("username", None))
        self._password = self.spawn_args.get("password", CONF.getValue("password",None))


        # Get the configuration for cassandra - may or may not be used depending on the backend class
        self._storage_conf = get_cassandra_configuration()



    @defer.inlineCallbacks
    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.
        
        if issubclass(self.index_store_class, cassandra.CassandraIndexedStore):
            log.info("Instantiating Cassandra Index Store")

            storage_provider = self._storage_conf[STORAGE_PROVIDER]
            keyspace = self._storage_conf[PERSISTENT_ARCHIVE]['name']

            self.index_store = self.index_store_class(self._username, self._password, storage_provider, keyspace, COMMIT_CACHE)

            yield self.register_life_cycle_object(self.index_store)
        else:
            self.index_store = self.index_store_class(self, indices=COMMIT_INDEXED_COLUMNS )

        log.info('SLC_INIT Association Service: index store class - %s' % self.index_store_class)

    @defer.inlineCallbacks
    def _get_subjects(self, predicate_pairs):
        life_cycle_pair = None
        type_of_pair = None

        subjects = set()

        # subject_keys is the set of keys for the associated subjects - to reject quickly any that are not present
        subject_keys = set()

        first_pair = True

        for pair in predicate_pairs:


            q = store.Query()
            # Get only the latest version of the association!
            q.add_predicate_gt(BRANCH_NAME,'')

            # Build a query for the predicate of the search
            if pair.predicate.ObjectType != PREDICATE_REFERENCE_TYPE:
                raise AssociationServiceError('Invalid predicate type in _get_subjects.', BAD_REQUEST)


            # if the predicate is for life cycle state or type - do not use associations to find it.
            if pair.predicate.key == HAS_LIFE_CYCLE_STATE_ID:
                if pair.object.ObjectType != LifeCycleStateObject:
                    raise AssociationServiceError('Invalid object type in _get_subjects.', BAD_REQUEST)

                if not life_cycle_pair:
                    life_cycle_pair = pair
                else:
                    # two life cycle pairs in one request is an error!
                    raise AssociationServiceError('Invalid search by life cycle state - two predicate object pairs in the query specify life cycle. There can be only One!', BAD_REQUEST)  # Highlander!

                continue

            elif pair.predicate.key == TYPE_OF_ID:

                if not type_of_pair:
                    type_of_pair = pair

                else:
                    # two life cycle pairs in one request is an error!
                    raise AssociationServiceError('Invalid search by type - two predicate object pairs in the query specify type_of. There can be only One!', BAD_REQUEST)
                continue

            q.add_predicate_eq(PREDICATE_KEY, pair.predicate.key)

            q.add_predicate_eq(OBJECT_KEY, pair.object.key)

            rows = yield self.index_store.query(q)

            # subject_pointers is the resulting set of pointers to the current state of the association subject
            subjects_pointers = set()

            current_keys = set()
            for key, row in rows.items():


                #@TODO - check for divergence and branches in the association and in the object - not just the subject

                if not first_pair and row[SUBJECT_KEY] not in subject_keys:
                    # The result we are looking for is an intersection operation. If this key is not here escape!
                    continue
                current_keys.add(row[SUBJECT_KEY])

                # Get the latest commits for the Subject_Key
                subject_query = store.Query()
                # Get only the head or get all? Hmmm not sure...
                subject_query.add_predicate_gt(BRANCH_NAME,'')
                subject_query.add_predicate_eq(REPOSITORY_KEY,row[SUBJECT_KEY])
                subject_heads = yield self.index_store.query(subject_query)

                branches = []
                for commit_key, commit_row in subject_heads.items():

                    if commit_row[BRANCH_NAME] in branches:
                        raise NotImplementedError('Dealing with divergence in an associated Subject is not yet supported')

                    else:
                        branches.append(commit_row[BRANCH_NAME])

                    if commit_row[BRANCH_NAME] == row[SUBJECT_BRANCH]:
                        # We do not need to determine ancestry - the branch name is the same!

                        # return the pointer to this commit - this is the latest version of the associated subject!
                        totalkey = (row[SUBJECT_KEY] , row[SUBJECT_BRANCH])

                        # Check to make sure we did not hit an inconsistent state where there appear to be two head commits on the association!
                        subjects_pointers.add(totalkey)
                    else:
                        raise NotImplementedError('Dealing with associations to a subject with multiple branches is not yet supported')


            # Now - at the end of the loop over the pairs - take the intersection with the current search results!
            if first_pair:
                subjects = subjects_pointers
                subject_keys.update(current_keys)
                first_pair = False
            else:
                subject_keys.intersection_update(current_keys)
                subjects.intersection_update(subjects_pointers)


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
                q.add_predicate_eq(RESOURCE_LIFE_CYCLE_STATE, str(life_cycle_pair.object.lcs))


            # Get all the results that meet the type / state query
            rows = yield self.index_store.query(q)

            # This is a simple search - just add the results!
            for key, row in rows.items():

                totalkey = (row[REPOSITORY_KEY] , row[BRANCH_NAME])

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
                    q.add_predicate_eq(RESOURCE_LIFE_CYCLE_STATE, str(life_cycle_pair.object.lcs))

                if type_of_pair:
                    q.add_predicate_eq(RESOURCE_OBJECT_TYPE, type_of_pair.object.key)

                # Get all the results that meet the type / state query
                rows = yield self.index_store.query(q)

                for key, row in rows.items():

                    totalkey = (row[REPOSITORY_KEY] , row[BRANCH_NAME])

                    new_set.add(totalkey)

            # Keep the results from our narrowed search
            subjects = new_set


        log.info('Found %s subjects!' % len(subjects))
        defer.returnValue(subjects)

    @defer.inlineCallbacks
    def op_get_subjects(self, predicate_object_query, headers, msg):
        """
        @see AssociationServiceClient.get_subjects
        """
        log.info('op_get_subjects: ')

        if predicate_object_query.MessageType != PREDICATE_OBJECT_QUERY_TYPE:
            raise AssociationServiceError('Unexpected type received \n %s' % str(predicate_object_query), predicate_object_query.ResponseCodes.BAD_REQUEST)

        if len(predicate_object_query.pairs) == 0:
            raise AssociationServiceError('Invalid Predicate Object Query received - zero length pairs!', predicate_object_query.ResponseCodes.BAD_REQUEST)

        subjects = yield self._get_subjects(predicate_object_query.pairs)
        list_of_subjects = yield self.message_client.create_instance(QUERY_RESULT_TYPE)

        for subject in subjects:

            link = list_of_subjects.idrefs.add()

            idref= list_of_subjects.CreateObject(IDREF_TYPE)
            idref.key = subject[0]
            idref.branch = subject[1]
            #idref.commit = subject[2]

            link.SetLink(idref)

        yield self.reply_ok(msg, list_of_subjects)

    @defer.inlineCallbacks
    def _get_objects(self, subject_pairs):
        # The resulting set of Objects
        objects = set()

        first_pair = True

        # subject_keys is the set of keys for the associated subjects - to reject quickly any that are not present
        object_keys = set()

        for pair in subject_pairs:


            q = store.Query()
            # Get only the latest version of the association!
            q.add_predicate_gt(BRANCH_NAME,'')

            # Build a query for the predicate of the search
            if pair.predicate.ObjectType != PREDICATE_REFERENCE_TYPE:
                raise AssociationServiceError('Invalid predicate type in _get_objects.', BAD_REQUEST)

            q.add_predicate_eq(PREDICATE_KEY, pair.predicate.key)

            q.add_predicate_eq(SUBJECT_KEY, pair.subject.key)

            rows = yield self.index_store.query(q)

            # subject_pointers is the resulting set of pointers to the current state of the association subject
            objects_pointers = set()

            current_keys=set()
            for key, row in rows.items():

                if not first_pair and row[OBJECT_KEY] not in object_keys:
                    # The result we are looking for is an intersection operation. If this key is not her escape!
                    continue

                current_keys.add(row[OBJECT_KEY])

                # Get the latest commits for the Subject_Key
                object_query = store.Query()
                # Get only the head or get all? Hmmm not sure...
                object_query.add_predicate_gt(BRANCH_NAME,'')
                object_query.add_predicate_eq(REPOSITORY_KEY,row[OBJECT_KEY])
                object_heads = yield self.index_store.query(object_query)

                branches = []
                for commit_key, commit_row in object_heads.items():

                    if commit_row[BRANCH_NAME] in branches:
                        raise NotImplementedError('Dealing with divergence in an associated Object is not yet supported')

                    else:
                        branches.append(commit_row[BRANCH_NAME])

                    if commit_row[BRANCH_NAME] == row[OBJECT_BRANCH]:
                        # We do not need to determine ancestry - the branch name is the same!

                        # return the pointer to this commit - this is the latest version of the associated subject!
                        totalkey = (row[OBJECT_KEY] , row[OBJECT_BRANCH])

                        # Check to make sure we did not hit an inconsistent state where there appear to be two head commits on the association!
                        objects_pointers.add(totalkey)
                    else:
                        raise NotImplementedError('Dealing with associations to a Object with multiple branches is not yet supported')


            # Now - at the end of the loop over the pairs - take the intersection with the current search results!
            if first_pair:
                objects = objects_pointers
                object_keys.update(current_keys)
                first_pair = False
            else:
                object_keys.intersection_update(current_keys)
                objects.intersection_update(objects_pointers)

        log.info('Found %s objects!' % len(objects))
        defer.returnValue(objects)

    @defer.inlineCallbacks
    def op_get_objects(self, subject_predicate_query, headers, msg):
        """
        @see AssociationServiceClient.get_objects
        """
        log.info('op_get_objects: ')

        if subject_predicate_query.MessageType != SUBJECT_PREDICATE_QUERY_TYPE:
            raise AssociationServiceError('Unexpected type received \n %s' % str(subject_predicate_query), subject_predicate_query.ResponseCodes.BAD_REQUEST)


        if len(subject_predicate_query.pairs) is 0:
            raise AssociationServiceError('Invalid Subject Predicate Query received - zero length pairs!', subject_predicate_query.ResponseCodes.BAD_REQUEST)

        objects = yield self._get_objects(subject_predicate_query.pairs)
        list_of_objects = yield self.message_client.create_instance(QUERY_RESULT_TYPE)

        for obj in objects:

            link = list_of_objects.idrefs.add()

            idref= list_of_objects.CreateObject(IDREF_TYPE)
            idref.key = obj[0]
            idref.branch = obj[1]

            link.SetLink(idref)

        yield self.reply_ok(msg, list_of_objects)

    @defer.inlineCallbacks
    def op_get_star(self, content, headers, msg):
        """

        """
        log.info('op_get_star')

        if content.MessageType != ASSOCIATION_GET_STAR_MSG_TYPE:
            raise AssociationServiceError('Unexpected type received \n %s' % str(content), content.ResponseCodes.BAD_REQUEST)

        if len(content.subject_pairs) == 0 or len(content.object_pairs) == 0:
           raise AssociationServiceError('Invalid getstar query received - zero length pairs!', content.ResponseCodes.BAD_REQUEST)

        subjects = yield self._get_subjects(content.object_pairs)
        objects = yield self._get_objects(content.subject_pairs)

        stars = set(subjects)
        stars.intersection_update(objects)

        log.info("Intersection: %d items" % len(stars))

        list_of_star = yield self.message_client.create_instance(QUERY_RESULT_TYPE)
        for obj in stars:

            link = list_of_star.idrefs.add()

            idref= list_of_star.CreateObject(IDREF_TYPE)
            idref.key = obj[0]
            idref.branch = obj[1]

            link.SetLink(idref)

        yield self.reply_ok(msg, list_of_star)

        log.info('/op_get_star')

    @defer.inlineCallbacks
    def op_object_associations(self, object_reference, headers, msg):
        """
        @see AssociationServiceClient.object_associations
        """
        log.info('op_get_objects: ')

        if object_reference.MessageType != IDREF_TYPE:
            raise AssociationServiceError('Unexpected type received \n %s' % str(object_reference), object_reference.ResponseCodes.BAD_REQUEST)


        q = store.Query()
        # Get only the latest version of the association!
        q.add_predicate_gt(BRANCH_NAME,'')

        q.add_predicate_eq(OBJECT_KEY, object_reference.key)

        rows = yield self.index_store.query(q)


        list_of_associations = yield self.message_client.create_instance(QUERY_RESULT_TYPE)

        # Make a place to store the branches found for each association
        repo_branches={}

        for key, row in rows.items():

            branches = repo_branches.get(row[REPOSITORY_KEY],None)
            if branches is None:
                branches = set()
                repo_branches[row[REPOSITORY_KEY]] = branches

            if row[BRANCH_NAME] in branches:
                raise NotImplementedError('Divergent state in an association is not yet supported')
            else:
                branches.add(row[BRANCH_NAME])

            if object_reference.IsFieldSet('branch') is True:
                if  True and row[OBJECT_BRANCH] == object_reference.branch:
                    pass

                else:
                    # Get the objects commits and check in parents!
                    raise NotImplementedError('Branches in an association are not yet supported')


            link = list_of_associations.idrefs.add()

            idref= list_of_associations.CreateObject(IDREF_TYPE)
            idref.key = row[REPOSITORY_KEY]
            idref.branch = row[BRANCH_NAME]

            link.SetLink(idref)

        yield self.reply_ok(msg, list_of_associations)



    @defer.inlineCallbacks
    def op_subject_associations(self, subject_reference, headers, msg):
        """
        @see AssociationServiceClient.subject_associations
        """
        log.info('op_subject_associations: ')

        if subject_reference.MessageType != IDREF_TYPE:
            raise AssociationServiceError('Unexpected type received \n %s' % str(subject_reference), subject_reference.ResponseCodes.BAD_REQUEST)

        q = store.Query()
        # Get only the latest version of the association!
        q.add_predicate_gt(BRANCH_NAME,'')

        q.add_predicate_eq(SUBJECT_KEY, subject_reference.key)

        rows = yield self.index_store.query(q)

        list_of_associations = yield self.message_client.create_instance(QUERY_RESULT_TYPE)

        # Make a place to store the branches found for each association
        repo_branches={}

        for key, row in rows.items():

            branches = repo_branches.get(row[REPOSITORY_KEY],None)
            if branches is None:
                branches = set()
                repo_branches[row[REPOSITORY_KEY]] = branches

            if row[BRANCH_NAME] in branches:
                raise NotImplementedError('Divergent state in an association is not yet supported')
            else:
                branches.add(row[BRANCH_NAME])

            if subject_reference.IsFieldSet('branch') is True:

                if row[SUBJECT_BRANCH] == subject_reference.branch:
                    pass

                else:
                    # Get the objects commits and check in parents!
                    raise NotImplementedError('Branches in an association are not yet supported')


            link = list_of_associations.idrefs.add()

            idref= list_of_associations.CreateObject(IDREF_TYPE)
            idref.key = row[REPOSITORY_KEY]
            idref.branch = row[BRANCH_NAME]

            link.SetLink(idref)

        yield self.reply_ok(msg, list_of_associations)
        log.info("/op_subject_associations")


    @defer.inlineCallbacks
    def op_get_association(self, association_query, headers, msg):
        log.info('op_get_association: ')

        rows = yield self._get_association(association_query)

        if len(rows) == 1:

            key, row = rows.popitem()
            response = yield self.message_client.create_instance(IDREF_TYPE)
            response.key = row[REPOSITORY_KEY]
            response.branch = row[BRANCH_NAME]

        elif len(rows)==0:

            raise AssociationServiceError('No association found for the specified triple!', association_query.ResponseCodes.NOT_FOUND)

        else:

            raise AssociationServiceError('More than one association found for the specified triple!', association_query.ResponseCodes.BAD_REQUEST)


        yield self.reply_ok(msg, response)


    @defer.inlineCallbacks
    def op_association_exists(self, association_query, headers, msg):
        """
        @see AssociationServiceClient.association_exists
        """

        log.info('op_association_exists: ')

        rows = yield self._get_association(association_query)

        response = yield self.message_client.create_instance(MessageContentTypeID=BOOL_MSG_TYPE)
        if not rows:
            response.result = False
        elif len(rows)==1:
            response.result = True
        else:
            raise AssociationServiceError('More than one association found for the specified triple!', association_query.ResponseCodes.BAD_REQUEST)


        yield self.reply_ok(msg, response)




    def _get_association(self, association_query):

        if association_query.MessageType != ASSOCIATION_QUERY_MSG_TYPE:
            raise AssociationServiceError('Unexpected type received \n %s' % str(association_query), association_query.ResponseCodes.BAD_REQUEST)

        q = store.Query()
        # Get only the latest version of the association!
        q.add_predicate_gt(BRANCH_NAME,'')

        q.add_predicate_eq(SUBJECT_KEY, association_query.subject.key)

        q.add_predicate_eq(PREDICATE_KEY, association_query.predicate.key)

        q.add_predicate_eq(OBJECT_KEY, association_query.object.key)

        return self.index_store.query(q)


    @defer.inlineCallbacks
    def op_get_associations(self, association_query, headers, msg):
        """
        @see AssociationServiceClient.get_associations
        """
        log.info('op_get_association: ')

        if association_query.MessageType != ASSOCIATION_QUERY_MSG_TYPE:
            raise AssociationServiceError('Unexpected type received \n %s' % str(association_query), association_query.ResponseCodes.BAD_REQUEST)

        q = store.Query()
        # Get only the latest version of the association!
        q.add_predicate_gt(BRANCH_NAME,'')

        if association_query.IsFieldSet('subject'):
            q.add_predicate_eq(SUBJECT_KEY, association_query.subject.key)

        if association_query.IsFieldSet('predicate'):
            q.add_predicate_eq(PREDICATE_KEY, association_query.predicate.key)

        if association_query.IsFieldSet('object'):
            q.add_predicate_eq(OBJECT_KEY, association_query.object.key)

        rows = yield self.index_store.query(q)

        response = yield self.message_client.create_instance(QUERY_RESULT_TYPE)

        for key, row in rows.iteritems():
            
            link = response.idrefs.add()

            idref= response.CreateObject(IDREF_TYPE)
            idref.key = row[REPOSITORY_KEY]
            idref.branch = row[BRANCH_NAME]

            link.SetLink(idref)

        yield self.reply_ok(msg, response)

    def association_query_from_request(self, asc_query):
        q = store.Query()
        # Get only the latest version of the association!
        q.add_predicate_gt(BRANCH_NAME,'')

        if 'subject' in asc_query:      q.add_predicate_eq(SUBJECT_KEY, asc_query['subject'])
        if 'predicate' in asc_query:    q.add_predicate_eq(PREDICATE_KEY, asc_query['predicate'])
        if 'object' in asc_query:       q.add_predicate_eq(OBJECT_KEY, asc_query['object'])

        return self.index_store.query(q)

    @defer.inlineCallbacks
    def op_get_associations_map(self, asc_query, headers, msg):
        """
        @see AssociationServiceClient.get_associations
        """
        log.info('op_get_associations_map: ')

        rows = yield self.association_query_from_request(asc_query)

        role_map = dict((row[SUBJECT_KEY], row[OBJECT_KEY]) for key,row in rows.iteritems())
        yield self.reply_ok(msg, role_map)

    @defer.inlineCallbacks
    def op_get_associations_list(self, asc_query, headers, msg):
        """
        @see AssociationServiceClient.get_associations
        """
        log.info('op_get_associations_list: ')

        rows = yield self.association_query_from_request(asc_query)

        role_list = [{'id': row[REPOSITORY_KEY], 'user_id': row[SUBJECT_KEY], 'role_id': row[OBJECT_KEY]}
                      for key,row in rows.iteritems()]
        yield self.reply_ok(msg, role_list)


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
        """
        @brief Find the subjects which have associations including the given predicate object pairs.
        Example Pairs: TypeOf - Dataset, LifeCycleState - Active, Owner - John Doe
            Would return all active dataset resources owned by John Doe
        @param params msg, GPB 15/1, a Predicate Object query message
        @retval Query Results GPB 22/1
        @GPB{Input,15,1}
        @GPB{Returns,22,1}
        """
        yield self._check_init()
        
        (content, headers, msg) = yield self.rpc_send('get_subjects', msg)
        
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_objects(self, msg):
        """
        @brief Find the objects which have associations including the given subject predicate pairs.
        @param params msg, GPB 16/1, a Subject Predicate query message
        @retval Query Results GPB 22/1
        @GPB{Input,16,1}
        @GPB{Returns,22,1}
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('get_objects', msg)

        defer.returnValue(content)


    @defer.inlineCallbacks
    def get_subject_associations(self, msg):
        """
        @brief Get all the associations of a given subject
        @param params msg, GPB 4/1, an IDRef for the subject in question
        @retval Query Results GPB 22/1
        @GPB{Input,4,1}
        @GPB{Returns,22,1}
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('subject_associations', msg)

        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_object_associations(self, msg):
        """
        @brief Get all the associations of a given object
        @param params msg, GPB 4/1, an IDRef for the object in question
        @retval Query Results GPB 22/1
        @GPB{Input,14,1}
        @GPB{Returns,22,1}
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('object_associations', msg)

        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_association(self, msg):
        """
        @brief Get the identity of the association between these objects
        @param params msg, GPB 27/1, an association query message with IDrefs for each of the subject, predicate and object
        @retval IdRef of an association GPB 4/1
        @GPB{Input,27,1}
        @GPB{Returns,4,1}
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('get_association', msg)

        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_associations(self, msg):
        """
        @brief Get the associations between any of subject, predicate and object. Becareful - you can ask very big questions with this method!
        @param params msg, GPB 27/1, an association query message with IDrefs for each of the subject, predicate and object
        @retval Query Results GPB 22/1
        @GPB{Input,27,1}
        @GPB{Returns,22,1}
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('get_associations', msg)

        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_associations_map(self, msg):
        """
        @brief Get the associations between any of subject, predicate and object. Becareful - you can ask very big questions with this method!
        @param params msg, a dictionary with keys for each of the subject, predicate and object
        @retval Query Results as a dict
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('get_associations_map', msg)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_associations_list(self, msg):
        """
        @brief Get the associations between any of subject, predicate and object. Becareful - you can ask very big questions with this method!
        @param params msg, a dictionary with keys for each of the subject, predicate and object
        @retval Query Results as a list
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('get_associations_list', msg)
        defer.returnValue(content)


    @defer.inlineCallbacks
    def association_exists(self, msg):
        """
        @brief Get the identity of the association between these objects
        @param params msg, GPB 27/1, an association query message with IDrefs for each of the subject, predicate and object
        @retval Boolen Result Message GPB 22/1
        @GPB{Input,27,1}
        @GPB{Returns,30,1}
        """
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('association_exists', msg)

        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_star(self, msg):
        """
        @brief Get the intersection of two queries - one a set of subject/predicate pairs, one a set of predicate/object pairs.
        @param params msg, GPB 28/1, an association get star query message with subject_pairs and object_pairs.
        @retval Query Result Message GPB 22/1
        @GPB{Input,28,1}
        @GPB{Returns,30,1}
        """
        yield self._check_init()

        content, _, _ = yield self.rpc_send('get_star', msg)
        defer.returnValue(content)

# Spawn of the process using the module name
factory = ProcessFactory(AssociationService)


