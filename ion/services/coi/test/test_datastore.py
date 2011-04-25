#!/usr/bin/env python

"""
@file ion/services/coi/test/test_datastore.py
@author David Stuebe
@author Matt Rodriguez
"""
import base64
from ion.core.object.gpb_wrapper import CDM_ARRAY_FLOAT32_TYPE

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core import ioninit
CONF = ioninit.config(__name__)

from ion.util.itv_decorator import itv

from ion.util import procutils as pu
from ion.test.iontest import IonTestCase

from ion.core.object import object_utils
from ion.core.object import workbench

from ion.core.data import cassandra_bootstrap
from ion.core.data import storage_configuration_utility

from ion.core.data.storage_configuration_utility import COMMIT_INDEXED_COLUMNS
from ion.core.data.storage_configuration_utility import BLOB_CACHE, COMMIT_CACHE, PERSISTENT_ARCHIVE

from telephus.cassandra.ttypes import InvalidRequestException

from ion.services.coi.datastore import ION_DATASETS_CFG, PRELOAD_CFG, ID_CFG, DataStoreClient
# Pick three to test existence
from ion.services.coi.datastore_bootstrap.ion_preload_config import HAS_A_ID, DATASET_RESOURCE_TYPE_ID, ROOT_USER_ID, NAME_CFG, CONTENT_ARGS_CFG, PREDICATE_CFG

from ion.services.coi.datastore_bootstrap.ion_preload_config import ION_DATASETS, ION_PREDICATES, ION_RESOURCE_TYPES, ION_IDENTITIES, ION_AIS_RESOURCES_CFG, ION_AIS_RESOURCES, SAMPLE_PROFILE_DATASET_ID

from ion.core.object.workbench import REQUEST_COMMIT_BLOBS_MESSAGE_TYPE, BLOBS_MESSAGE_TYPE
from ion.core.object.gpb_wrapper import CDM_ARRAY_FLOAT32_TYPE, CDM_ATTRIBUTE_TYPE, StructureElement

person_type = object_utils.create_type_identifier(object_id=20001, version=1)
addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
addressbook_type = object_utils.create_type_identifier(object_id=20002, version=1)
association_type = object_utils.create_type_identifier(object_id=13, version=1)


class DataStoreTest(IonTestCase):
    """
    Testing Datastore service.
    """

    services = [
            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{PRELOAD_CFG:{ION_DATASETS_CFG:True, ION_AIS_RESOURCES_CFG:True}}
                },
            {'name':'workbench_test1',
             'module':'ion.core.object.test.test_workbench',
             'class':'WorkBenchProcess',
             'spawnargs':{'proc-name':'wb1'}
                },
        ]


    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        yield self.setup_services()

    @defer.inlineCallbacks
    def setup_services(self):

        self.sup = yield self._spawn_processes(self.services)


        child_ds1 = yield self.sup.get_child_id('ds1')
        log.debug('Process ID:' + str(child_ds1))
        self.ds1 = self._get_procinstance(child_ds1)


        child_proc1 = yield self.sup.get_child_id('workbench_test1')
        log.info('Process ID:' + str(child_proc1))
        workbench_process1 = self._get_procinstance(child_proc1)
        self.wb1 = workbench_process1

        repo = workbench_process1.workbench.create_repository(addresslink_type)
        ab=repo.root_object

        p = repo.create_object(person_type)
        p.name='David'
        p.id = 5
        p.email = 'd@s.com'
        ph = p.phone.add()
        ph.type = p.PhoneType.WORK
        ph.number = '123 456 7890'

        ab.owner = p

        ab.person.add()
        ab.person[0] = p

        p = repo.create_object(person_type)
        p.name='John'
        p.id = 222
        p.email = 'd222@s.com'
        ph = p.phone.add()
        ph.type = p.PhoneType.WORK
        ph.number = '321 456 7890'

        ab.person.add()
        ab.person[1] = p

        ab.title='Datastore Addressbook'

        repo.commit()

        self.repo_key = repo.repository_key


    def test_instantiate(self):
        pass
    
    @defer.inlineCallbacks
    def tearDown(self):
        log.info('Tearing Down Test Container')

        yield self._shutdown_processes()
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_push(self):

        log.info('DataStore1 Push addressbook to DataStore1')

        result = yield self.wb1.workbench.push_by_name('datastore',self.repo_key)

        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        log.info('DataStore1 Push addressbook to DataStore1: complete')


    @defer.inlineCallbacks
    def test_existence(self):

        # Test preloaded stuff:
        is_there = yield self.ds1.workbench.test_existence(HAS_A_ID)
        self.assertEqual(is_there,True)

        is_there = yield self.ds1.workbench.test_existence(DATASET_RESOURCE_TYPE_ID)
        self.assertEqual(is_there,True)

        is_there = yield self.ds1.workbench.test_existence(ROOT_USER_ID)
        self.assertEqual(is_there,True)

        # Test the repo we just made but have not pushed
        is_there = yield self.ds1.workbench.test_existence(self.repo_key)
        self.assertEqual(is_there,False)

        log.info('DataStore1 Push addressbook to DataStore1')

        result = yield self.wb1.workbench.push_by_name('datastore',self.repo_key)

        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        log.info('DataStore1 Push addressbook to DataStore1: complete')

        # Now test that after the push it exists!
        is_there = yield self.ds1.workbench.test_existence(self.repo_key)
        self.assertEqual(is_there,True)


    def test_pull_invalid(self):

        self.failUnlessFailure(self.wb1.workbench.pull('datastore', 'foobar'), workbench.WorkBenchError)

    @defer.inlineCallbacks
    def test_put_blobs(self):
        msg = yield self.wb1.message_client.create_instance(BLOBS_MESSAGE_TYPE)

        sentkeyset = set()
        for key,val in self.wb1.workbench.get_repository(self.repo_key).index_hash.iteritems():
            link = msg.blob_elements.add()
            obj = msg.Repository._wrap_message_object(val._element)

            link.SetLink(obj)
            sentkeyset.add(key)

        # get list of keys ds1 has currently
        origkeyset = set(self.ds1.workbench._blob_store.kvs.keys())

        dsc = DataStoreClient()

        yield dsc.put_blobs(msg)

        # examine ds1
        postkeyset = set(self.ds1.workbench._blob_store.kvs.keys())

        self.failUnlessEquals(len(origkeyset) + len(sentkeyset), len(postkeyset))

        for key in sentkeyset:
            self.failIf(key in origkeyset)
            self.failUnless(key in sentkeyset)

    @defer.inlineCallbacks
    def test_checkout(self):
        result = yield self.wb1.workbench.pull('datastore', SAMPLE_PROFILE_DATASET_ID,excluded_types=[])
        repo = self.wb1.workbench.get_repository(SAMPLE_PROFILE_DATASET_ID)

        yield repo.checkout('master',excluded_types=[])

        commit = repo._current_branch.commitrefs[0]
        key = commit.GetLink('objectroot').key

        msg = yield self.wb1.message_client.create_instance(REQUEST_COMMIT_BLOBS_MESSAGE_TYPE)
        msg.commit_root_object = key

        dsc = DataStoreClient()

        content = yield dsc.checkout(msg)

        # length of the blobs we got back here should be one less than in the repo (due to objectroot not being in response, i think?)
        self.failUnlessEquals(len(content.blob_elements), len(repo.index_hash) - 1)

        for blob in content.blob_elements:
            self.failUnless(blob.key in repo.index_hash.keys())

        # load em into this object
        for blob in content.blob_elements:
            element = StructureElement(blob.GPBMessage)
            content.Repository.index_hash[element.key] = element

        element = content.Repository.index_hash[key]
        objroot = content.Repository._load_element(element)
        content.Repository.load_links(objroot, [])

        # a list of all keys in the full dataset with no filtering. we use this below to compute difference
        # lists.
        ckeys = [x.key for x in content.blob_elements]

        def does_blob_have_parent_of_type(rep, curblob, parentblobs, targetkey, targetgpbtype, counter=0):
            """
            Helper method to find if a target node (by key) is or has a parent of targetgpbtype.

            Performs a recursive DFS from the root node (as a StructureElement), maintaining a list of
            all parents to that recursive call. When the target key is found, those parents are checked to
            ensure that one of them is in the targetgpbtype list.

            @param  rep             The repository to use to look up nodes by key.
            @param  curblob         The current node we're looking at, as a StructureElement.
            @param  parentblobs     A list of parent nodes we have visited, as StructureElements. The first
                                    item in this list is the most recent parent, aka the immediate parent of
                                    curblob.
            @param  targetkey       The key of the node we're looking for.
            @param  targetgpbtype   A list of target GPB types we want to make sure the targetkey is a child
                                    of.
            @param  counter         Debug parameter for recursion depth.

            @returns    A boolean if the target node is parented by one of the targetgpbtypes. None if the key
                        was never found.
            """

            if curblob.key == targetkey:

                if curblob.type in targetgpbtype:
                    return True

                for ttype in targetgpbtype:
                    if ttype in [x.type for x in parentblobs]:
                        return True

                return False

            newchilds = []
            for x in curblob.ChildLinks:

                # @TODO: why is this sometimes a string?
                if isinstance(x, str):
                    newchilds.append(x)
                else:
                    newchilds.append(x.key)

            for childkey in newchilds:
                childblob = rep.index_hash[childkey]

                retval = does_blob_have_parent_of_type(rep, childblob, [curblob] + parentblobs, targetkey, targetgpbtype, counter+1)
                if retval is not None:
                    return retval

            return None

        # run a checkout for every item in this list. the items in this list signify excluding items of the types
        # in the lists.
        
        for excludetypes in [[CDM_ARRAY_FLOAT32_TYPE], [CDM_ARRAY_FLOAT32_TYPE, CDM_ATTRIBUTE_TYPE]]:

            # checkout with multiple filtering
            msg = yield self.wb1.message_client.create_instance(REQUEST_COMMIT_BLOBS_MESSAGE_TYPE)
            msg.commit_root_object = key

            for extype in excludetypes:
                exobj = msg.excluded_types.add()
                exobj.object_id = extype.object_id
                exobj.version = extype.version

            content4 = yield dsc.checkout(msg)

            # should be less than the previous checkout
            self.failUnless(len(content4.blob_elements) < len(repo.index_hash) - 1)

            # make sure all exist
            for blob in content4.blob_elements:
                self.failUnless(blob.key in repo.index_hash.keys())

            # make sure we didn't get any items of excludetypes
            objids = set([x.type.object_id for x in content4.blob_elements])
            for extype in excludetypes:
                self.failIf(extype.object_id in objids)

            # diff content4's keys against unfiltered keys
            c3keys      = [x.key for x in content4.blob_elements]
            difflist    = [x for x in ckeys if not x in c3keys]

            # mathematically this has to be true...
            self.failUnlessEquals(len(difflist) + len(c3keys), len(ckeys))
    
            # load all blobs into the index hash of the message
            for blob in content4.blob_elements:
                element = StructureElement(blob.GPBMessage)
                content4.Repository.index_hash[element.key] = element

            # make sure all excluded nodes are or are parented by one of the two excluded types.
            for blobkey in difflist:
                retval = does_blob_have_parent_of_type(repo, repo.index_hash[objroot.MyId], [], blobkey, excludetypes)
                self.failUnless(retval)

    @defer.inlineCallbacks
    def test_push_clear_pull(self):

        log.info('DataStore1 Push addressbook to DataStore1')

        result = yield self.wb1.workbench.push_by_name('datastore',self.repo_key)

        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        log.info('DataStore1 Push addressbook to DataStore1: complete')

        

        self.wb1.workbench.clear_non_persistent()

        self.ds1.workbench.clear_non_persistent()


        repo = self.wb1.workbench.get_repository(self.repo_key)
        self.assertEqual(repo,None)

        result = yield self.wb1.workbench.pull('datastore',self.repo_key)

        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)


        # use the value - the key of the first to get it from the workbench on the 2nd
        repo = self.wb1.workbench.get_repository(self.repo_key)

        ab = yield repo.checkout('master')

        self.assertEqual(ab.title,'Datastore Addressbook')

    @defer.inlineCallbacks
    def test_push_clear_pull_again(self):

        log.info('DataStore1 Push addressbook to DataStore1')

        result = yield self.wb1.workbench.push_by_name('datastore',self.repo_key)

        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        log.info('DataStore1 Push addressbook to DataStore1: complete')



        self.wb1.workbench.clear_non_persistent()

        self.ds1.workbench.clear_non_persistent()


        repo = self.wb1.workbench.get_repository(self.repo_key)
        self.assertEqual(repo,None)

        result = yield self.wb1.workbench.pull('datastore',self.repo_key)

        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)


        # use the value - the key of the first to get it from the workbench on the 2nd
        repo = self.wb1.workbench.get_repository(self.repo_key)

        ab = yield repo.checkout('master')

        self.assertEqual(ab.title,'Datastore Addressbook')

        
        self.wb1.workbench.clear_non_persistent()

        self.ds1.workbench.clear_non_persistent()


        repo = self.wb1.workbench.get_repository(self.repo_key)
        self.assertEqual(repo,None)

        result = yield self.wb1.workbench.pull('datastore',self.repo_key)

        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)


        # use the value - the key of the first to get it from the workbench on the 2nd
        repo = self.wb1.workbench.get_repository(self.repo_key)

        ab = yield repo.checkout('master')

        self.assertEqual(ab.title,'Datastore Addressbook')


    @defer.inlineCallbacks
    def test_push_clear_pull_branched(self):


        repo = self.wb1.workbench.get_repository(self.repo_key)

        branch1_key = repo.current_branch_key()
        branch2_key = repo.branch()

        # Delete the reference
        del repo

        log.info('DataStore1 Push addressbook to DataStore1')
        result = yield self.wb1.workbench.push_by_name('datastore',self.repo_key)

        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        log.info('DataStore1 Push addressbook to DataStore1: complete')



        self.wb1.workbench.clear_non_persistent()

        self.ds1.workbench.clear_non_persistent()


        result = yield self.wb1.workbench.pull('datastore',self.repo_key)

        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)


        # use the value - the key of the first to get it from the workbench on the 2nd
        repo = self.wb1.workbench.get_repository(self.repo_key)

        # Check that we got back both branches!
        ab = yield repo.checkout(branchname=branch1_key)

        ab = yield repo.checkout(branchname=branch2_key)



        self.assertEqual(ab.title,'Datastore Addressbook')






    @defer.inlineCallbacks
    def test_push_clear_pull_many(self):

        number = 5
        key_list = self.create_many_repos(number)

        log.info('DataStore1 Push addressbook to DataStore1')

        result = yield self.wb1.workbench.push_by_name('datastore',key_list)

        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        log.info('DataStore1 Push addressbook to DataStore1: complete')


        # Clear all workbenchs
        self.wb1.workbench.clear_non_persistent()

        self.ds1.workbench.clear_non_persistent()



        for n in range(number):
            key = key_list[n]

            result = yield self.wb1.workbench.pull('datastore',key)

            self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)


            # use the value - the key of the first to get it from the workbench on the 2nd
            repo = self.wb1.workbench.get_repository(key)

            ab = yield repo.checkout('master')

            self.assertEqual(ab.title,'WB TITLE: %s' % str(n))


    def create_many_repos(self,number):

        key_list =[]
        for n in range(number):
            repo = self.wb1.workbench.create_repository(addresslink_type)
            repo.root_object.title = 'WB TITLE: %s' % str(n)
            key_list.append(repo.repository_key)
            repo.commit('repo %s commit' % str(n))

        return key_list

    @defer.inlineCallbacks
    def test_checkout_defaults(self):

        defaults={}
        defaults.update(ION_RESOURCE_TYPES)
        defaults.update(ION_DATASETS)
        defaults.update(ION_IDENTITIES)
        defaults.update(ION_AIS_RESOURCES)

        for key, value in defaults.items():

            repo_name = value[ID_CFG]

            c_args = value.get(CONTENT_ARGS_CFG)
            if c_args and not c_args.get('filename'):
                break



            result = yield self.wb1.workbench.pull('datastore',repo_name)
            self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

            repo = self.wb1.workbench.get_repository(repo_name)

            # Check that we got back both branches!
            default_obj = yield repo.checkout(branchname='master')

            self.assertEqual(default_obj.name, value[NAME_CFG])


        for key, value in ION_PREDICATES.items():

            repo_name = value[ID_CFG]

            result = yield self.wb1.workbench.pull('datastore',repo_name)
            self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

            repo = self.wb1.workbench.get_repository(repo_name)

            # Check that we got back both branches!
            default_obj = yield repo.checkout(branchname='master')

            self.assertEqual(default_obj.word, value[PREDICATE_CFG])



class CassandraBackedDataStoreTest(DataStoreTest):


    username = CONF.getValue('cassandra_username', None)
    password = CONF.getValue('cassandra_password', None)


    services=[]
    services.append(
        {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
         'spawnargs':{COMMIT_CACHE:'ion.core.data.cassandra_bootstrap.CassandraIndexedStoreBootstrap',
                      BLOB_CACHE:'ion.core.data.cassandra_bootstrap.CassandraStoreBootstrap',
                      PRELOAD_CFG:{ION_DATASETS_CFG:True, ION_AIS_RESOURCES_CFG:True},
                       }
                })

    services.append(DataStoreTest.services[1])


    @itv(CONF)
    @defer.inlineCallbacks
    def setUp(self):

        yield self._start_container()

        storage_conf = storage_configuration_utility.get_cassandra_configuration()

        self.keyspace = storage_conf[PERSISTENT_ARCHIVE]["name"]

        # Use a test harness cassandra client to set it up the way we want it for the test and tear it down
        test_harness = cassandra_bootstrap.CassandraSchemaProvider(self.username, self.password, storage_conf, error_if_existing=False)

        test_harness.connect()

        self.test_harness = test_harness


        try:
            yield self.test_harness.client.system_drop_keyspace(self.keyspace)
        except InvalidRequestException, ire:
            log.info('No Keyspace to remove in setup: ' + str(ire))

        yield test_harness.run_cassandra_config()


        yield DataStoreTest.setup_services(self)


    @defer.inlineCallbacks
    def tearDown(self):

        try:
            yield self.test_harness.client.system_drop_keyspace(self.keyspace)
        except InvalidRequestException, ire:
            log.info('No Keyspace to remove in teardown: ' + str(ire))


        self.test_harness.disconnect()

        yield DataStoreTest.tearDown(self)




