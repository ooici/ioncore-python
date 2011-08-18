#!/usr/bin/env python

"""
@file ion/services/coi/test/test_datastore.py
@author David Stuebe
@author David Foster
@author Matt Rodriguez
"""
from twisted.trial import unittest
from ion.core.exception import ReceivedContainerError, ReceivedApplicationError
from ion.core.messaging.receiver import Receiver, WorkerReceiver
from ion.core.process.process import Process
from ion.core.object.object_utils import ARRAY_STRUCTURE_TYPE, CDM_ARRAY_FLOAT64_TYPE, CDM_ARRAY_FLOAT32_TYPE, CDM_ARRAY_FLOAT32_TYPE, CDM_ATTRIBUTE_TYPE

import ion.util.ionlog

log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core import ioninit
CONF = ioninit.config(__name__)

from ion.util import procutils as pu
from ion.test.iontest import IonTestCase

from ion.core.object import object_utils
from ion.core.object import workbench

from ion.core.data import cassandra_bootstrap
from ion.core.data import storage_configuration_utility

from ion.core.data.storage_configuration_utility import COMMIT_INDEXED_COLUMNS
from ion.core.data.storage_configuration_utility import BLOB_CACHE, COMMIT_CACHE, PERSISTENT_ARCHIVE

from telephus.cassandra.ttypes import InvalidRequestException

from ion.services.coi.datastore import ION_DATASETS_CFG, PRELOAD_CFG, ID_CFG, DataStoreClient, CDM_BOUNDED_ARRAY_TYPE
# Pick three to test existence
from ion.services.coi.datastore_bootstrap.ion_preload_config import HAS_A_ID, DATASET_RESOURCE_TYPE_ID, ROOT_USER_ID, NAME_CFG, CONTENT_ARGS_CFG, PREDICATE_CFG, ION_RESOURCE_TYPES_CFG, ION_PREDICATES_CFG, ION_IDENTITIES_CFG, SAMPLE_PROFILE_DATA_SOURCE_ID

from ion.services.coi.datastore_bootstrap.ion_preload_config import ION_DATASETS, ION_PREDICATES, ION_RESOURCE_TYPES, ION_IDENTITIES, ION_AIS_RESOURCES_CFG, ION_AIS_RESOURCES, SAMPLE_PROFILE_DATASET_ID, HAS_A_ID

from ion.core.object.workbench import REQUEST_COMMIT_BLOBS_MESSAGE_TYPE, BLOBS_MESSAGE_TYPE, IDREF_TYPE, GET_OBJECT_REQUEST_MESSAGE_TYPE, GPBTYPE_TYPE, DATA_REQUEST_MESSAGE_TYPE, GET_LCS_REQUEST_MESSAGE_TYPE
from ion.core.object.gpb_wrapper import StructureElement

person_type = object_utils.create_type_identifier(object_id=20001, version=1)
addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
addressbook_type = object_utils.create_type_identifier(object_id=20002, version=1)
association_type = object_utils.create_type_identifier(object_id=13, version=1)

OPAQUE_ARRAY_TYPE = object_utils.create_type_identifier(object_id=10016, version=1)




@defer.inlineCallbacks
def create_large_object(wb):

    rand = open('/dev/urandom','r')

    repo = yield wb.create_repository(OPAQUE_ARRAY_TYPE)
    MB = 1024 * 124
    repo.root_object.value.extend(rand.read(2 *MB))

    repo.commit('Commit before send...')

    log.info('Repository size: %d bytes, array len %d' % (repo.__sizeof__(), len(repo.root_object.value)))

    rand.close()


    defer.returnValue(repo)




class DataStoreTest(IonTestCase):
    """
    Testing Datastore service.
    """

    # Number or repetitions for pull large object test
    repetitions = 3

    # when setting high repetitions, the timeout must be increased.
    #timeout = 600

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

        yield self.failUnlessFailure(self.wb1.workbench.pull('datastore', 'foobar'), workbench.WorkBenchError)

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
    def test_get_object(self):
        p = Process(proc_name='test_anon')
        yield p.spawn()

        msg = yield p.message_client.create_instance(GET_OBJECT_REQUEST_MESSAGE_TYPE)

        idref = msg.CreateObject(IDREF_TYPE)
        idref.key = SAMPLE_PROFILE_DATASET_ID

        msg.object_id = idref

        dsc = DataStoreClient(proc=p)
        obj = yield dsc.get_object(msg)

        # test object, make sure it is what we expect
        self.failUnlessEquals(obj.retrieved_object.ObjectType.object_id, 1102)

        # daf: this is only for pulling out keys to be able to test extract_data, leaving it in for ease
        #for k,v in obj.Repository.index_hash.iteritems():
        #    if v.type.object_id == 10025:
        #        print "an array!", base64.encodestring(k)
        #self.failUnless(0)
        
        # test excluded types!
        msg = yield p.message_client.create_instance(GET_OBJECT_REQUEST_MESSAGE_TYPE)

        idref = msg.CreateObject(IDREF_TYPE)
        idref.key = SAMPLE_PROFILE_DATASET_ID

        msg.object_id = idref

        exobj = msg.excluded_object_types.add()
        typer = msg.CreateObject(GPBTYPE_TYPE)

        typer.object_id = CDM_ARRAY_FLOAT32_TYPE.object_id
        typer.version = CDM_ARRAY_FLOAT32_TYPE.version

        exobj.SetLink(typer)

        obj2 = yield dsc.get_object(msg)

        # is obj2 the right object type?
        self.failUnlessEquals(obj.retrieved_object.ObjectType.object_id, 1102)

        # is the index_hash of obj2 less than obj due to exclusions?
        self.failUnless(len(obj2.Repository.index_hash) < len(obj.Repository.index_hash))

        # make sure we see the correct things in our repo's excluded types
        self.failUnless(CDM_ARRAY_FLOAT32_TYPE in obj2.Repository.excluded_types)

    @defer.inlineCallbacks
    def test_get_object_with_treeish(self):
        """
        Since SAMPLE_PROFILE_DATASET_ID does not have a history that we know about, we'll ask for something
        outlandish back in its history to make sure it is parsing/trying to resolve the treeish.  Actual
        treeish tests are in the Repository's tests.
        """
        p = Process(proc_name='test_anon')
        yield p.spawn()

        msg = yield p.message_client.create_instance(GET_OBJECT_REQUEST_MESSAGE_TYPE)

        idref = msg.CreateObject(IDREF_TYPE)
        idref.key = SAMPLE_PROFILE_DATASET_ID
        idref.treeish = "~20"

        msg.object_id = idref

        dsc = DataStoreClient(proc=p)

        fo = yield self.failUnlessFailure(dsc.get_object(msg), ReceivedContainerError)
        self.failUnless('resolve treeish' in fo.msg_content.MessageResponseBody)    # @TODO: this will not survive l10n/i18n

    @defer.inlineCallbacks
    def test_push_clear_pull(self):

        log.info('DataStore1 Push addressbook to DataStore1')

        result = yield self.wb1.workbench.push_by_name('datastore',self.repo_key)

        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        log.info('DataStore1 Push addressbook to DataStore1: complete')


        self.wb1.workbench.clear()

        self.ds1.workbench.clear()


        repo = self.wb1.workbench.get_repository(self.repo_key)
        self.assertEqual(repo,None)

        repo = self.ds1.workbench.get_repository(self.repo_key)
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



        self.wb1.workbench.clear()

        self.ds1.workbench.clear()


        repo = self.wb1.workbench.get_repository(self.repo_key)
        self.assertEqual(repo,None)

        result = yield self.wb1.workbench.pull('datastore',self.repo_key)

        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)


        # use the value - the key of the first to get it from the workbench on the 2nd
        repo = self.wb1.workbench.get_repository(self.repo_key)

        ab = yield repo.checkout('master')

        self.assertEqual(ab.title,'Datastore Addressbook')

        
        self.wb1.workbench.clear()

        self.ds1.workbench.clear()


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



        self.wb1.workbench.clear()

        self.ds1.workbench.clear()


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
        self.wb1.workbench.clear()

        self.ds1.workbench.clear()



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

    @defer.inlineCallbacks
    def test_get_lcs(self):
        p = Process(proc_name='test_anon')
        yield p.spawn()

        #msg = yield p.message_client.create_instance(GET_OBJECT_REQUEST_MESSAGE_TYPE)

        #idref = msg.CreateObject(IDREF_TYPE)
        #idref.key = SAMPLE_PROFILE_DATASET_ID
        #msg.object_id = idref

        dsc = DataStoreClient(proc=p)
        request = yield p.message_client.create_instance(GET_LCS_REQUEST_MESSAGE_TYPE)
        request.keys.append(SAMPLE_PROFILE_DATASET_ID)

        obj = yield dsc.get_lcs(request)

        self.failUnlessEquals(len(obj.key_lcs_pairs), 1)
        self.failUnlessEqual(obj.key_lcs_pairs[0].lcs, obj.key_lcs_pairs[0].LifeCycleState.ACTIVE)

        # now get multiple
        request = yield p.message_client.create_instance(GET_LCS_REQUEST_MESSAGE_TYPE)
        request.keys.append(SAMPLE_PROFILE_DATASET_ID)
        request.keys.append(SAMPLE_PROFILE_DATA_SOURCE_ID)

        obj2 = yield dsc.get_lcs(request)

        self.failUnlessEquals(len(obj2.key_lcs_pairs), 2)
        self.failUnlessEquals([x.lcs for x in obj2.key_lcs_pairs], [obj.key_lcs_pairs[0].LifeCycleState.ACTIVE] * len(obj2.key_lcs_pairs))

    @defer.inlineCallbacks
    def test_get_lcs_invalid_key(self):
        p = Process(proc_name='test_anon')
        yield p.spawn()


        request = yield p.message_client.create_instance(GET_LCS_REQUEST_MESSAGE_TYPE)
        request.keys.append("this key does not exist")

        dsc = DataStoreClient(proc=p)
        lcsdef = dsc.get_lcs(request)

        yield self.failUnlessFailure(lcsdef, ReceivedApplicationError)

    """


    @defer.inlineCallbacks
    def test_pull_object(self):

        repo = yield create_large_object(self.wb1.workbench)

        result = yield self.wb1.workbench.push('datastore',repo)

        self.repo_key = repo.repository_key

        for i in range(self.repetitions):

            log.info("Testing pull loop!!!")

            result = yield self.wb1.workbench.pull('datastore',self.repo_key)

            self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

            self.wb1.workbench.manage_workbench_cache('Test runner context!')

            log.info(pu.print_memory_usage())
            log.info("WB1: %s" % self.wb1.workbench_memory())
            log.info("DS1: %s" % self.ds1.workbench_memory())
            #import objgraph
            #objgraph.show_growth()


    @defer.inlineCallbacks
    def test_get_blobs(self):

        log.info('Starting test_get_blobs')

        wb = self.ds1.workbench

        obj_repo = yield create_large_object(wb)

        log.info('Created large object')


        for i in range(self.repetitions):
            load_repo = yield wb.create_repository(OPAQUE_ARRAY_TYPE)

            blobs = yield wb._get_blobs(load_repo,[obj_repo.commit_head.MyId])

            wb.clear_repository(load_repo)

            log.info("DS1: %s" % self.ds1.workbench_memory())
            mem = yield pu.print_memory_usage()
            log.info(mem)





    @defer.inlineCallbacks
    def test_large_objects(self):


        for i in range(self.repetitions):
            repo = yield create_large_object(self.wb1.workbench)

            result = yield self.wb1.workbench.push('datastore',repo)

            self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

            log.info("WB1: %s" % self.wb1.workbench_memory())
            log.info("DS1: %s" % self.ds1.workbench_memory())
            log.info("Expect memory to grow unless you are using cassandra backend and the cache is full")

            mem = yield pu.print_memory_usage()
            log.info(mem)

            self.wb1.workbench.manage_workbench_cache('Test runner context!')




    @defer.inlineCallbacks
    def test_checkout_a_lot(self):


        for i in range(self.repetitions):
            yield self.test_checkout_defaults()
            self.wb1.workbench.manage_workbench_cache('Test runner context!')

            for key, repo in self.wb1.workbench._repo_cache.iteritems():
                log.info('Repo Name - %s, size - %d, # of blobs - %d' % (key, repo.__sizeof__(), len(repo.index_hash)))

            log.info("WB1: %s" % self.wb1.workbench_memory())
            log.info("DS1: %s" % self.ds1.workbench_memory())

            mem = yield pu.print_memory_usage()
            log.info(mem)

    """

class MulitDataStoreTest(IonTestCase):
    """
    Testing Datastore service.
    """

    preload = { ION_PREDICATES_CFG:False,
                ION_RESOURCE_TYPES_CFG:False,
                ION_IDENTITIES_CFG:False,
                ION_DATASETS_CFG:False,
                ION_AIS_RESOURCES_CFG:False}

    services = [

            {'name':'ds2','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{PRELOAD_CFG:preload}
                },
            {'name':'ds3','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{PRELOAD_CFG:preload}
                },
            {'name':'ds4','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{PRELOAD_CFG:preload}
                },

            # Start this one last to preload...
            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
                }, # The first one does the preload by default

            {'name':'workbench_test1',
             'module':'ion.core.object.test.test_workbench',
             'class':'WorkBenchProcess',
             'spawnargs':{'proc-name':'wb1'}
                },

            {'name':'workbench_test2',
             'module':'ion.core.object.test.test_workbench',
             'class':'WorkBenchProcess',
             'spawnargs':{'proc-name':'wb2'}
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

        child_ds2 = yield self.sup.get_child_id('ds2')
        log.debug('Process ID:' + str(child_ds2))
        self.ds2 = self._get_procinstance(child_ds2)

        child_ds3 = yield self.sup.get_child_id('ds3')
        log.debug('Process ID:' + str(child_ds3))
        self.ds3 = self._get_procinstance(child_ds3)

        child_ds4 = yield self.sup.get_child_id('ds4')
        log.debug('Process ID:' + str(child_ds4))
        self.ds4 = self._get_procinstance(child_ds4)


        child_proc1 = yield self.sup.get_child_id('workbench_test1')
        log.info('Process ID:' + str(child_proc1))
        workbench_process1 = self._get_procinstance(child_proc1)
        self.wb1 = workbench_process1

        child_proc2 = yield self.sup.get_child_id('workbench_test2')
        log.info('Process ID:' + str(child_proc2))
        workbench_process2 = self._get_procinstance(child_proc2)
        self.wb2 = workbench_process2

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
    def test_pull_stuff(self):
        log.info('starting multi pull test...')

        tp = Process()
        yield tp.spawn()

        n = 8
        for i in range(n):
            tp.workbench.clear()
            yield tp.workbench.pull('datastore',HAS_A_ID)

            hasa = tp.workbench.get_repository(HAS_A_ID)
            yield hasa.checkout('master')

            hasa.root_object.word = 'has_a'


    @defer.inlineCallbacks
    def test_push_sync(self):
        log.info('starting multi push test...')

        repo = self.wb1.workbench.get_repository(self.repo_key)

        n=12
        for i in range(n):

            repo.root_object.person[0].id = i

            repo.commit('The %d commit!' % i)

            log.info('Commit #%d and push workbench test object:\n%s' % (i, self.wb1.workbench))
            yield self.wb1.workbench.push('datastore', repo)


        repo1 = yield self.ds1.workbench._resolve_repo_state(self.repo_key, fail_if_not_found=True)
        repo1.checkout('master')

        repo2 = yield self.ds2.workbench._resolve_repo_state(self.repo_key, fail_if_not_found=True)
        repo2.checkout('master')


        repo3 = yield self.ds3.workbench._resolve_repo_state(self.repo_key, fail_if_not_found=True)
        repo3.checkout('master')


        repo4 = yield self.ds4.workbench._resolve_repo_state(self.repo_key, fail_if_not_found=True)
        repo4.checkout('master')

        ### DEBUG....
        #if repo4 != repo:
        #    print 'REPO 4'
        #    print repo4

        #print repo1.log_commits()


        self.assertEqual(repo, repo1)
        self.assertEqual(repo, repo2)
        self.assertEqual(repo, repo3)
        self.assertEqual(repo, repo4)



    @defer.inlineCallbacks
    def test_push_pull(self):
        log.info('starting multi push test...')

        repo = self.wb1.workbench.get_repository(self.repo_key)

        n=12
        for i in range(n):

            repo.root_object.person[0].id = i

            repo.commit('The %d commit!' % i)

            log.info('Commit #%d and push workbench test object:\n%s' % (i, self.wb1.workbench))
            yield self.wb1.workbench.push('datastore', repo)

        tp = Process()
        yield tp.spawn()

        print repo.log_commits()

        for i in range(4):
            tp.workbench.clear()

            log.info('Pull the object back to new process - # %d' % i)
            yield tp.workbench.pull('datastore', self.repo_key)

            repo = tp.workbench.get_repository(self.repo_key)
            repo.checkout('master')

            self.assertEqual(repo.root_object.person[0].id,n-1)


    @defer.inlineCallbacks
    def test_divergence(self):
        log.info('starting multi push test...')

        repo1 = self.wb1.workbench.get_repository(self.repo_key)

        # Initial push to the datastore
        yield self.wb1.workbench.push('datastore', repo1)

        # make some local changes
        repo1.root_object.person[0].id = 1
        repo1.commit('The %d commit!' % 1)


        # Get the repo from the datastore in a different workbench
        yield self.wb2.workbench.pull('datastore', self.repo_key)
        repo2 = self.wb2.workbench.get_repository(self.repo_key)
        repo2.checkout('master')

        # make some local changes
        repo2.root_object.person[0].id = 2
        repo2.commit('The %d commit!' % 2)

        # Both push to the data store!
        yield self.wb1.workbench.push('datastore', repo1)
        yield self.wb2.workbench.push('datastore', repo2)

        # Pick any datastore instance and show that there are two current commit refs on the master branch
        repo3 = yield self.ds3.workbench._resolve_repo_state(self.repo_key, fail_if_not_found=True)

        self.assertEqual(len(repo3.get_branch('master').commitrefs), 2)

        # pull it back and show that the divergence is resolved during checkout
        tp = Process()
        yield tp.spawn()

        yield tp.workbench.pull('datastore', self.repo_key)
        repo = tp.workbench.get_repository(self.repo_key)

        self.assertEqual(len(repo.get_branch('master').commitrefs), 2)

        repo.checkout('master')

        self.assertEqual(len(repo.get_branch('master').commitrefs), 1)

        # The later state wins... for now
        self.assertEqual(repo.root_object.person[0].id,repo2.root_object.person[0].id)




class DataStoreExtractDataTest(IonTestCase):
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

        self.proc = Process(proc_name='ds_extract_proc')
        yield self.proc.spawn()

        yield self.setup_services()
        yield self.setup_array_structure()
        yield self.setup_data_listener()

        self.dsc = DataStoreClient(proc=self.proc)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

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

    @defer.inlineCallbacks
    def setup_array_structure(self):

        # create two arraystructures and push them to the datastore (via putblobs)
        # one is 3d, as a single bounded array
        # the other is 4d as multiple ba's

        repo = self.wb1.workbench.create_repository(ARRAY_STRUCTURE_TYPE)

        # Create the array structure
        content = repo.root_object
        ba1 = yield repo.create_object(CDM_BOUNDED_ARRAY_TYPE)
        arr1 = yield repo.create_object(CDM_ARRAY_FLOAT64_TYPE)

        ba1.bounds.add()
        ba1.bounds[0].origin = 0
        ba1.bounds[0].size = 15

        ba1.bounds.add()
        ba1.bounds[1].origin = 0
        ba1.bounds[1].size = 40

        ba1.bounds.add()
        ba1.bounds[2].origin = 0
        ba1.bounds[2].size = 200

        arr1.value.extend((float(val) for val in xrange(0, 200*40*15)))

        # assemble this array struc
        ba1.ndarray = arr1
        ref = content.bounded_arrays.add(); ref.SetLink(ba1)

        repo.commit()

        self.first_struct_repo_key = repo.repository_key
        self.first_struct_as_key = content.MyId

        # create second array structure
        repo = self.wb1.workbench.create_repository(ARRAY_STRUCTURE_TYPE)

        # Create the array structure
        content = repo.root_object

        bas = []
        arrs = []
        totalintopdim = 20 ** 3
        for x in xrange(4):
            ba = yield repo.create_object(CDM_BOUNDED_ARRAY_TYPE)
            bas.append(ba)

            ba.bounds.add()
            ba.bounds[0].origin = x
            ba.bounds[0].size = 1

            for y in xrange(1, 4):
                ba.bounds.add()
                ba.bounds[y].origin = 0
                ba.bounds[y].size = 20

            arr = yield repo.create_object(CDM_ARRAY_FLOAT64_TYPE)
            arrs.append(arr)

            arr.value.extend((float(val) for val in xrange(x * totalintopdim, (x+1)*totalintopdim)))

            ba.ndarray = arr

            ref = content.bounded_arrays.add()
            ref.SetLink(ba)

        repo.commit()
        
        self.second_struct_repo_key = repo.repository_key
        self.second_struct_as_key = content.MyId

        # send the array structs to the datastore (as putblobs)

        msg = yield self.wb1.message_client.create_instance(BLOBS_MESSAGE_TYPE)

        for repokey in [self.first_struct_repo_key, self.second_struct_repo_key]:
            for key,val in self.wb1.workbench.get_repository(repokey).index_hash.iteritems():
                link = msg.blob_elements.add()
                obj = msg.Repository._wrap_message_object(val._element)
                link.SetLink(obj)

        dsc = DataStoreClient(proc=self.proc)
        yield dsc.put_blobs(msg)

    def setup_data_listener(self):
        '''
        Monkey patches Datastore's workbench's _send_data_chunk method so that we don't have to test via the
        entire messaging stack.
        '''

        self._recv_data = []
        self._def_done = defer.Deferred()   # this is called back in the handler when the "done" message comes through

        # patch up datastore's _send_data_chunk method
        def fake_send_chunk(data_routing_key, chunkmsg):
            self._recv_data.append({'ndarray':      chunkmsg.ndarray.value[:],
                                    'start_index':  chunkmsg.start_index,
                                    'seq_number':   chunkmsg.seq_number,
                                    'seq_max':      chunkmsg.seq_max})
            if chunkmsg.done:
                self._def_done.callback(True)

        self._old_send_chunk = self.ds1.workbench._send_data_chunk
        self.ds1.workbench._send_data_chunk = fake_send_chunk

    @defer.inlineCallbacks
    def test_full_one_ba_with_messaging(self):

        # setup a messaging listener here (self._recv_data and self._def_done exist due to call to setup_data_listener)
        @defer.inlineCallbacks
        def datahandler(data, msg):
            self._recv_data.append({'ndarray': data['content'].ndarray.value[:],
                                    'start_index':data['content'].start_index,
                                    'seq_number':data['content'].seq_number,
                                    'seq_max':data['content'].seq_max})

            #print "DataStoreExtractDataTest: received data", data['content'].seq_number, '/', data['content'].seq_max
            if data['content'].done:
                self._def_done.callback(True)
            yield msg.ack()

        consumer_config = { 'exchange' : 'magnet.topic',
                'exchange_type' : 'topic',
                'durable': False,
                'auto_delete': True,
                'mandatory': True,
                'immediate': False,
                'warn_if_exists': False,
                'routing_key' : 'data_listener',      # may be None, if so, no binding is made to the queue (routing_key is incorrectly named in the dict used by Receiver)
                'queue' : None,              # may be None, if so, the queue is made anonymously (and stored in receiver's consumer.queue attr)
              }

        datarec = WorkerReceiver('data_listener', process=self.proc, scope=Receiver.SCOPE_GLOBAL, handler=datahandler, consumer_config=consumer_config)
        yield datarec.attach()

        # re-patch workbench back to old send method
        self.ds1.workbench._send_data_chunk = self._old_send_chunk

        msg = yield self.dsc.proc.message_client.create_instance(DATA_REQUEST_MESSAGE_TYPE)
        msg.structure_array_ref = self.first_struct_as_key

        bounds = msg.request_bounds.add()
        bounds.origin = 0
        bounds.size = 15

        bounds = msg.request_bounds.add()
        bounds.origin = 0
        bounds.size = 40

        bounds = msg.request_bounds.add()
        bounds.origin = 0
        bounds.size = 200

        msg.data_routing_key = "data_listener"

        yield self.dsc.extract_data(msg)
        yield self._def_done

        # should have some data now
        self.failUnless(len(self._recv_data) > 0)       # unable to predict exact as we may change chunk size

        # did we get the right amount of data?
        totalelems = reduce(lambda x, y: x+y, (len(x['ndarray']) for x in self._recv_data))
        self.failUnlessEquals(totalelems, 200*40*15)

        # should be contiguous too
        counter = 0
        for ndarray in (x['ndarray'] for x in self._recv_data):
            for data in ndarray:
                self.failUnlessEqual(int(data), counter)
                counter += 1

        # tear down datarec
        yield datarec.terminate()

    @defer.inlineCallbacks
    def test_full_one_ba(self):

        msg = yield self.dsc.proc.message_client.create_instance(DATA_REQUEST_MESSAGE_TYPE)
        msg.structure_array_ref = self.first_struct_as_key

        bounds = msg.request_bounds.add()
        bounds.origin = 0
        bounds.size = 15

        bounds = msg.request_bounds.add()
        bounds.origin = 0
        bounds.size = 40

        bounds = msg.request_bounds.add()
        bounds.origin = 0
        bounds.size = 200

        msg.data_routing_key = "data_listener"

        yield self.dsc.extract_data(msg)
        yield self._def_done

        # should have some data now
        self.failUnless(len(self._recv_data) > 0)       # unable to predict exact as we may change chunk size

        # did we get the right amount of data?
        totalelems = reduce(lambda x, y: x+y, (len(x['ndarray']) for x in self._recv_data))
        self.failUnlessEquals(totalelems, 200*40*15)

        # should be contiguous too
        counter = 0
        for ndarray in (x['ndarray'] for x in self._recv_data):
            for data in ndarray:
                self.failUnlessEqual(int(data), counter)
                counter += 1
        
    @defer.inlineCallbacks
    def test_partial_one_ba(self):
        msg = yield self.dsc.proc.message_client.create_instance(DATA_REQUEST_MESSAGE_TYPE)
        msg.structure_array_ref = self.first_struct_as_key

        bounds = msg.request_bounds.add()
        bounds.origin = 2
        bounds.size = 1

        bounds = msg.request_bounds.add()
        bounds.origin = 0
        bounds.size = 10

        bounds = msg.request_bounds.add()
        bounds.origin = 50
        bounds.size = 100

        msg.data_routing_key = "data_listener"

        yield self.dsc.extract_data(msg)
        yield self._def_done

        # should have some data now
        self.failUnless(len(self._recv_data) > 0)

        # did we get the right amount of data?
        totalelems = reduce(lambda x, y: x+y, (len(x['ndarray']) for x in self._recv_data))
        self.failUnlessEquals(totalelems, 100 * 10 * 1)

        # spot check values - to do this we must assemble all the ndarrays into one
        bigndarray = []
        for ndarray in (x['ndarray'] for x in self._recv_data):
           bigndarray.extend(ndarray)

        self.failIfEquals(int(bigndarray[0]), 0)    # it's not 0 here, we started much further in

        self.failIfEquals(int(bigndarray[100]) - int(bigndarray[99]), 1)    # this is a slice boundary of returned data - the value there
                                                                            # is the actual index in the big array and should differ quite a bit

    @defer.inlineCallbacks
    def test_stride_one_ba(self):
        msg = yield self.dsc.proc.message_client.create_instance(DATA_REQUEST_MESSAGE_TYPE)
        msg.structure_array_ref = self.first_struct_as_key

        bounds = msg.request_bounds.add()
        bounds.origin = 0
        bounds.size = 1

        bounds = msg.request_bounds.add()
        bounds.origin = 0
        bounds.size = 10

        bounds = msg.request_bounds.add()
        bounds.origin = 50
        bounds.size = 100
        bounds.stride = 10

        msg.data_routing_key = "data_listener"

        yield self.dsc.extract_data(msg)
        yield self._def_done

        # should have some data now
        self.failUnless(len(self._recv_data) > 0)

        # did we get the right amount of data?
        totalelems = reduce(lambda x, y: x+y, (len(x['ndarray']) for x in self._recv_data))
        self.failUnlessEquals(totalelems, 10 * 10 * 1) # 50-150 stride 10 is 10

        # we strode the last dimension, each value should be 10 up from the previous
        last = None

        # assemble all the ndarrays into one
        bigndarray = []
        for ndarray in (x['ndarray'] for x in self._recv_data):
           bigndarray.extend(ndarray)

        for x in xrange(10):
            if last is not None:
                self.failUnlessEqual(int(bigndarray[x]), last+10)

            last = int(bigndarray[x])


    @defer.inlineCallbacks
    def test_full_multi_ba(self):
        
        msg = yield self.dsc.proc.message_client.create_instance(DATA_REQUEST_MESSAGE_TYPE)
        msg.structure_array_ref = self.second_struct_as_key

        bounds = msg.request_bounds.add()
        bounds.origin = 0
        bounds.size = 4

        for x in range(3):
            bounds = msg.request_bounds.add()
            bounds.origin = 0
            bounds.size = 20

        msg.data_routing_key = "data_listener"

        yield self.dsc.extract_data(msg)
        yield self._def_done

        # should have some data now
        self.failUnless(len(self._recv_data) > 0)

        # did we get the right amount of data?
        totalelems = reduce(lambda x, y: x+y, (len(x['ndarray']) for x in self._recv_data))
        self.failUnlessEquals(totalelems, 4 * (20 ** 3))

        # should be contiguous too
        counter = 0
        for ndarray in (x['ndarray'] for x in self._recv_data):
            for data in ndarray:
                self.failUnlessEqual(int(data), counter)
                counter += 1

    @defer.inlineCallbacks
    def test_partial_one_ba_multi_ba(self):
        msg = yield self.dsc.proc.message_client.create_instance(DATA_REQUEST_MESSAGE_TYPE)
        msg.structure_array_ref = self.second_struct_as_key

        bounds = msg.request_bounds.add()
        bounds.origin = 0
        bounds.size = 1     # just request one, this keeps it all in one ba

        for x in range(3):
            bounds = msg.request_bounds.add()
            bounds.origin = 0
            bounds.size = 20

        msg.data_routing_key = "data_listener"

        yield self.dsc.extract_data(msg)
        yield self._def_done

        # should have some data now
        self.failUnless(len(self._recv_data) > 0)

        # did we get the right amount of data?
        totalelems = reduce(lambda x, y: x+y, (len(x['ndarray']) for x in self._recv_data))
        self.failUnlessEquals(totalelems, 1 * (20 ** 3))

        # should be contiguous too
        counter = 0
        for ndarray in (x['ndarray'] for x in self._recv_data):
            for data in ndarray:
                self.failUnlessEqual(int(data), counter)
                counter += 1

    @defer.inlineCallbacks
    def test_partial_crossing_bas_multi_ba(self):
        msg = yield self.dsc.proc.message_client.create_instance(DATA_REQUEST_MESSAGE_TYPE)
        msg.structure_array_ref = self.second_struct_as_key

        bounds = msg.request_bounds.add()
        bounds.origin = 1
        bounds.size = 2

        for x in range(3):
            bounds = msg.request_bounds.add()
            bounds.origin = 10
            bounds.size = 5

        msg.data_routing_key = "data_listener"

        yield self.dsc.extract_data(msg)
        yield self._def_done

        # should have some data now
        self.failUnless(len(self._recv_data) > 0)

        # did we get the right amount of data?
        totalelems = reduce(lambda x, y: x+y, (len(x['ndarray']) for x in self._recv_data))
        self.failUnlessEquals(totalelems, 2 * (5 ** 3))

        # build the big array so we can spot check
        bigndarray = []
        for ndarray in (x['ndarray'] for x in self._recv_data):
           bigndarray.extend(ndarray)

        self.failIfEquals(int(bigndarray[0]), 0)    # we requested further in from there

        zerothval = (20**3)*1 + (20**2)*10 + 20 * 10 + 10
        self.failUnlessEquals(int(bigndarray[0]), zerothval)

        # check that the topmost dimension's 2nd entry is exactly 20**3 more than zerothval
        self.failUnlessEquals(int(bigndarray[5**3]), zerothval + 20**3)

    @defer.inlineCallbacks
    def test_stride_higherdim_multi_ba(self):
        msg = yield self.dsc.proc.message_client.create_instance(DATA_REQUEST_MESSAGE_TYPE)
        msg.structure_array_ref = self.second_struct_as_key

        bounds = msg.request_bounds.add()
        bounds.origin = 2
        bounds.size = 1

        bounds = msg.request_bounds.add()
        bounds.origin = 0
        bounds.size = 20
        bounds.stride = 5

        for x in range(2):
            bounds = msg.request_bounds.add()
            bounds.origin = 10
            bounds.size = 10

        msg.data_routing_key = "data_listener"

        yield self.dsc.extract_data(msg)
        yield self._def_done

        # should have some data now
        self.failUnless(len(self._recv_data) > 0)

        # did we get the right amount of data?
        totalelems = reduce(lambda x, y: x+y, (len(x['ndarray']) for x in self._recv_data))
        self.failUnlessEquals(totalelems, 1 * 4 * (10**2))  # 0-20 stride 5 gives 4 values (0, 5, 10, 15)

        # build the big array so we can spot check
        bigndarray = []
        for ndarray in (x['ndarray'] for x in self._recv_data):
           bigndarray.extend(ndarray)

        zerothval = (20**3) * 2 + 20 * 10 + 10
        self.failUnlessEquals(int(bigndarray[0]), zerothval)

        # calculate next strided value in the 2nd dimension
        nextval = (20**3) * 2 + (20**2) * 5 + 20 * 10 + 10

        # now the next index in our returned array
        nextidx = 10 * 10
        self.failUnlessEquals(int(bigndarray[nextidx]), nextval)
