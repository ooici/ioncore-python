#!/usr/bin/env python

"""
@file ion/services/coi/test/test_hello.py
@author David Stuebe
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.util.itv_decorator import itv
from ion.core import ioninit
CONF = ioninit.config(__name__)

from ion.test.iontest import IonTestCase

from net.ooici.play import addressbook_pb2
from ion.util import procutils as pu
from ion.core.object import object_utils

from ion.core.data.storage_configuration_utility import COMMIT_INDEXED_COLUMNS


person_type = object_utils.create_type_identifier(object_id=20001, version=1)
addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
addressbook_type = object_utils.create_type_identifier(object_id=20002, version=1)
association_type = object_utils.create_type_identifier(object_id=13, version=1)


class DataStoreTest(IonTestCase):
    """
    Testing example hello service.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        services = [
            {'name':'index_store_service','module':'ion.core.data.index_store_service','class':'IndexStoreService',
                'spawnargs':{'indices':COMMIT_INDEXED_COLUMNS} },

            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{'commit_store_class':'ion.core.data.index_store_service.IndexStoreServiceClient'}
                },
            {'name':'workbench_test1',
             'module':'ion.core.object.test.test_workbench',
             'class':'WorkBenchProcess',
             'spawnargs':{'proc-name':'wb1'}},
        ]


        self.sup = yield self._spawn_processes(services)


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


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_push(self):

        log.info('DataStore1 Push addressbook to DataStore1')

        result = yield self.wb1.workbench.push_by_name('datastore',self.repo_key)

        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)

        log.info('DataStore1 Push addressbook to DataStore1: complete')


        
        # Test to make sure pushing a non existent workbench fails
        
        # How do I test raises in a deferred call?
        #self.assertRaises(KeyError,proc_ds1.push, 'ps2','NonExistentRepositoryName')
        

    @defer.inlineCallbacks
    def test_push_clear_pull(self):

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

        ab = yield repo.checkout('master')

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




    '''
    @defer.inlineCallbacks
    def test_push_associated(self):


        child_ds1 = yield self.sup.get_child_id('ds1')
        log.debug('Process ID:' + str(child_ds1))
        proc_ds1 = self._get_procinstance(child_ds1)
        
        child_ds2 = yield self.sup.get_child_id('ds2')
        log.debug('Process ID:' + str(child_ds2))
        proc_ds2 = self._get_procinstance(child_ds2)
        
        ab1 = proc_ds1.workbench.create_repository(addresslink_type,'addressbook1')
        ab2 = proc_ds1.workbench.create_repository(addresslink_type,'addressbook2')
        ab3 = proc_ds1.workbench.create_repository(addresslink_type,'addressbook3')
        assoc = proc_ds1.workbench.create_repository(association_type,'association')
        
        ab1.root_object.title = 'Junk'
        ab1.commit('test1')
        
        ab2.root_object.title = 'Predicate Junk'
        ab2.commit('test2')
        
        ab3.root_object.title = 'Associated Junk'
        ab3.commit('test3')
        
        assoc.root_object.subject = proc_ds1.workbench.reference_repository('addressbook1', current_state=True)
        assoc.root_object.predicate = proc_ds1.workbench.reference_repository('addressbook2', current_state=True)
        assoc.root_object.object = proc_ds1.workbench.reference_repository('addressbook3', current_state=True)
        assoc.commit('associated!')
        
        print 'ASSOC:', assoc.root_object
        print 'assoc subject:', assoc.root_object.subject
        
        
        obj_list = ['addressbook1','addressbook2','addressbook3','association']
        
        result = yield proc_ds1.push('ps2',obj_list)
            
        self.assertEqual(result.MessageResponseCode, result.ResponseCodes.OK)
    '''




