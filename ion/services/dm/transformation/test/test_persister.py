#!/usr/bin/env python

"""
@file ion/services/dm/transformation/test/test_persister.py
@test ion.services.dm.persister Persister unit tests
@author Paul Hubbard
@author Matt Rodriguez
@date 6/7/10
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer
from twisted.trial import unittest

#from ion.services.dm.preservation.persister import PersisterService
#from ion.services.sa.fetcher import FetcherService, FetcherClient
from ion.core import bootstrap
from ion.services.dm.distribution import base_consumer

from ion.services.dm.transformation import persister

from ion.data import dataobject
from ion.test.iontest import IonTestCase
from ion.services.dm.util import dap_tools

from ion.util import procutils as pu
from ion.resources import dm_resource_descriptions
import exceptions
import os

TEST_ARCHIVE_PATH=os.path.join(os.path.sep, "tmp")

class PersisterDirectTest(unittest.TestCase):


    def setUp(self):
        # Give tearDown a junk name to try and delete if it is not set otherwise
        self.fname_test = os.path.join(os.path.sep, "tmp" ,"sies72390hf86seut28.junk")

    def tearDown(self):
        try:
            os.remove(self.fname_test)
        except exceptions.OSError:
            pass

    def test_newfile(self):
        """
        Test that the persister works against a new file
        """


        ds=dap_tools.simple_grid_dataset()
        fname = os.path.join(TEST_ARCHIVE_PATH, ds.name +'.nc')
        self.fname_test = fname

        fid=open(fname, "w")
        fid.close()

        p = persister.PersisterConsumer()

        retval = p._save_dap_dataset(ds, TEST_ARCHIVE_PATH)

        self.assertEqual(retval,0)

        ds_r = dap_tools.read_netcdf_from_file(fname)

        self.assertIn(ds.name,ds_r.name)

    def test_nofile(self):
        """
        Test that the persister works against a new file
        """


        ds=dap_tools.simple_grid_dataset()
        p = persister.PersisterConsumer()

        retval = p._save_dap_dataset(ds, TEST_ARCHIVE_PATH)

        # Result is not Zero return value - change to exception?
        self.assertEqual(retval,1)

    def test_appendfile(self):
        ds=dap_tools.simple_grid_dataset()
        fname = os.path.join(TEST_ARCHIVE_PATH, ds.name)
        self.fname_test = fname

        fid=open(fname+'.nc', "w")
        fid.close()

        p = persister.PersisterConsumer()

        # New file
        retval = p._save_dap_dataset(ds, TEST_ARCHIVE_PATH)
        self.assertEqual(retval,0)
        #test result
        ds_r = dap_tools.read_netcdf_from_file(fname)
        self.assertIn(ds.name,ds_r.name)

        #append file
        retval = p._save_dap_dataset(ds, TEST_ARCHIVE_PATH)
        self.assertEqual(retval,0)
        # Test results
        ds_r = dap_tools.read_netcdf_from_file(fname)
        self.assertIn(ds.name,ds_r.name)
        barray = ds.grid.time[0:3] == ds_r.grid.time[0:3]
        self.assert_(barray.all())
        barray = ds.grid.time[0:3] == ds_r.grid.time[4:7]
        self.assert_(barray.all())




class PersisterTest(IonTestCase):
    '''
    Test cases for the persister methods
    '''


    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        #self.sup = yield self._spawn_processes(services)

        #Create a test queue
        queue1=dataobject.create_unique_identity()
        queue_properties = {queue1:{'name_type':'fanout', 'args':{'scope':'global'}}}
        yield bootstrap.declare_messaging(queue_properties)
        self.queue1 = queue1


        # Create a dataset to test with
        ds=dap_tools.simple_grid_dataset()
        fname = os.path.join(TEST_ARCHIVE_PATH, ds.name +'.nc')
        self.fname_test = fname

        fid=open(fname, "w")
        fid.close()


        # Create a persister process
        pd1={'name':'persister_number_1',
                 'module':'ion.services.dm.preservation.persister',
                 'procclass':'PersisterConsumer',
                 'spawnargs':{'attach':self.queue1,
                              'process parameters':{'filename':fname}}}

        self.child1 = base_consumer.ConsumerDesc(**pd1)
        child1_id = yield self.test_sup.spawn_child(self.child1)

        # Don't do this - you can only get the instance in a test case -
        # this is not a valid pattern in OTP
        self.dc1 = self._get_procinstance(child1_id)
        # Make sure it is up and working!
        self.assertIn(self.queue1,self.dc1.dataReceivers)

        self.ds = ds
        self.fname = fname



    @defer.inlineCallbacks
    def tearDown(self):
        try:
            os.remove(self.fname)
        except exceptions.OSError:
            pass

        yield self._stop_container()
        # Kill the queues?


    @defer.inlineCallbacks
    def test_persister_consumer_dap(self):

        msg=dap_tools.ds2dap_msg(self.ds)

        yield self.test_sup.send(self.queue1,'data',msg.encode())

        yield pu.asleep(1)

        msg_cnt = yield self.child1.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent,{})
        self.assertEqual(received.get(self.queue1),1)

        yield self.test_sup.send(self.queue1,'data',msg.encode())

        yield pu.asleep(1)

        msg_cnt = yield self.child1.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent,{})
        self.assertEqual(received.get(self.queue1),2)

        ## Test results - figure out why this is not working the way it should!
        #ds_r = dap_tools.read_netcdf_from_file(self.fname)
        #self.assertIn(self.ds.name,ds_r.name)
        #barray = self.ds.grid.time[0:3] == ds_r.grid.time[0:3]
        #print barray
        #print 'ds.time',self.ds.grid.time[0:3]
        #print 'dsr.time',ds_r.grid.time.data.var[:]
        #self.assert_(barray.all())
        #barray = self.ds.grid.time[0:3] == ds_r.grid.time[4:7]
        #self.assert_(barray.all())


    @defer.inlineCallbacks
    def test_persister_consumer_string(self):
        msg = dm_resource_descriptions.StringMessageObject()
        msg.data = "I am a string"
        yield self.test_sup.send(self.queue1, 'data', msg.encode())
        msg_cnt = yield self.child1.get_msg_count()
        self.failUnless(msg_cnt > 0)
        # Check that the file is there!
    '''
    @defer.inlineCallbacks
    def test_persister_consumer_dap_no_file(self):
        try:
            os.remove(TEST_ARCHIVE_FILE)
        except OSError:
            pass
        msg=dap_tools.ds2dap_msg(dap_tools.simple_dataset(\
            {'DataSet Name':'SimpleData','variables':\
                {'time':{'long_name':'Data and Time','units':'seconds'},\
                'height':{'long_name':'person height','units':'meters'}}}, \
            {'time':(111,112,123,114,115,116,117,118,119,120), \
            'height':(8,6,4,-2,-1,5,3,1,4,5)}))

        try:
            yield self.test_sup.send(self.queue1,'data',msg.encode())
        except RuntimeError:
            log.info("Caught expected RuntimeError")
            pass
        msg_cnt = yield self.child1.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent,{})
        self.assertEqual(received.get(self.queue1),1)


    @defer.inlineCallbacks
    def test_persister_consumer_string(self):

        msg = dm_resource_descriptions.StringMessageObject()
        msg.data='Some junk'
        msg.notification = 'notes'
        msg.timestamp = 3.14159
        yield self.test_sup.send(self.queue1,'data',msg.encode())

        msg_cnt = yield self.child1.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent,{})
        self.assertEqual(received.get(self.queue1),1)
        # Check that the file is there!

    @defer.inlineCallbacks
    def test_persister_consumer_dictionary(self):

        msg = dm_resource_descriptions.DictionaryMessageObject()
        msg.data={'value':'Some junk'}
        msg.notification = 'notes'
        msg.timestamp = 3.14159
        yield self.test_sup.send(self.queue1,'data',msg.encode())

        msg_cnt = yield self.child1.get_msg_count()
        received = msg_cnt.get('received',{})
        sent = msg_cnt.get('sent',{})
        self.assertEqual(sent,{})
        self.assertEqual(received.get(self.queue1),1)
        # Check that the file is there!



     Wait till we have the unit test working!
    @defer.inlineCallbacks
    def test_fetcher_svc_persister_client(self):
        raise unittest.SkipTest('Timing out on EC2')
        """
        Trying to track down a failure - use fetcher service and
        persister client.
        """
        services = [
            {'name': 'persister', 'module': 'ion.services.dm.preservation.persister',
             'class': 'PersisterService'},
            ]
        boss = yield self._spawn_processes(services)
        fs = FetcherService()
        dset = fs._get_dataset_no_xmit(TEST_DSET)
        pc = PersisterClient(proc=boss)
        rc = yield pc.persist_dap_dataset(dset)
        self.failUnlessSubstring('OK', rc)
    '''

    '''
    @defer.inlineCallbacks
    def test_svcs_and_messaging(self):
        #raise unittest.SkipTest('Timing out on EC2')
        services = [
            {'name': 'persister', 'module': 'ion.services.dm.preservation.persister',
             'class': 'PersisterService'},
            {'name': 'fetcher', 'module': 'ion.services.sa.fetcher',
             'class': 'FetcherService'},
        ]
        boss = yield self._spawn_processes(services)

        fc = FetcherClient(proc=boss)
        log.debug('Grabbing dataset ' + TEST_DSET)
        dset = yield fc.get_dap_dataset(TEST_DSET)

        pc = PersisterClient(proc=boss)
        #ps = PersisterService()
        log.debug('Saving dataset...')
        rc = yield pc.persist_dap_dataset(dset)
        #ps._save_no_xmit(dset)
        self.failUnlessSubstring('OK', rc)
    '''

    '''
    @defer.inlineCallbacks
    def test_append_operation(self):

        services = [
            {'name': 'persister', 'module': 'ion.services.dm.preservation.persister',
             'class': 'PersisterService'},
            {'name': 'fetcher', 'module': 'ion.services.sa.fetcher',
             'class': 'FetcherService'},
        ]
        boss = yield self._spawn_processes(services)

        fc = FetcherClient(proc=boss)

        log.debug('Grabbing dataset ' + TEST_ADSET1)
        dset1 = yield fc.get_dap_dataset(TEST_ADSET1)
        log.debug('Grabbing dataset ' + TEST_ADSET2)
        dset2 = yield fc.get_dap_dataset(TEST_ADSET2)

        pc = PersisterClient(proc=boss)

        log.debug('Saving dataset...')
        rc = yield pc.persist_dap_dataset(dset1)


        self.failUnlessSubstring('OK', rc)
        pattern = TEST_APATTERN
        dataset = TEST_ADSET1
        rc = yield pc.append_dap_dataset(dataset, pattern, dset2)

        self.failUnlessSubstring('OK', rc)
    '''

    '''
class ServiceTester(unittest.TestCase):
    """
    Create an instance of the fetcher and persister services
    and test w/out capability container & messaging.

    Way too clever.
    """
    def setUp(self):
        """
        Instantiate the service classes
        """
        self.ps = PersisterService()
        self.fs = FetcherService()
        self.timeout = 120

    def test_instantiation_only(self):
        # Create and destroy the instances - any errors?
        pass

    def test_fetcher_and_persister_no_messaging(self):
        """
        More complex than it might appear - reach in and use the methods
        to get and persist a full dataset from amoeba (5.2MB)
        """
        raise unittest.SkipTest('Causes timeout on my workstation')

        # generate filename so we can look for it after saving
        local_dir = '/tmp/'
        fname = generate_filename(TEST_DSET, local_dir=local_dir)

        dset = self.fs._get_dataset_no_xmit(TEST_DSET)
        self.ps._save_no_xmit(dset, local_dir=local_dir)

        f = open(fname, 'r')
        if f:
           f.close()
        else:
            self.fail('Datafile not found!')
'''
