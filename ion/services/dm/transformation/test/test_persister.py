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
                 'module':'ion.services.dm.transformation.persister',
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
