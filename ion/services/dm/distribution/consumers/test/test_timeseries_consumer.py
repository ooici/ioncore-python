#!/usr/bin/env python


"""
@file ion/services/dm/distribution/consumers/test/test_example_consumer.py
@author David Stuebe
@brief test for the example consumer process
"""


from twisted.trial import unittest

import logging
log = logging.getLogger(__name__)
from ion.test.iontest import IonTestCase

from ion.services.dm.distribution.consumers import timeseries_consumer

from ion.services.dm.util import dap_tools
from twisted.internet import defer

from ion.services.dm.util import dap_tools

from pydap.model import DatasetType, GridType, BaseType, Float32

import numpy

import random

class TestExampleConsumer(IonTestCase):
    '''
    @Brief Test case for a simple Example consumer. The test is a unit test for
    the business logic of the consumer only - the ondata method.
    '''
    
    
    @defer.inlineCallbacks
    def setUp(self):
        
        sz=12
        time = numpy.arange(float(0),float(sz))        
        data = numpy.arange(float(sz))
        for ind in range(sz):
            data[ind] = numpy.random.random()
    
        ds = DatasetType(name='SimpleGridData')
        g = GridType(name='TimeSeries')

        # The name in the dictionary must match the name in the basetype
        g['timeseries'] = BaseType(name='timeseries', data=data, shape=data.shape, type=Float32, dimensions=('time'))
        g['time'] = BaseType(name='time', data=time, shape=(sz,), type=Float32)
        
        ds[g.name]=g    
    
        self.ds1 = ds
        
        
        self.tc = timeseries_consumer.TimeseriesConsumer()
        yield self.tc.plc_init()
        
    def test_data1(self):
        self.tc.ondata(self.ds1, 'note', 1.0, queue='abc')
        
        print 'PDATA1:', self.tc.pdata
        self.assertEqual(len(self.tc.msgs_to_send),1)
        
        self.tc.ondata(self.ds1, 'note', 1.0, queue='abc')
        
        print 'PDATA2:', self.tc.pdata
        self.assertEqual(len(self.tc.msgs_to_send),2)
        
        

