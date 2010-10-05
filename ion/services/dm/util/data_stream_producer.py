#!/usr/bin/env python

"""
@file ion/services/dm/util/data_stream_producer.py
@author David Stuebe
@brief a data stream producer process - it spits out dap messages
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import time

from twisted.internet import defer
from twisted.internet.task import LoopingCall
from twisted.internet import reactor
from ion.core.process.process import ProcessFactory

from ion.core.process.process import Process, ProcessDesc
import ion.util.procutils as pu

from ion.data import dataobject
from ion.resources.dm_resource_descriptions import \
    DAPMessageObject, DataMessageObject

from ion.services.dm.util import dap_tools

from pydap.model import DatasetType

#import numpy
import random
from ion.services.dm.util import dap_tools


class DataStreamProducer(Process):
    '''
    @brief a data stream producer process - it spits out dap messages
    '''


    #@defer.inlineCallbacks
    def plc_init(self):

        #self.params = self.spawn_args.get('process parameters',{})
        self.deliver = self.spawn_args.get('delivery queue','')

        # Scheduled interval delivery - digest mode!
        self.delivery_interval = self.spawn_args.get('delivery interval',None)
        if self.delivery_interval:
            assert isinstance(self.delivery_interval, (int,float)), 'delivery interval must be a float or a integer'
            assert self.delivery_interval > 0.0, 'delivery interval must be greater than zero'
        else:
            raise RuntimeError('Invalid delivery interval specified')

        self.loop = LoopingCall(self.stream)
        # Have to wait to actually start the loop
        reactor.callLater(1, self.loop.start, self.delivery_interval)
        self.index = 0



    @defer.inlineCallbacks
    def stream(self):


        sz=10

        metadata = {'DataSet Name':'SimpleData','variables':\
                {'time':{'long_name':'Data and Time','units':'seconds'},\
                'height':{'long_name':'person height','units':'meters'}}}


        t = range(self.index,self.index+sz)
        self.index += sz

        z=[]
        for i in range(sz):
            z.append(random.randint(-3, 120))

        data = {'time':t, 'height':z}
        #print 'data',data

        msg = dap_tools.simple_datamessage(metadata, data)

        yield self.send(self.deliver,'data',msg.encode())


# Spawn of the process using the module name
factory = ProcessFactory(DataStreamProducer)
