#!/usr/bin/env python

from twisted.trial import unittest
import logging
logging = logging.getLogger(__name__)

from ion.data import resource
from twisted.python import reflect


from twisted.internet import defer
from ion.data.dataobject import DataObject

# To Test messages using DataObjects
from ion.test.iontest import IonTestCase
from twisted.internet import defer

from magnet.spawnable import Receiver
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

from ion.data.datastore.cas import sha1bin
from ion.data.datastore.cas import sha1hex

class TestDeviceResource(unittest.TestCase):
    
    def setUp(self):
        
        res = resource.DeviceResource()
        res.mfg = 'seabird'
        res.serial = 10
        res.voltage = 3.14159
        self.res = res
        self.res_type = reflect.fullyQualifiedName(resource.DeviceResource)
        self.encoded=[('voltage', 'float\x003.14159'), ('serial', 'int\x0010'), ('mfg', 'str\x00seabird')]
     
    def testPrintResource(self):
                
        logging.info(self.res)
        
    def testEncode(self):
        """
        """
        enc = self.res.encode()
        self.assertEqual(self.encoded,enc)
        
    def testDecode(self):
        dec = resource.BaseResource.decode(self.res_type,self.encoded)()
        #print 'dec',dec
        self.assert_(self.res==dec)


class BinaryResource(resource.BaseResource):
    name = resource.TypedAttribute(str)
    binary = resource.TypedAttribute(str)

class TestBinaryResource(TestDeviceResource):
    def setUp(self):
        # Need to come up with better binary data to test with!
        res = BinaryResource()
        res.name = 'Binary Junk'
        res.binary = sha1bin(res.name)
        self.res = res
        self.res_type = reflect.fullyQualifiedName(BinaryResource)
        self.encoded=[('binary', "str\x00\xca\x98T\x17~\x0e41\x83\xcf'\xb6\xba&l\x1d\xd1\x9d\xd8["), ('name', 'str\x00Binary Junk')]
     
class ListResource(resource.BaseResource):
    name = resource.TypedAttribute(str)
    rlist = resource.TypedAttribute(list)
     
class TestListResource(TestDeviceResource):
    def setUp(self):
        res = ListResource()
        res.name = 'a big list'
        res.rlist = ['a',3,4.0,{'a':3}]
        self.res = res
        self.res_type = reflect.fullyQualifiedName(ListResource)
        self.encoded=[('rlist', "list\x00['a', 3, 4.0, {'a': 3}]"), ('name', 'str\x00a big list')]
     
class NestedResource(resource.BaseResource):
    name = resource.TypedAttribute(str)
    device = resource.TypedAttribute(resource.DeviceResource)

#class TestNestedResource(TestDeviceResource):
#    def setUp(self):
#        
#        dev = resource.DeviceResource()
#        dev.mfg = 'seabird'
#        dev.serial = 10
#        dev.voltage = 3.14159
#        
#        res = NestedResource()
#        res.name = 'a dev resource'
#        res.device = dev
#        self.res = res
#        self.res_type = reflect.fullyQualifiedName(NestedResource)
#        self.encoded=[('rlist', "list\x00['a', 3, 4.0, {'a': 3}]"), ('name', 'str\x00a big list')]

        
class ResponseService(BaseService):
    """Example service implementation
    """
    # Declaration of service
    declare = BaseService.service_declare(name='responder', version='0.1.0', dependencies=[])
    
    def slc_init(self):
        pass

    @defer.inlineCallbacks
    def op_respond(self, content, headers, msg):
        logging.info('op_respond: '+str(content))
        
        res = resource.BaseResource.decode(content['type'],content['body'])()
        rep={}
        rep['type'] = reflect.fullyQualifiedName(res.__class__)
        rep['body'] = res.encode()

        # The following line shows how to reply to a message
        yield self.reply(msg, 'reply', rep, {})

class ResponseServiceClient(BaseServiceClient):
    """
    This is an exemplar service class that calls the hello service. It
    applies the RPC pattern.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "responder"
        BaseServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def send_resource_object(self, res):
        yield self._check_init()
        print res
        msg={}
        msg['type'] =  reflect.fullyQualifiedName(resource.__class__)
        msg['body'] = res.encode()
        
        (content, headers, msg) = yield self.rpc_send('respond', msg, {})
        logging.info('Responder replied: '+str(content))
        rep = resource.BaseResource.decode(content['type'],content['body'])()
        defer.returnValue(rep)

# Spawn of the process using the module name
factory = ProtocolFactory(ResponseService)



class Send_Resource_Object_Test(IonTestCase):
    """Testing service classes of resource registry
    """
    @defer.inlineCallbacks
    def setUp(self):
        res = resource.DeviceResource()
        res.mfg = 'seabird'
        res.serial = 10
        res.voltage = 3.14159
        self.res = res
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_send_data_object(self):

        services = [
            {'name':'responder','module':'ion.data.test.test_resource','class':'ResponseService'},
        ]

        sup = yield self._spawn_processes(services)

        rsc = ResponseServiceClient(sup)
        
        # Simple Send and Check value:
        response = yield rsc.send_resource_object(self.res)
        self.assertEqual(self.res, response)
        
        
#class Send_Binary_Resource_Object(Send_Resource_Object_Test):
#    @defer.inlineCallbacks
#    def setUp(self):
#        res = BinaryResource()
#        res.name = 'Binary Junk'
#        res.binary = sha1bin(res.name)
#        self.res = res
#        yield self._start_container()

class Send_List_Resource_Object(Send_Resource_Object_Test):
    @defer.inlineCallbacks
    def setUp(self):
        res = ListResource()
        res.name = 'a big list'
        res.rlist = ['a',3,4.0,{'a':3}]
        self.res = res
        yield self._start_container()
 
 
 