#!/usr/bin/env python

from twisted.trial import unittest
import logging
logging = logging.getLogger(__name__)

from twisted.python import reflect


from twisted.internet import defer
from ion.data import dataobject

# To Test messages using DataObjects
from ion.test.iontest import IonTestCase
from twisted.internet import defer

from magnet.spawnable import Receiver
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient

from ion.data.datastore import cas

class SimpleObject(dataobject.DataObject):
    key = dataobject.TypedAttribute(str, 'xxx')
    name = dataobject.TypedAttribute(str, 'blank')


class TestSimpleObject(unittest.TestCase):
    
    def setUp(self):
        
        obj = SimpleObject()
        obj.key = 'seabird'
        obj.name = 'David'
        self.obj = obj
        self.encoded=[('key', 'str\x00seabird'),('name', 'str\x00David')]
     
    def testPrintObject(self):
                
        logging.info(self.obj)
        
    def testEncode(self):
        """
        """
        enc = self.obj.encode()
        self.assertEqual(self.encoded,enc)
        
    def testDecode(self):
        dec = dataobject.DataObject.decode(self.encoded)()
        #print 'dec',dec
        self.assert_(self.obj==dec)

class PrimaryTypesObject(SimpleObject):
#    key = dataobject.TypedAttribute(str, 'xxx')
#    name = dataobject.TypedAttribute(str, 'blank')
    integer = dataobject.TypedAttribute(int,5)
    floating = dataobject.TypedAttribute(float,5.0)

class TestPrimaryTypesObject(TestSimpleObject):
    def setUp(self):
        obj = PrimaryTypesObject()
        obj.key = 'seabird'
        obj.name = 'David'
        obj.floating = 3.14159
        obj.integer = 42
        self.obj = obj
        self.encoded=[('key', 'str\x00seabird'), ('floating', 'float\x003.14159'), ('integer', 'int\x0042'), ('name', 'str\x00David')]
        
class BinaryObject(dataobject.DataObject):
    name = dataobject.TypedAttribute(str)
    binary = dataobject.TypedAttribute(str)

class TestBinaryObject(TestSimpleObject):
    def setUp(self):
        # Need to come up with better binary data to test with!
        obj = BinaryObject()
        obj.name = 'Binary Junk'
        obj.binary = cas.sha1bin(obj.name)
        self.obj = obj
        self.encoded=[('binary', "str\x00\xca\x98T\x17~\x0e41\x83\xcf'\xb6\xba&l\x1d\xd1\x9d\xd8["), ('name', 'str\x00Binary Junk')]
     
class ListObject(dataobject.DataObject):
    name = dataobject.TypedAttribute(str)
    rlist = dataobject.TypedAttribute(list)
     
#class TestListObject(TestSimpleObject):
#    def setUp(self):
#        obj = ListObject()
#        obj.name = 'a big list'
#        obj.rlist = ['a',3,4.0,{'a':3}]
#        self.obj = obj
#        self.encoded=[('rlist', "list\x00['a', 3, 4.0, {'a': 3}]"), ('name', 'str\x00a big list')]
     
#class NestedResource(dataobject.DataObject):
#    name = dataobject.TypedAttribute(str)
#   device = dataobject.TypedAttribute(dataobject.DeviceResource)

#class TestNestedResource(TestDeviceResource):
#    def setUp(self):
#        
#        dev = dataobject.DeviceResource()
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
        
        obj = dataobject.DataObject.decode(content)()
        logging.info(obj)
        response = obj.encode()

        # The following line shows how to reply to a message
        yield self.reply(msg, 'reply', response, {})

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
    def send_data_object(self, obj):
        yield self._check_init()
        #print obj
        msg=obj.encode()
        
        (content, headers, msg) = yield self.rpc_send('respond', msg, {})
        logging.info('Responder replied: '+str(content))
        response = dataobject.DataObject.decode(content)()
        defer.returnValue(response)

# Spawn of the process using the module name
factory = ProtocolFactory(ResponseService)



class TestSendDataObject(IonTestCase):
    """Testing service classes of resource registry
    """
    @defer.inlineCallbacks
    def setUp(self):
        obj = SimpleObject()
        obj.key = 'seabird'
        obj.name = 'David'
        self.obj = obj
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_send_data_object(self):

        services = [
            {'name':'responder','module':'ion.data.test.test_dataobject','class':'ResponseService'},
        ]

        sup = yield self._spawn_processes(services)

        rsc = ResponseServiceClient(sup)
        
        # Simple Send and Check value:
        response = yield rsc.send_data_object(self.obj)
        self.assertEqual(self.obj, response)

class TestSendTypesDataObject(TestSendDataObject):
    """Testing service classes of resource registry
    """
    @defer.inlineCallbacks
    def setUp(self):
        obj = PrimaryTypesObject()
        obj.key = 'seabird'
        obj.name = 'David'
        obj.floating = 3.14159
        obj.integer = 42
        self.obj = obj
        yield self._start_container()

        
#class Send_Binary_Resource_Object(Send_Resource_Object_Test):
#    @defer.inlineCallbacks
#    def setUp(self):
#        res = BinaryResource()
#        res.name = 'Binary Junk'
#        res.binary = sha1bin(res.name)
#        self.res = res
#        yield self._start_container()

#class Send_List_Resource_Object(Send_Resource_Object_Test):
#    @defer.inlineCallbacks
#    def setUp(self):
#        res = ListResource()
#        res.name = 'a big list'
#        res.rlist = ['a',3,4.0,{'a':3}]
#        self.res = res
#        yield self._start_container()
 
 
 