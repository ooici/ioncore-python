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

"""
Define some data objects for testing
"""

class Inherit0(dataobject.DataObject):
    inherit0 = dataobject.TypedAttribute(str, '0')

class Inherit1(Inherit0):
    inherit1 = dataobject.TypedAttribute(str, '1')

class Inherit2(Inherit1):
    inherit2 = dataobject.TypedAttribute(str, '2')

class Inherit3(Inherit2):
    inherit3 = dataobject.TypedAttribute(str, '3')

class Inherit210(Inherit2, Inherit1, Inherit0):
    """
    """

class TestInheritObject(unittest.TestCase):
    
    def test_inherit(self):
        i0 = Inherit0()
        i1 = Inherit1()
        i2 = Inherit2()
        i3 = Inherit3()
        logging.info(i0)
        logging.info(i1)
        logging.info(i2)
        logging.info(i3)
        self.assertIsInstance(i3,Inherit0)
        self.assertIsInstance(i2,Inherit0)
        self.assertIsInstance(i1,Inherit0)
        self.assertIsInstance(i3,Inherit3)

        self.failUnlessIn('inherit0',i0.attributes)
        self.assertEqual(len(i0.attributes),1)

        self.failUnlessIn('inherit0',i1.attributes)
        self.failUnlessIn('inherit1',i1.attributes)
        self.assertEqual(len(i1.attributes),2)
        
        self.failUnlessIn('inherit2',i2.attributes)
        self.failUnlessIn('inherit1',i2.attributes)
        self.failUnlessIn('inherit0',i2.attributes)
        self.assertEqual(len(i2.attributes),3)
        
        self.failUnlessIn('inherit3',i3.attributes)
        self.failUnlessIn('inherit2',i3.attributes)
        self.failUnlessIn('inherit1',i3.attributes)
        self.failUnlessIn('inherit0',i3.attributes)
        self.assertEqual(len(i3.attributes),4)





class SimpleObject(dataobject.DataObject):
    """
    @Brief A simple data object to use as a base class
    """
    name = dataobject.TypedAttribute(str, 'blank')
    key = dataobject.TypedAttribute(str, 'xxx')

dataobject.DataObject._types['SimpleObject']=SimpleObject

class TestSimpleObject(unittest.TestCase):
    
    def setUp(self):
        
        obj = SimpleObject()
        obj.name = 'David'
        obj.key = 'seabird'
        self.obj = obj
        self.encoded=[('Object_Type', 'SimpleObject'),('key', 'str\x00seabird'),('name', 'str\x00David')]
     
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
        #print 'self.obj',self.obj
        self.assertEqual(self.obj,dec)
        self.assertEqual(type(self.obj).__name__,type(dec).__name__)

        
class PrimaryTypesObject(SimpleObject):
    """
    @Brief PrimaryTypesObject inherits attributes from Simple Object
    """
    integer = dataobject.TypedAttribute(int,5)
    floating = dataobject.TypedAttribute(float,5.0)
    boolen = dataobject.TypedAttribute(bool,True)
    
dataobject.DataObject._types['PrimaryTypesObject']=PrimaryTypesObject

class TestPrimaryTypesObject(TestSimpleObject):
    def setUp(self):
        obj = PrimaryTypesObject()
        obj.key = 'seabird'
        obj.name = 'David'
        obj.floating = 3.14159
        obj.boolen = False
        obj.integer = 42
        self.obj = obj
        self.encoded=[('Object_Type', 'PrimaryTypesObject'),('key', 'str\x00seabird'), ('floating', 'float\x003.14159'), ('integer', 'int\x0042'),('boolen', 'bool\x00False'), ('name', 'str\x00David')]        
        #self.encoded=[('Object_Type', 'PrimaryTypesObject'),('boolen', 'bool\x00False'), ('floating', 'float\x003.14159'), ('integer', 'int\x0042'), ('key', 'str\x00seabird'), ('name', 'str\x00David')]

class BinaryObject(dataobject.DataObject):
    name = dataobject.TypedAttribute(str)
    binary = dataobject.TypedAttribute(str)

dataobject.DataObject._types['BinaryObject']=BinaryObject

class TestBinaryObject(TestSimpleObject):
    def setUp(self):
        # Need to come up with better binary data to test with!
        obj = BinaryObject()
        obj.name = 'Binary Junk'
        obj.binary = cas.sha1bin(obj.name)
        self.obj = obj
        self.encoded=[('Object_Type', 'BinaryObject'),('binary', "str\x00\xca\x98T\x17~\x0e41\x83\xcf'\xb6\xba&l\x1d\xd1\x9d\xd8["), ('name', 'str\x00Binary Junk')]
     
class ListObject(dataobject.DataObject):
    name = dataobject.TypedAttribute(str)
    rlist = dataobject.TypedAttribute(list)
     
dataobject.DataObject._types['ListObject']=ListObject
     
class TestListObject(TestSimpleObject):
    def setUp(self):
        obj = ListObject()
        obj.name = 'a big list'
        obj.rlist = ['a',3,4.0]
        self.obj = obj
        self.encoded=[('Object_Type', 'ListObject'),('rlist', 'list\x00["str\\u0000a", "int\\u00003", "float\\u00004.0"]'),('name', 'str\x00a big list')]
     
class TestListOfObjects(TestSimpleObject):
    def setUp(self):
        obj = ListObject()
        obj.name = 'a big list of objects'
        obj.rlist = [PrimaryTypesObject(),PrimaryTypesObject(),SimpleObject()]
        self.obj = obj
        self.encoded=[('Object_Type', 'ListObject'),('rlist','list\x00["PrimaryTypesObject\\u0000[[\\"key\\", \\"str\\\\u0000xxx\\"], [\\"floating\\", \\"float\\\\u00005.0\\"], [\\"integer\\", \\"int\\\\u00005\\"], [\\"boolen\\", \\"bool\\\\u0000True\\"], [\\"name\\", \\"str\\\\u0000blank\\"]]", '+ 
                       '"PrimaryTypesObject\\u0000[[\\"key\\", \\"str\\\\u0000xxx\\"], [\\"floating\\", \\"float\\\\u00005.0\\"], [\\"integer\\", \\"int\\\\u00005\\"], [\\"boolen\\", \\"bool\\\\u0000True\\"], [\\"name\\", \\"str\\\\u0000blank\\"]]", '+
                       '"SimpleObject\\u0000[[\\"key\\", \\"str\\\\u0000xxx\\"], [\\"name\\", \\"str\\\\u0000blank\\"]]"]'),
                        ('name', 'str\x00a big list of objects')]
     
class SetObject(dataobject.DataObject):
    name = dataobject.TypedAttribute(str)
    rset = dataobject.TypedAttribute(set)
     
dataobject.DataObject._types['SetObject']=SetObject

class TestSetObject(TestSimpleObject):
    def setUp(self):
        obj = SetObject()
        obj.name = 'a big set'
        obj.rset = set(['a',3,4.0])
        self.obj = obj
        self.encoded=[('Object_Type', 'SetObject'),('rset', 'set\x00["str\\u0000a", "int\\u00003", "float\\u00004.0"]'),('name', 'str\x00a big set')]

class TupleObject(dataobject.DataObject):
    name = dataobject.TypedAttribute(str)
    rtuple = dataobject.TypedAttribute(tuple)
     
dataobject.DataObject._types['TupleObject']=TupleObject

     
class TestTupleObject(TestSimpleObject):
    def setUp(self):
        obj = TupleObject()
        obj.name = 'a big tuple'
        obj.rtuple = ('a',3,4.0)
        self.obj = obj
        self.encoded=[('Object_Type', 'TupleObject'),('rtuple', 'tuple\x00["str\\u0000a", "int\\u00003", "float\\u00004.0"]'),('name', 'str\x00a big tuple')]
     
class NestedObject(dataobject.DataObject):
    name = dataobject.TypedAttribute(str,'stuff')
    rset = dataobject.TypedAttribute(SetObject)
    primary = dataobject.TypedAttribute(PrimaryTypesObject)
    
    dataobject.DataObject._types['PrimaryTypesObject']=PrimaryTypesObject
    dataobject.DataObject._types['SetObject']=SetObject

dataobject.DataObject._types['NestedObject']=NestedObject

class TestNestedObject(TestSimpleObject):
    def setUp(self):
        sobj = SetObject()
        sobj.name = 'a big set'
        sobj.rset = set(['a',3,4.0])
        
        obj=NestedObject()
        obj.rset = sobj
        
        self.obj = obj
        self.encoded=[  ('Object_Type', 'NestedObject'),
                        ('primary','PrimaryTypesObject\x00[["key", "str\\u0000xxx"], ["floating", "float\\u00005.0"], ["integer", "int\\u00005"], ["boolen", "bool\\u0000True"], ["name", "str\\u0000blank"]]'),
                        ('rset','SetObject\x00[["rset", "set\\u0000[\\"str\\\\u0000a\\", \\"int\\\\u00003\\", \\"float\\\\u00004.0\\"]"], ["name", "str\\u0000a big set"]]'),
                        ('name', 'str\x00stuff')]
        
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
        logging.info('Sending Encoded resource:'+str(msg))
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

        
#class Send_Binary_Resource_Object(TestSendDataObject):
#    @defer.inlineCallbacks
#    def setUp(self):
#        res = BinaryObject()
#        res.name = 'Binary Junk'
#        res.binary = cas.sha1bin(res.name)
#        self.obj = res
#        yield self._start_container()

class Send_List_Data_Object(TestSendDataObject):
    @defer.inlineCallbacks
    def setUp(self):
        res = ListObject()
        res.name = 'a big list'
        res.rlist = ['a',3,4.0,PrimaryTypesObject()]
        self.obj = res
        yield self._start_container()
 
class Send_Set_Data_Object(TestSendDataObject):
    @defer.inlineCallbacks
    def setUp(self):
        res = SetObject()
        res.name = 'a big set'
        res.rlist = set(['a',3,4.0,PrimaryTypesObject()])
        self.obj = res
        yield self._start_container()
 
class TestSendResourceReference(TestSendDataObject):
    """
    """
    @defer.inlineCallbacks
    def setUp(self):
        obj = dataobject.Resource.create_new_resource()
        obj.name = 'complex'
        obj.ref = dataobject.ResourceReference(RegistryBranch='david',RegistryIdentity='mine', RegistryCommit='yours')
        self.obj = obj
        yield self._start_container()
    

class TestResource(unittest.TestCase):
    
    def test_create(self):
        res = dataobject.Resource.create_new_resource()
        self.assertEqual(res.RegistryBranch,'master')
        self.assert_(res.RegistryIdentity)
        self.assertNot(res.RegistryCommit)

    def test_reference(self):
        res = dataobject.Resource.create_new_resource()
        res.RegistryCommit = 'LotsOfJunk'
        
        ref = res.reference()
        self.assertEqual(res.RegistryIdentity,ref.RegistryIdentity)
        self.assertEqual(res.RegistryCommit,ref.RegistryCommit)
        self.assertEqual(res.RegistryBranch,ref.RegistryBranch)

        ref = res.reference(head=True)
        self.assertEqual(res.RegistryIdentity,ref.RegistryIdentity)
        self.assertEqual('',ref.RegistryCommit)
        self.assertEqual(res.RegistryBranch,ref.RegistryBranch)

    
    def test_set_lcstate(self):
        
        #logging.info(registry.LCStates)
        res = dataobject.Resource.create_new_resource()
        #logging.info(res.get_lifecyclestate())
        self.assertEqual(res.lifecycle, dataobject.LCStates.new)

        res.set_lifecyclestate(dataobject.LCStates.active)
        self.assertEqual(res.lifecycle, dataobject.LCStates.active)
        #logging.info(res.get_lifecyclestate())
        
        res.set_lifecyclestate(dataobject.LCStates['retired'])
        self.assertEqual(res.lifecycle, dataobject.LCStates.retired)
        #logging.info(res.get_lifecyclestate())

        self.failUnlessRaises(TypeError,res.set_lifecyclestate,'new')

    def test_get_lcstate(self):
 
        res = dataobject.Resource.create_new_resource()
        
        self.assertEqual(res.get_lifecyclestate(),dataobject.LCState('new'))
 
