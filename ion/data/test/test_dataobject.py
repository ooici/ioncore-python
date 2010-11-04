#!/usr/bin/env python

from twisted.trial import unittest
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.python import reflect


from twisted.internet import defer
from ion.data import dataobject

# To Test messages using DataObjects
from ion.test.iontest import IonTestCase
from twisted.internet import defer

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

from ion.services.dm.preservation import cas

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

class InheritOver(Inherit210,Inherit3):
    inheritover = dataobject.TypedAttribute(str, 'a new one')
    inherit2 = dataobject.TypedAttribute(str, 'over')
    inherit0 = dataobject.TypedAttribute(str, 'over')

#class InheritOverOver(Inherit210,InheritOver):
    """
    This will fail - it is an illegal inheritance pattern
    """
class InheritOverUnder(InheritOver,Inherit210):
    """
    Legal but does nothing?
    """

class TestInheritedObject(unittest.TestCase):

    def test_inheritance(self):
        i0 = Inherit0()
        i1 = Inherit1()
        i2 = Inherit2()
        i3 = Inherit3()
        log.info(i0)
        log.info(i1)
        log.info(i2)
        log.info(i3)
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


    def test_Inherit210(self):
        i210 = Inherit210()
        log.info(i210)
        self.assertEqual(i210.inherit2, '2')
        self.assertEqual(i210.inherit1, '1')
        self.assertEqual(i210.inherit0, '0')

    def test_InheritOver(self):
        io = InheritOver()
        log.info(io)
        self.assertEqual(io.inherit2, 'over')
        self.assertEqual(io.inherit1, '1')
        self.assertEqual(io.inherit0, 'over')
        self.assertEqual(io.inheritover, 'a new one')

    '''
    def test_InheritOverOver(self):
        raise unittest.SkipTest('This is not a legal inheritance pattern!')
        #ioo = InheritOverOver()
        #log.info(io)
        #self.assertEqual(ioo.inherit2, 'over')
        #self.assertEqual(ioo.inherit1, '1')
        #self.assertEqual(ioo.inherit0, 'over')
        #self.assertEqual(ioo.inheritover, 'a new one')
    '''

    def test_InheritOverUnder(self):
        iou = InheritOverUnder()
        log.info(iou)
        self.assertEqual(iou.inherit2, 'over')
        self.assertEqual(iou.inherit1, '1')
        self.assertEqual(iou.inherit0, 'over')
        self.assertEqual(iou.inheritover, 'a new one')

    def test_get_typedattributes(self):
        io = InheritOver()

        atts = io.get_typedattributes()

        cls_atts = InheritOver.get_typedattributes()

        # Make sure the method works for both class and instance
        self.assertEqual(atts, cls_atts)

        # check that the dict of atts is correct
        self.assertEqual(atts['inherit2'].default,io.inherit2)
        self.assertEqual(atts['inherit2'].type,type(io.inherit2))

        self.assertEqual(atts['inherit1'].default,io.inherit1)
        self.assertEqual(atts['inherit1'].type,type(io.inherit1))

        self.assertEqual(atts['inherit0'].default,io.inherit0)
        self.assertEqual(atts['inherit0'].type,type(io.inherit0))

        self.assertEqual(atts['inheritover'].default,io.inheritover)
        self.assertEqual(atts['inheritover'].type,type(io.inheritover))

class TestDataObjectComparison(unittest.TestCase):

    def test_eq_inherit(self):
        i0=Inherit0()
        i1=Inherit1()
        a1=Inherit1()

        self.assertEqual(i0,i0)
        self.assertEqual(i1,i1)
        self.assertEqual(i1,a1)


        self.assertNotEqual(i0,i1)
        self.assertNotEqual(i1,i0)

    def test_eq_primarytypes(self):
        p1=PrimaryTypesObject()
        p2=PrimaryTypesObject()

        p1.key = 'equal?'
        self.assertNotEqual(p1,p2)
        p2.key = 'equal?'
        self.assertEqual(p1,p2)

        p1.boolen = False
        self.assertNotEqual(p1,p2)
        p2.boolen = False
        self.assertEqual(p1,p2)

        p1.int = 42
        self.assertNotEqual(p1,p2)
        p2.int = 42
        self.assertEqual(p1,p2)

        p1.float = 3.14159
        self.assertNotEqual(p1,p2)
        p2.float = 3.14159
        self.assertEqual(p1,p2)


    def test_ge_inherit(self):
        i0=Inherit0()
        i1=Inherit1()
        a1=Inherit1()

        self.assert_(i0<=i1)
        self.assert_(i1>=i0)
        self.assertNot(i0>=i1)
        self.assertNot(i1<=i0)
        self.assert_(i0<=i0)
        self.assert_(i1<=i1)
        self.assert_(i1>=i1)

    def test_ge_primarytypes(self):
        p1=PrimaryTypesObject()
        p2=PrimaryTypesObject()

        p1.key = 'equal?'
        self.assertNot(p1>=p2)
        p2.key = 'equal?'
        self.assert_(p1>=p2)

        p1.boolen = False
        self.assertNot(p1<=p2)
        p2.boolen = False
        self.assert_(p1<=p2)

        p1.integer = 42
        self.assertNot(p1<=p2)
        p2.integer = 42
        self.assert_(p1>=p2)

        p1.floating = 3.14159
        self.assertNot(p1>=p2)
        p2.floating = 3.14159
        self.assert_(p1>=p2)

    def test_comare_to_inherits(self):

        p1=PrimaryTypesObject()
        s1=SimpleObject()

        # The attributes of S1 are still equal to the same attributes of P1
        p1.integer=42
        self.assert_(s1.compared_to(p1))

        p1.name = 'NotEqual'
        self.assertNot(s1.compared_to(p1,regex=True))

        s1.name = 'Not'
        self.assert_(s1.compared_to(p1,regex=True))


    def test_compare_to_certain_atts(self):

        p1=PrimaryTypesObject()
        p2=PrimaryTypesObject()

        atts=['name','integer']


        # The attributes of S1 are still equal to the same attributes of P1
        p1.integer=42
        self.assertNot(p1.compared_to(p2,attnames=atts))

        p2.integer=42
        self.assert_(p1.compared_to(p2,attnames=atts))

        p2.floating = 3.14159
        self.assert_(p1.compared_to(p2,attnames=atts))

        p2.name = 'NotEqual'
        self.assertNot(p1.compared_to(p2,regex=True,attnames=atts))

        p1.name = 'Not'
        self.assert_(p1.compared_to(p2,regex=True,attnames=atts))

    def test_ignoring_defaults(self):

        p1=PrimaryTypesObject()
        p2=PrimaryTypesObject()

        p1.name='test'
        p1.integer=90
        p1.floating=472.0

        p2.name='test'

        self.assert_(p2.compared_to(p1,ignore_defaults=True))
        self.assertNot(p1.compared_to(p2,ignore_defaults=True))

        p2.name='tes'
        self.assert_(p2.compared_to(p1,ignore_defaults=True,regex=True))
        self.assertNot(p1.compared_to(p2,ignore_defaults=True,regex=True))



class SimpleObject(dataobject.DataObject):
    """
    @brief A simple data object to use as a base class
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

        log.info(self.obj)

    def testEncode(self):
        """
        """
        enc = self.obj.encode()
        self.assertEqual(self.encoded,enc)

    def testDecode(self):
        dec = dataobject.DataObject.decode(self.encoded)
        #print 'dec',dec
        #print 'self.obj',self.obj
        self.assertEqual(self.obj,dec,'Original: %s \n Decoded: %s' % (str(self.obj), str(dec)))
        self.assertEqual(type(self.obj).__name__,type(dec).__name__)


class PrimaryTypesObject(SimpleObject):
    """
    @brief PrimaryTypesObject inherits attributes from Simple Object
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

class TestDataObjectMethods(unittest.TestCase):
    def setUp(self):
        obj = PrimaryTypesObject()
        obj.key = 'seabird'
        obj.name = 'David'
        obj.floating = 3.14159
        obj.boolen = False
        obj.integer = 42
        self.obj = obj

    def test_attributes(self):
        atts = self.obj.attributes
        self.assertIn('key',atts)
        self.assertIn('name',atts)
        self.assertIn('floating',atts)
        self.assertIn('boolen',atts)
        self.assertIn('integer',atts)

    def test_get_attributes(self):
        atts = self.obj.get_attributes()
        self.assertEqual(atts['key'],self.obj.key)
        self.assertEqual(atts['name'],self.obj.name)
        self.assertEqual(atts['floating'],self.obj.floating)
        self.assertEqual(atts['boolen'],self.obj.boolen)
        self.assertEqual(atts['integer'],self.obj.integer)

    def test_get_typedattributes(self):
        atts = self.obj.get_typedattributes()
        self.assertEqual(atts['key'].type,type(self.obj.key))
        self.assertEqual(atts['name'].type,type(self.obj.name))
        self.assertEqual(atts['floating'].type,type(self.obj.floating))
        self.assertEqual(atts['boolen'].type,type(self.obj.boolen))
        self.assertEqual(atts['integer'].type,type(self.obj.integer))

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

class TestListObjectBehavior(unittest.TestCase):
    def test_recursive(self,int=None):

        if not int:
            int = 1
        else:
            int = int +1

        obj1 = ListObject()
        obj1.rlist.append(int)

        if int <= 5:
            self.test_recursive(int=int)
        else:
            self.assertEqual(obj1.rlist,[int])


    def test_two_instances(self):

        obj1 = ListObject()
        obj1.rlist.append(1)
        obj1.rlist.append(2)
        #obj1.rlist = [1,2,4]


        obj2 = ListObject()
        self.assertEqual(obj2.rlist, [])
        self.assertEqual(obj1.rlist, [1,2])


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


class DictObject(dataobject.DataObject):
    name = dataobject.TypedAttribute(str)
    rdict = dataobject.TypedAttribute(dict)

dataobject.DataObject._types['DictObject']=DictObject

class TestDictObject(TestSimpleObject):
    def setUp(self):
        obj = DictObject()
        obj.name = 'David'
        obj.rdict = {'a':'a','b':5,'data':3.14159} # Must be 'Jasonable'
        self.obj = obj
        self.encoded=[('Object_Type', 'DictObject'),
                    ('rdict', 'dict\x00{"a": "a", "b": 5, "data": 3.1415899999999999}'),
                    ('name', 'str\x00David')]

class TestDictObject2(TestSimpleObject):
    def setUp(self):
        obj = DictObject()
        obj.name = 'David'
        obj.rdict = {'a':'a','b':5,'data':3.14159,'list':[1,2,3]} # Must be 'Jasonable'
        self.obj = obj
        self.encoded=[('Object_Type', 'DictObject'),
                     ('rdict',
                      'dict\x00{"a": "a", "list": [1, 2, 3], "b": 5, "data": 3.1415899999999999}'),
                     ('name', 'str\x00David')]


class TestDictObject3(TestSimpleObject):
    def setUp(self):
        obj = DictObject()
        obj.name = 'David'
        obj.rdict = {'a':'a','b':5,'data':3.14159,'d3':{'1':2,'3':4}} # Must be 'Jasonable'
        self.obj = obj
        self.encoded=[('Object_Type', 'DictObject'),
                    ('rdict', 'dict\x00{"a": "a", "b": 5, "data": 3.1415899999999999, "d3": {"1": 2, "3": 4}}'),
                    ('name', 'str\x00David')]


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

"""
Complex nested Object - similar to the pattern used in DM CDMDataset
"""

class DataType(dataobject.DataObject):
    """
    """
dataobject.DataObject._types['DataType']=DataType


class DataType1(DataType):
    f = dataobject.TypedAttribute(float)
    s = dataobject.TypedAttribute(str)

dataobject.DataObject._types['DataType1']=DataType1

class DataType2(DataType):
    i = dataobject.TypedAttribute(int)
    b = dataobject.TypedAttribute(bool)

dataobject.DataObject._types['DataType2']=DataType2

class DataContainer(dataobject.DataObject):
    name = dataobject.TypedAttribute(str)
    dt = dataobject.TypedAttribute(DataType)

dataobject.DataObject._types['DataContainer']=DataContainer

class TestDataContainer(TestSimpleObject):
    def setUp(self):
        obj = DataContainer()
        obj.name = 'a container with datatype1'
        obj.dt = DataType1()

        obj.dt.f = 3.14159
        obj.dt.s = 'Datatype1'

        self.obj = obj
        self.encoded=[('Object_Type', 'DataContainer'),
                    ('dt','DataType1\x00[["f", "float\\u00003.14159"], ["s", "str\\u0000Datatype1"]]'),
                    ('name', 'str\x00a container with datatype1')]

class ResponseService(ServiceProcess):
    """Example service implementation
    """
    # Declaration of service
    declare = ServiceProcess.service_declare(name='responder', version='0.1.0', dependencies=[])

    def slc_init(self):
        pass

    @defer.inlineCallbacks
    def op_respond(self, content, headers, msg):
        log.info('op_respond: '+str(content))


        obj = dataobject.DataObject.decode(content)
        log.info(obj)
        response = obj.encode()

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, response)

class ResponseServiceClient(ServiceClient):
    """
    This is an exemplar service class that calls the hello service. It
    applies the RPC pattern.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "responder"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def send_data_object(self, obj):
        yield self._check_init()
        #print obj
        msg=obj.encode()
        log.info('Sending Encoded resource:'+str(msg))
        (content, headers, msg) = yield self.rpc_send('respond', msg, {})
        log.info('Responder replied: '+str(content))
        #response = dataobject.DataObject.decode(content['value'])
        response = dataobject.DataObject.decode(content)
        defer.returnValue(response)

# Spawn of the process using the module name
factory = ProcessFactory(ResponseService)



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

        #log.info(registry.LCStates)
        res = dataobject.StatefulResource.create_new_resource()
        #log.info(res.get_lifecyclestate())
        self.assertEqual(res.lifecycle, dataobject.LCStates.new)

        res.set_lifecyclestate(dataobject.LCStates.active)
        self.assertEqual(res.lifecycle, dataobject.LCStates.active)
        #log.info(res.get_lifecyclestate())

        res.set_lifecyclestate(dataobject.LCStates['retired'])
        self.assertEqual(res.lifecycle, dataobject.LCStates.retired)
        #log.info(res.get_lifecyclestate())

        self.failUnlessRaises(TypeError,res.set_lifecyclestate,'new')

    def test_get_lcstate(self):

        res = dataobject.StatefulResource.create_new_resource()

        self.assertEqual(res.get_lifecyclestate(),dataobject.LCState('new'))


class TestDEncoder(unittest.TestCase):
    """
    """
