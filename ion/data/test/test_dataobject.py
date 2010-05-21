#!/usr/bin/env python

from twisted.trial import unittest
from twisted.internet import defer
import logging
from ion.data.dataobject import DataObject

# To Test messages using DataObjects
from ion.test.iontest import IonTestCase
from twisted.internet import defer

from magnet.spawnable import Receiver
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient


class DataObjectTest(unittest.TestCase):
    """Testing service classes of resource registry
    """
#    def setUp(self):
#        pass
    
    def test_dataobject(self):
        
        dobj = DataObject()

        dobj.set_attr('thing1','thing2')
        self.assertIsInstance(dobj.__dict__, dict)
        
        self.assertIsInstance(dobj.encode(),dict)
        
        self.assertEqual(dobj.get_attr('thing1'),'thing2')
        
        # test a simple dictionary
        d = dict()
        d['a']=1
        d['b']='b'
        d['c']=3.14159
        
        dobj.decode(d)
        
        self.assertEqual(dobj.get_attr('a'),1)
        self.assertEqual(dobj.get_attr('b'),'b')
        self.assertEqual(dobj.get_attr('c'),3.14159)
        
        dobj2=DataObject.from_encoding(d)
        
        self.assertEqual(dobj2.get_attr('a'),1)
        self.assertEqual(dobj2.get_attr('b'),'b')
        self.assertEqual(dobj2.get_attr('c'),3.14159)
        
        self.assertIdentical(dobj,dobj)

        # The Uniquie ID makes these two not equal
        self.assertNotEqual(dobj,dobj2)
        
        self.assertEqual(dobj2,DataObject.from_encoding(dobj2.encode()))
       
        self.assertRaises(AssertionError,dobj.set_attr, 6, 6) 
        
        # Put a list, tuple, dict, DataObject and set in a dictionary

        # List
        d = dict()
        l=[1,'f',3.4]
        d['a']=l
        
        dobj3=DataObject.from_encoding(d)
        
        self.assertIsInstance(dobj3.get_attr('a'),list)
        self.assertEqual(dobj3.get_attr('a'),l)
        
        # For now, Assuming Lists are protected Blobs!
        ## Can't put complex things in a list
        #lx = list(l)
        #lx.append({6:5})
        #dlx={'a':lx}
        #self.assertRaises(AssertionError,DataObject.from_encoding,dlx)
      
        
        # Tuple
        t=('t','u','p','l','e')
        dt={'b':t}
        
        self.assertRaises(AssertionError,DataObject.from_encoding,dt)
        #dobj3=DataObject.from_encoding(dt)
        ## Made Tuple an error!
        #self.assertIsInstance(dobj3.get_attr('b'),tuple)
        #self.assertEqual(dobj3.get_attr('b'),t)

        # Can't use tuple as Dict Key in DataObject
        dt={'b':t,(9,8):5}
        self.assertRaises(AssertionError,DataObject.from_encoding,dt)

        
        # Set
        s=set()
        for i in t:
            s.add(i)
        ds={'s':s}
        self.assertRaises(AssertionError,DataObject.from_encoding,ds)
        
        #self.assertIsInstance(dobj3.get_attr('c'),set)
        #self.assertEqual(dobj3.get_attr('c'),s)
        
        # Dictionary in a dictionary
        e=dict()
        f=dict()
        e['d']=d
        f['e']=e
        dobj3=DataObject.from_encoding(f)
        
        self.assertIsInstance(dobj3.get_attr('e'),DataObject)
        de=dobj3.get_attr('e')
        self.assertIsInstance(de.get_attr('d'),DataObject)
        dd=de.get_attr('d')
        self.assertEqual(dd.get_attr('a'),l)

        # Data Object
        a=DataObject.from_encoding(d)
        b=DataObject.from_encoding(d)
        c=DataObject.from_encoding(d)
        
        do=DataObject()
        do.set_attr('a',a)
        do.set_attr('b',b)
        do.set_attr('c',c)

        self.assertIsInstance(do.get_attr('a'),DataObject)
        self.assertEqual(do.get_attr('a'),a)
        print 'obja',a
        print 'objref',do.get_attr('a')

        blob = do.encode()   

        d0 = DataObject.from_encoding(blob)
        
        #print '===a',d0.get_attr('a').__dict__
        #print '===b',d0.get_attr('b').__dict__
        #print '===c',d0.get_attr('c').__dict__
        
        self.assertEqual(d0.get_attr('a'),a)

        # Recover the tuple
#        dt=d0.get_attr('a')
#        ti=dt.get_attr('b')
#        self.assertEqual(ti,t)
        
        # Recover the set
#        ds=d0.get_attr('a')
#        si=dt.get_attr('c')
#        self.assertEqual(si,s)
        
        self.assertEqual(do,d0)
        
        # Test Memory leak issue:
        # http://code.activestate.com/recipes/52308-the-simple-but-handy-collector-of-a-bunch-of-named/
        
        for i in range(10**2): # try 10**9 
            a = DataObject()
            
            
            

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
        
        print 'Type is: ', type(content)
        assert type(content) is dict
        
        dobj = DataObject.from_encoding(content)

        # The following line shows how to reply to a message
        yield self.reply(msg, 'reply', dobj.encode(), {})

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
    def send_data_object(self, dobj):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('respond', dobj.encode(), {})
        logging.info('Responder replied: '+str(content))
        do = DataObject.from_encoding(content)    
        defer.returnValue(do)

# Spawn of the process using the module name
factory = ProtocolFactory(ResponseService)



class Send_Data_Object_Test(IonTestCase):
    """Testing service classes of resource registry
    """
    @defer.inlineCallbacks
    def setUp(self):
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

        rs = ResponseServiceClient(sup)
        
        # Simple Send and Check value:
        do = yield rs.send_data_object(DataObject.from_encoding({'test':'value'}))
        self.assertEqual(do.get_attr('test'),'value')
        
        
        t=(unicode('t'),unicode('u'),unicode('p'),unicode('l'),unicode('e'),3)
        l=[4,'a',15,'opq']
        d={'a':5,'b':3.14159}
        s=set()
        s.add(5)
        s.add('Cat')
        dct={
#            'test1':t,
            'test2':d,
            'test3':l}
#            'test4':s}

        dsend = DataObject.from_encoding(dct)
                         
                         
        dreceive = yield rs.send_data_object(dsend)
        
        # Can't allow tuples because it breaks DataObject comparison        
        ## Allow passing tuples, but they come out as lists!
        #self.assertIsInstance(dreceive.get_attr('test1'),list)
        #self.assertEqual(dreceive.get_attr('test1'),list(t))        
        self.assertEqual(dreceive,dsend)
        self.assertEqual(dreceive.get_attr('test2'),dsend.get_attr('test2'))
        self.assertEqual(dreceive.get_attr('test3'),l)
#        self.assertEqual(dreceive.get_attr('test4'),s)

                         