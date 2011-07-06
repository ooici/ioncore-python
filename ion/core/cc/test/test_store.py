
from twisted.internet import defer
from twisted.trial import unittest

from ion.core.cc.store import Store

class StoreTest(unittest.TestCase):

    def setUp(self):
        self.store = Store()
        self.store.kvs = {'foo':'bar'}

    @defer.inlineCallbacks
    def test_read(self):
        a = yield self.store.read('foo')
        self.assertEquals(a, 'bar')

    @defer.inlineCallbacks
    def test_write(self):
        yield self.store.write('doo', 'daa')
        a = yield self.store.read('doo')
        self.assertEquals(a, 'daa')

    @defer.inlineCallbacks
    def test_delete(self):
        yield self.store.delete('foo')
        a = yield self.store.read('foo')
        self.assertEquals(a, None)

    @defer.inlineCallbacks
    def test_query(self):
        yield self.store.query('f') #this is bs
        #The implementation doesn't really do anything
        #as long as this doesn't error, it passes
        

