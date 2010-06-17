
import uuid

from twisted.internet import defer
from twisted.trial import unittest

from ion.data import store
from ion.data.datastore import registry

class RegistryTest(unittest.TestCase):
    """
    """

    @defer.inlineCallbacks
    def setUp(self):
        s = yield store.Store.create_store()
        self.reg = registry.ResourceRegistry(s)

    @defer.inlineCallbacks
    def test_register(self):
        res_id = uuid.uuid4().hex
        res = registry.ResourceDescription()
        res.name = 'foo'
        id = yield self.reg.register(res_id, res)
        res2 = yield self.reg.get_description(res_id)
        self.failUnless(res == res2)

    @defer.inlineCallbacks
    def test_register_overwrite(self):
        res_id = uuid.uuid4().hex
        res = registry.ResourceDescription()
        res.name = 'foo'
        id = yield self.reg.register(res_id, res)

        resn = registry.ResourceDescription()
        resn.name = 'moo'
        id = yield self.reg.register(res_id, resn)

        res2 = yield self.reg.get_description(res_id)
        self.failUnless(resn == res2)

