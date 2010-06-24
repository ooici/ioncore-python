
import uuid


import logging
logging = logging.getLogger(__name__)

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

    def test_lcstate(self):
        
        logging.info(registry.LCStates)
        
        res = registry.Generic()
        logging.info(res.get_lifecyclestate())
        
        res.set_lifecyclestate(registry.LCStates.active)
        logging.info(res.get_lifecyclestate())
        res.set_lifecyclestate(registry.LCStates['retired'])
        logging.info(res.get_lifecyclestate())



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

    @defer.inlineCallbacks
    def test_register_get_list(self):
        res_id = uuid.uuid4().hex
        res = registry.ResourceDescription()
        res.name = 'foo'
        id = yield self.reg.register(res_id, res)

        res_id = uuid.uuid4().hex
        res.name = 'moo'
        id = yield self.reg.register(res_id, res)

        res_list = yield self.reg.list()
        #print res_list
        
        self.assertIn(res_id, res_list)
        
        res_s = yield self.reg.list_descriptions()
        for res in res_s:
            logging.info( str(res))
            
        self.assertIn(res, res_s)
        self.assertNotEqual(res_s[1],res_s[0])
        


