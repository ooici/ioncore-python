from twisted.trial import unittest
import datetime

import os
import time

from ion.services.cei.decisionengine.test.mockcontroller import DeeControl
from ion.services.cei.decisionengine.test.mockcontroller import DeeState
from ion.services.cei.decisionengine import EngineLoader

import ion.services.cei.states as InstanceStates
BAD_STATES = [InstanceStates.TERMINATING, InstanceStates.TERMINATED, InstanceStates.FAILED]

from ion.services.cei.epucontroller import PROVISIONER_VARS_KEY

ENGINE="ion.services.cei.decisionengine.impls.NpreservingEngine"

class NPreservingEngineTestCase(unittest.TestCase):

    def setUp(self):
        self.engine = EngineLoader().load(ENGINE)
        self.state = DeeState()
        self.state.new_qlen(0)
        self.control = DeeControl(self.state)

    def tearDown(self):
        pass


    # -----------------------------------------------------------------------
    # Basics
    # -----------------------------------------------------------------------
    
    def test_preserve_0(self):
        conf = {'preserve_n':'0'}
        self.engine.initialize(self.control, self.state, conf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 0

    def test_preserve_1(self):
        conf = {'preserve_n':'1'}
        self.engine.initialize(self.control, self.state, conf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1

    def test_preserve_N(self):
        conf = {'preserve_n':'5'}
        self.engine.initialize(self.control, self.state, conf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 5


    # -----------------------------------------------------------------------
    # PreserveN Reconfiguration
    # -----------------------------------------------------------------------
        
    def test_reconfigure1(self):
        conf = {'preserve_n':'0'}
        self.engine.initialize(self.control, self.state, conf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 0
        
        newconf = {'preserve_n':'1'}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        
        newconf2 = {'preserve_n':'0'}
        self.engine.reconfigure(self.control, newconf2)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 0
        
    def test_reconfigure2(self):
        for n in range(20):
            newconf = {'preserve_n':n}
            self.engine.reconfigure(self.control, newconf)
            self.engine.decide(self.control, self.state)
            assert self.control.num_launched == n
        for n in reversed(range(20)):
            newconf = {'preserve_n':n}
            self.engine.reconfigure(self.control, newconf)
            self.engine.decide(self.control, self.state)
            assert self.control.num_launched == n

    def test_bad_reconfigure1(self):
        conf = {'preserve_n':'asd'}
        try:
            self.engine.initialize(self.control, self.state, conf)
        except ValueError:
            return
        assert False
        
    def test_bad_reconfigure2(self):
        conf = {'preserve_n':'-1'}
        try:
            self.engine.initialize(self.control, self.state, conf)
        except ValueError:
            return
        assert False


    # -----------------------------------------------------------------------
    # Provisioner Vars Reconfiguration
    # -----------------------------------------------------------------------
        
    def test_provreconfigure1(self):
        newconf = {'preserve_n':'1'}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        assert self.control.prov_vars == None
        
    def test_provreconfigure2(self):
        pvars = {'workerid':'abcdefg'}
        conf = {'preserve_n':'0', PROVISIONER_VARS_KEY:pvars}
        self.engine.initialize(self.control, self.state, conf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 0
        
        # Provisioner vars are not configured initially by the engine itself 
        assert self.control.prov_vars == None
        
    def test_provreconfigure3(self):
        conf = {'preserve_n':'1'}
        self.engine.initialize(self.control, self.state, conf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        assert self.control.prov_vars == None
        
        pvars = {'workerid':'abcdefg'}
        newconf = {'preserve_n':'1', PROVISIONER_VARS_KEY:pvars}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        assert self.control.prov_vars != None
        assert self.control.prov_vars.has_key("workerid")
        

    # -----------------------------------------------------------------------
    # Unique Instance Support
    # -----------------------------------------------------------------------
    
    def _get_iaas_id(self, uniq_id):
        """Return the IaaS ID that is serving as the unique ID in
        question."""
        # This is NOT a standard engine API method
        return self.engine._iaas_id_from_uniq_id(uniq_id)

    def _is_iaas_id_active(self, iaas_id):
        """Return True if the (mock) controller thinks this ID is active,
        return False if it is in a BAD_STATE or if it is not present.
        """
        all_instance_lists = self.control.deestate.get_all("instance-state")
        for instance_list in all_instance_lists:
            one_state_item = instance_list[0]
            if one_state_item.key == iaas_id:
                for state_item in instance_list:
                    if state_item.value in BAD_STATES:
                        print state_item.value
                        return False
                return True
        return False


    # -----------------------------------------------------------------------
    # Unique Instances
    # -----------------------------------------------------------------------
    
    def test_uniques1(self):
        uniq1 = {"akey":"uniq1value"}
        uniqs = {"1":uniq1}
        newconf = {'preserve_n':'1', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        iaas_id = self._get_iaas_id("1")
        assert iaas_id != None
        assert self._is_iaas_id_active(iaas_id)

    def test_uniques2(self):
        uniq1 = {"akey":"uniq1value"}
        uniq2 = {"someotherkey":"uniq2value"}
        uniqs = {"1":uniq1, "2":uniq2}
        newconf = {'preserve_n':'2', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 2
        iaas_id = self._get_iaas_id("1")
        assert iaas_id != None
        assert self._is_iaas_id_active(iaas_id)
        iaas_id = self._get_iaas_id("2")
        assert iaas_id != None
        assert self._is_iaas_id_active(iaas_id)

    def test_bad_unique1(self):
        uniq1 = {"akey":"uniq1value"}
        uniq2 = {"someotherkey":"uniq2value"}
        uniqs = {"1":uniq1, "2":uniq2}
        newconf = {'preserve_n':'1', "unique_instances":uniqs}
        try:
            self.engine.reconfigure(self.control, newconf)
        except Exception:
            return
        assert False

    def test_uniques3(self):
        uniq1 = {"akey":"uniq1value"}
        uniq2 = {"someotherkey":"uniq2value"}
        uniqs = {"1":uniq1, "2":uniq2}
        newconf = {'preserve_n':'2', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 2
        iaas_id = self._get_iaas_id("1")
        assert iaas_id != None
        assert self._is_iaas_id_active(iaas_id)
        iaas_id = self._get_iaas_id("2")
        assert iaas_id != None
        assert self._is_iaas_id_active(iaas_id)
        newconf = {'preserve_n':'1', "unique_instances":uniqs}
        try:
            self.engine.reconfigure(self.control, newconf)
        except Exception:
            return
        assert False

    def test_uniques4(self):
        uniq1 = {"akey":"uniq1value"}
        uniq2 = {"someotherkey":"uniq2value"}
        uniqs = {"1":uniq1, "2":uniq2}
        newconf = {'preserve_n':'2', "unique_instances":uniqs}
        
        # Start two
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 2
        iaas_id = self._get_iaas_id("1")
        assert iaas_id != None
        assert self._is_iaas_id_active(iaas_id)
        iaas_id = self._get_iaas_id("2")
        assert iaas_id != None
        assert self._is_iaas_id_active(iaas_id)
        
        # Remove one
        uniq1 = {"akey":"uniq1value"}
        uniqs = {"1":uniq1}
        newconf = {'preserve_n':'1', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        iaas_id = self._get_iaas_id("1")
        assert iaas_id != None
        assert self._is_iaas_id_active(iaas_id)

    def test_uniques5(self):
        uniq1 = {"akey":"uniq1value"}
        uniqs = {"1":uniq1}
        newconf = {'preserve_n':'1', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        iaas_id = self._get_iaas_id("1")
        assert iaas_id != None
        assert self._is_iaas_id_active(iaas_id)
        
        same_iaas_id = iaas_id
        
        # Replace the variables of same unique instance
        uniq1 = {"akey":"uniq1value2"}
        uniqs = {"1":uniq1}
        newconf = {'preserve_n':'1', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        iaas_id = self._get_iaas_id("1")
        assert iaas_id != None
        assert self._is_iaas_id_active(iaas_id)
        
        # The variable replacement only should not cause a new VM instance
        # to be launched.
        assert iaas_id == same_iaas_id


    # -----------------------------------------------------------------------
    # Generic and Unique Instances combined
    # -----------------------------------------------------------------------
    
    def test_generic_and_unique1(self):
        uniq1 = {"akey":"uniq1value"}
        uniqs = {"1":uniq1}
        newconf = {'preserve_n':'2', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 2
        iaas_id = self._get_iaas_id("1")
        assert iaas_id != None
        assert self._is_iaas_id_active(iaas_id)
        
        
    def test_generic_and_unique2(self):
        uniq1 = {"akey":"uniq1value"}
        uniqs = {"1":uniq1}
        newconf = {'preserve_n':'5', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 5
        iaas_id = self._get_iaas_id("1")
        assert iaas_id != None
        assert self._is_iaas_id_active(iaas_id)

        newconf2 = {'preserve_n':'1'}
        self.engine.reconfigure(self.control, newconf2)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        iaas_id = self._get_iaas_id("1")
        assert iaas_id != None
        assert self._is_iaas_id_active(iaas_id)
        
        newconf3 = {'preserve_n':'2'}
        self.engine.reconfigure(self.control, newconf3)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 2
        iaas_id = self._get_iaas_id("1")
        assert iaas_id != None
        assert self._is_iaas_id_active(iaas_id)
        
        newconf4 = {'preserve_n':'1'}
        self.engine.reconfigure(self.control, newconf4)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        iaas_id = self._get_iaas_id("1")
        assert iaas_id != None
        assert self._is_iaas_id_active(iaas_id)
        

    def test_generic_and_unique3(self):
        uniq1 = {"akey":"uniq1value"}
        uniqs = {"1":uniq1}
        newconf = {'preserve_n':'2', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 2
        iaas_id = self._get_iaas_id("1")
        assert iaas_id != None
        assert self._is_iaas_id_active(iaas_id)
        
        original_iaas_id = iaas_id
        
        uniq1 = {"akey":"uniq1value"}
        uniq2 = {"someotherkey":"uniq2value"}
        uniqs = {"1":uniq1, "2":uniq2}
        newconf = {'preserve_n':'3', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 3
        iaas_id = self._get_iaas_id("1")
        assert iaas_id != None
        assert self._is_iaas_id_active(iaas_id)
        assert original_iaas_id == iaas_id
        iaas_id = self._get_iaas_id("2")
        assert iaas_id != None
        assert self._is_iaas_id_active(iaas_id)

        uniq1 = {"akey":"uniq1value"}
        uniqs = {"1":uniq1}
        newconf = {'preserve_n':'2', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 2
        iaas_id = self._get_iaas_id("1")
        assert iaas_id != None
        assert self._is_iaas_id_active(iaas_id)
        assert original_iaas_id == iaas_id
