from twisted.trial import unittest
import datetime
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import os
import time

from ion.services.cei.decisionengine.test.mockcontroller import DeeControl
from ion.services.cei.decisionengine.test.mockcontroller import DeeState
from ion.services.cei.decisionengine import EngineLoader

ENGINE="ion.services.cei.decisionengine.impls.QueueLengthBoundedEngine"

class QueueLengthBoundedEngineTestCase(unittest.TestCase):

    def setUp(self):
        log.debug("set up")
        self.engine = EngineLoader().load(ENGINE)
        self.state = DeeState()
        self.state.new_qlen(0)
        self.control = DeeControl(self.state)

    def tearDown(self):
        log.debug("tear down")
        pass


    # -----------------------------------------------------------------------

    def test_minimum_0(self):
        conf = {'queuelen_high_water':'50',
                'queuelen_low_water':'10',
                'min_instances':'0'}
        self.engine.initialize(self.control, self.state, conf)
        self.engine.decide(self.control, self.state)

        assert self.control.num_launched == 0

    def test_minimum_1(self):
        conf = {'queuelen_high_water':'50',
                'queuelen_low_water':'10',
                'min_instances':'1'}
        self.engine.initialize(self.control, self.state, conf)
        self.engine.decide(self.control, self.state)

        assert self.control.num_launched == 1

    def test_minimum_N(self):
        conf = {'queuelen_high_water':'50',
                'queuelen_low_water':'10',
                'min_instances':'5'}
        self.engine.initialize(self.control, self.state, conf)
        self.engine.decide(self.control, self.state)

        assert self.control.num_launched == 5

    # -----------------------------------------------------------------------

    def test_the_waters(self):
        conf = {'queuelen_high_water':'50',
                'queuelen_low_water':'10',
                'min_instances':'0'}
        self._the_waters(conf, 0)

    def test_the_waters_1(self):
        conf = {'queuelen_high_water':'50',
                'queuelen_low_water':'10',
                'min_instances':'1'}
        self._the_waters(conf, 1)

    def test_the_waters_N(self):
        conf = {'queuelen_high_water':'50',
                'queuelen_low_water':'10',
                'min_instances':'5'}
        self._the_waters(conf, 5)

    def _the_waters(self, conf, min_instances):

        self.engine.initialize(self.control, self.state, conf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == min_instances

        min_with_work = min_instances
        if min_instances == 0:
            min_with_work = 1

        # Not quite low water, but a conf with zero-minimum should
        # launch one here anyhow
        self.state.new_qlen(9)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == min_with_work

        # low water mark
        self.state.new_qlen(10)
        self.engine.decide(self.control, self.state)
        # should not have changed
        assert self.control.num_launched == min_with_work

        # high water mark
        self.state.new_qlen(50)
        self.engine.decide(self.control, self.state)
        # should not have changed
        assert self.control.num_launched == min_with_work

        # breach!
        self.state.new_qlen(51)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == min_with_work + 1

        # nothing should change
        self.state.new_qlen(40)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == min_with_work + 1

        # nothing should change
        self.state.new_qlen(10)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == min_with_work + 1

        # should contract
        self.state.new_qlen(5)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == min_with_work

        # should go back to minimum
        self.state.new_qlen(0)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == min_instances
