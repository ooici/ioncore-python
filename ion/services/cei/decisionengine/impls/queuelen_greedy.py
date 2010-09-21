import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import random

from ion.services.cei.decisionengine import Engine
from ion.services.cei.epucontroller import LaunchItem
import ion.services.cei.states as InstanceStates

BAD_STATES = [InstanceStates.TERMINATING, InstanceStates.TERMINATED, InstanceStates.FAILED]

class QueueLengthGreedyEngine(Engine):
    """
    A decision engine that makes sure there is one worker instance for each
    new job.  This is not a practical engine since it does not contract (see
    QueueLengthBoundedEngine for that) but it is useful for testing scale
    and other such "spendy" applications.
    
    """
    
    def __init__(self):
        super(QueueLengthGreedyEngine, self).__init__()
        
        # todo: get all of this from conf
        self.available_allocations = ["small"]
        self.available_sites = ["ec2-east"]
        self.available_types = ["epu_work_consumer"]
        
    def initialize(self, control, state, conf=None):
        """Engine API method"""
        # todo: need central constants for these key strings
        parameters = {"timed-pulse-irregular":5000}
        if conf and conf.has_key("force_site"):
            self.available_sites = [conf["force_site"]]
        control.configure(parameters)

    def decide(self, control, state):
        """Engine API method"""
        all_instance_lists = state.get_all("instance-state")
        
        valid_count = 0
        for instance_list in all_instance_lists:
            ok = True
            for state_item in instance_list:
                if state_item.value in BAD_STATES:
                    ok = False
                    break
            if ok:
                valid_count += 1
        
        #log.debug("Before: %s" % self._aware_txt(valid_count))
        
        qlen = self._getqlen(state)
        #log.debug("most recent qlen reading is %d" % qlen)
        while valid_count < qlen:
            self._launch_one(control)
            valid_count += 1
        
        log.debug("After: %s" % self._aware_txt(valid_count))
        
    def _aware_txt(self, valid_count):
        txt = "instance"
        if valid_count != 1:
            txt += "s"
        return "aware of %d running/starting %s" % (valid_count, txt)
        
    def _getqlen(self, state):
        all_qlens = state.get_all("queue-length")
        # should only be one queue reading for now:
        if len(all_qlens) == 0:
            log.debug("no queuelen readings to analyze")
            return 0
        
        if len(all_qlens) != 1:
            raise Exception("multiple queuelen readings to analyze?")
        
        qlens = all_qlens[0]
        
        if len(qlens) == 0:
            log.debug("no queuelen readings to analyze")
            return 0
            
        return qlens[-1].value
            
    def _launch_one(self, control):
        launch_description = {}
        launch_description["work_consumer"] = \
                LaunchItem(1, self._allocation(), self._site(), None)
        control.launch(self._deployable_type(), launch_description)
        
    def _deployable_type(self):
        return self.available_types[0]
        
    def _allocation(self):
        return self.available_allocations[0]
        
    def _site(self):
        return self.available_sites[0]
