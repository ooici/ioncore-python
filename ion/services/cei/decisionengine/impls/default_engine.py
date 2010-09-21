import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import random

from ion.services.cei.decisionengine import Engine
from ion.services.cei.epucontroller import LaunchItem
import ion.services.cei.states as InstanceStates


class DefaultEngine(Engine):
    """
    A decision engine that makes sure there is exactly N instances of a
    deployable type.
    
    """
    
    def __init__(self):
        super(DefaultEngine, self).__init__()
        
        # todo: get all of this from conf
        self.preserve_n = 2
        self.available_allocations = ["small"]
        self.available_sites = ["ec2-east"]
        self.available_types = ["epu_work_consumer"]
        
    def initialize(self, control, state, conf=None):
        """Engine API method"""
        # todo: need central constants for these key strings
        parameters = {"timed-pulse-irregular":2500}
        if conf and conf.has_key("force_site"):
            self.available_sites = [conf["force_site"]]
            
        control.configure(parameters)

    def decide(self, control, state):
        """Engine API method"""
        all_instance_lists = state.get_all("instance-state")
        
        bad_states = [InstanceStates.TERMINATING, InstanceStates.TERMINATED, InstanceStates.FAILED]
        valid_count = 0
        for instance_list in all_instance_lists:
            ok = True
            for state_item in instance_list:
                if state_item.value in bad_states:
                    ok = False
                    break
            if ok:
                valid_count += 1
        
        while valid_count < self.preserve_n:
            self._launch_one(control)
            valid_count += 1
            
    def _launch_one(self, control):
        log.info("Requesting instance")
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
        
    
