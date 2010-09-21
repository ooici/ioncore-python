import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import random

from ion.services.cei.decisionengine import Engine
from ion.services.cei.epucontroller import LaunchItem

class InfiniteEngine(Engine):
    """
    A decision engine that decides to launch a new VM instance every 5th
    "decide" call.
    
    """
    
    def __init__(self):
        super(InfiniteEngine, self).__init__()
        self.call_count = 0
        self.available_allocations = ["small", "medium", "large", "supersized"]
        self.available_sites = ["McDonalds", "Burger King", "Wendy's"]
        self.available_types = ["Happy Meal", "Double Chzbrgr", "Salad"]
        
    def initialize(self, control, state, conf=None):
        """Engine API method"""
        parameters = {"timed-pulse-irregular":800}
        control.configure(parameters)

    def decide(self, control, state):
        """Engine API method"""
        self.call_count += 1
        if self.call_count == 1:
            log.debug("Thought about this 1 time.")
        else:
            log.debug("Thought about this %d times." % self.call_count)
        if self.call_count % 5 == 0:
            log.info("Requesting new instance")
            self._launch_one(control)
            
    def _launch_one(self, control):
        launch_description = {}
        launch_description["head-node"] = \
                LaunchItem(1, self._allocation(), self._site(), None)
        numworkers = random.randint(3,30)
        launch_description["worker-nodes"] = \
                LaunchItem(numworkers, self._allocation(), self._site(), None)
        control.launch(self._deployable_type(), launch_description)
        
    def _deployable_type(self):
        idx = random.randint(0, len(self.available_types)-1)
        return self.available_types[idx]
        
    def _allocation(self):
        idx = random.randint(0, len(self.available_allocations)-1)
        return self.available_allocations[idx]
        
    def _site(self):
        idx = random.randint(0, len(self.available_sites)-1)
        return self.available_sites[idx]
        
    