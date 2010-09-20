import logging
import random

from ion.services.cei.decisionengine import Engine
from ion.services.cei.epucontroller import LaunchItem
import ion.services.cei.states as InstanceStates

BAD_STATES = [InstanceStates.TERMINATING, InstanceStates.TERMINATED, InstanceStates.FAILED]

CONF_HIGH_WATER = "queuelen_high_water"
CONF_LOW_WATER = "queuelen_low_water"
CONF_MIN_INSTANCES = "min_instances"

class QueueLengthBoundedEngine(Engine):
    """
    A decision engine that looks at queue length.  If there are more queued
    messages than the maximum, it will launch compensation.  If the queue
    falls below the given minimum, it will contract instances (unless there
    is only one instance left).
    """
    
    def __init__(self):
        super(QueueLengthBoundedEngine, self).__init__()
        self.high_water = 0
        self.low_water = 0
        self.min_instances = 0
        # todo: get all of this from conf:
        self.available_allocations = ["small"]
        self.available_sites = ["ec2-east"]
        self.available_types = ["epu_work_consumer"]
        
    def initialize(self, control, state, conf=None):
        """Engine API method"""
        # todo: need central constants for these key strings
        parameters = {"timed-pulse-irregular":5000}
        if conf and conf.has_key("force_site"):
            self.available_sites = [conf["force_site"]]
        if not conf:
            raise Exception("cannot initialize without external configuration")
        
        if not conf.has_key(CONF_HIGH_WATER):
            raise Exception("cannot initialize without %s" % CONF_HIGH_WATER)
        if not conf.has_key(CONF_LOW_WATER):
            raise Exception("cannot initialize without %s" % CONF_LOW_WATER)
        self.high_water = int(conf[CONF_HIGH_WATER])
        self.low_water = int(conf[CONF_LOW_WATER])
        
        if conf.has_key(CONF_MIN_INSTANCES):
            self.min_instances = int(conf[CONF_MIN_INSTANCES])
            if self.min_instances < 0:
                raise Exception("cannot have negative %s conf" % CONF_MIN_INSTANCES)
        else:
            self.min_instances = 0
        
        logging.info("Bounded queue length engine initialized, high water mark is %d, low water %d" % (self.high_water, self.low_water))
        
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
        
        
        # If there is an explicit minimum, always respect that.
        if valid_count < self.min_instances:
            logging.info("Bringing instance count up to the explicit minimum")
            while valid_count < self.min_instances:
                self._launch_one(control)
                valid_count += 1
        
        
        # Won't make a decision if there are pending instances. This would
        # need to be a lot more elaborate (requiring a datastore) to get a
        # faster response time whilst not grossly overcompensating. 
        any_pending = False
        for instance_list in all_instance_lists:
            # "has it contextualized at some point in its life?"
            found_started = False
            for state_item in instance_list:
                if state_item.value == InstanceStates.RUNNING:
                    found_started = True
                    break
            if not found_started:
                any_pending = True
        
        if any_pending:
            logging.debug("Will not analyze with pending instances")
            return
        
        heading = self._heading(state, valid_count)
        if heading > 0:
            self._launch_one(control)
            valid_count += 1
        elif heading < 0:
            instanceid = self._pick_instance_to_die(all_instance_lists)
            if not instanceid:
                logging.error("There are no valid instances to terminate")
            else:
                self._destroy_one(control, instanceid)
                valid_count -= 1
        
        txt = "instance"
        if valid_count != 1:
            txt += "s"
        logging.debug("Aware of %d running/starting %s" % (valid_count, txt))
            
    def _heading(self, state, valid_count):
        all_qlens = state.get_all("queue-length")
        # should only be one queue reading for now:
        if len(all_qlens) == 0:
            logging.debug("no queuelen readings to analyze")
            return 0
        
        if len(all_qlens) != 1:
            raise Exception("multiple queuelen readings to analyze?")
        
        qlens = all_qlens[0]
        
        if len(qlens) == 0:
            logging.debug("no queuelen readings to analyze")
            return 0
            
        recent = qlens[-1].value
        msg = "most recent qlen reading is %d" % recent
        
        if recent == 0 and valid_count == 0:
            logging.debug(msg + " (empty queue and no instances)")
            return 0
        
        # If there are zero started already and a non-zero qlen, start one
        # even if it is below the low water mark.  Work still needs to be
        # drained by one instance.
        if recent != 0 and valid_count == 0:
            logging.debug(msg + " (non-empty queue and no instances yet)")
            return 1
        
        if recent > self.high_water:
            logging.debug(msg + " (above high water)")
            return 1
        elif recent < self.low_water:
            logging.debug(msg + " (below low water)")
            if valid_count == self.min_instances:
                if self.min_instances == 1:
                    txt = "1 instance"
                else:
                    txt = "%d instances" % self.min_instances
                logging.info("Down to %s, cannot reduce" % txt)
                return 0
            else:
                return -1
        else:
            logging.debug(msg + " (inside bounds)")
            return 0
            
    def _launch_one(self, control):
        logging.info("Requesting instance")
        launch_description = {}
        launch_description["work_consumer"] = \
                LaunchItem(1, self._allocation(), self._site(), None)
        control.launch(self._deployable_type(), launch_description)
    
    def _pick_instance_to_die(self, all_instance_lists):
        # filter out instances that are in terminating state or 'worse'
        
        candidates = []
        for instance_list in all_instance_lists:
            ok = True
            for state_item in instance_list:
                if state_item.value in BAD_STATES:
                    ok = False
                    break
            if ok:
                candidates.append(state_item.key)
        
        logging.debug("Found %d instances that could be killed:\n%s" % (len(candidates), candidates))
        
        if len(candidates) == 0:
            return None
        elif len(candidates) == 1:
            return candidates[0]
        else:
            idx = random.randint(0, len(candidates)-1)
            return candidates[idx]
    
    def _destroy_one(self, control, instanceid):
        logging.info("Destroying an instance ('%s')" % instanceid)
        instance_list = [instanceid]
        control.destroy_instances(instance_list)
        
    def _deployable_type(self):
        return self.available_types[0]
        
    def _allocation(self):
        return self.available_allocations[0]
        
    def _site(self):
        return self.available_sites[0]
