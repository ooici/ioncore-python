import logging
import os
import signal
import sys
import time
import uuid

from ion.services.cei.decisionengine import EngineLoader
from ion.services.cei.epucontroller import Control
from ion.services.cei.epucontroller import State
from ion.services.cei.epucontroller import StateItem
    
# -------
# HARNESS
# -------

class DecisionEngineExerciser(object):
    """
    This is a standalone controller which provides a 'mock' environment for
    running a decision engine.
    
    """
    
    def __init__(self, engineclass):
        self.continue_running = True
        self.engine = EngineLoader().load(engineclass)
        self.state = DeeState()
        self.control = DeeControl(self.state)
    
    def run_forever(self):
        """Initialize the decision engine and call 'decide' until killed.""" 
        
        conf = {}
        self.engine.initialize(self.control, self.state, conf)
        while self.continue_running:
            time.sleep(self.control.sleep_seconds)
            self.engine.decide(self.control, self.state)
        logging.warn("Controller is exiting")
    
    def crashing(self):
        """Experiment with crash scenarios (should the engine API change?)"""
        self.continue_running = False


# ----------------------
# CONTROLLER API OBJECTS
# ----------------------

class DeeControl(Control):
    def __init__(self, deestate):
        super(DeeControl, self).__init__()
        self.sleep_seconds = 5.0
        self.deestate = deestate
    
    def configure(self, parameters):
        """Control API method"""
        if parameters and parameters.has_key("timed-pulse-irregular"):
            sleep_ms = int(parameters["timed-pulse-irregular"])
            self.sleep_seconds = sleep_ms / 1000.0
        logging.info("Control is configured")
    
    def launch(self, deployable_type_id, launch_description):
        """Control API method"""
        launch_id = uuid.uuid4()
        logging.info("Request for DP '%s' is a new launch with id '%s'" % (deployable_type_id, launch_id))
        for group,item in launch_description.iteritems():
            logging.info(" - %s is %d %s from %s" % (group, item.num_instances, item.allocation_id, item.site))
            for i in range(item.num_instances):
                item.instance_ids.append(uuid.uuid4())
        return (launch_id, launch_description)
    
    def destroy_instances(self, instance_list):
        """Control API method"""
        raise NotImplementedError
    
    def destroy_launch(self, launch_id):
        """Control API method"""
        raise NotImplementedError
        

class DeeState(State):
    def __init__(self):
        super(DeeState, self).__init__()
        self.instance_states = {}
        self.queue_lengths = {}
        
    def get_all(typename):
        if typename == "instance-state":
            data = self.instance_states
        elif typename == "queue-length":
            data = self.queue_lengths
        else:
            raise KeyError("Unknown typename: '%s'" % typename)
        
        return data.values()
    
    def get(typename, key):
        if typename == "instance-state":
            data = self.instance_states
        elif typename == "queue-length":
            data = self.queue_lengths
        else:
            raise KeyError("Unknown typename: '%s'" % typename)
        
        ret = []
        if data.has_key(key):
            ret.append(data[key])
        return ret


# ---------------
# SIGNAL HANDLING
# ---------------

def getcontroller():
    try:
        _controller
    except:
        return None
    return _controller
    
def setcontroller(controller):
    global _controller
    _controller = controller

def sigint_handler(signum, frame):
    logging.critical("The sky is falling.")
    try:
        controller = getcontroller()
        if controller:
            controller.crashing()
    except:
        exception_type = sys.exc_type
        try:
            exceptname = exception_type.__name__
        except AttributeError:
            exceptname = exception_type
        err = "Problem: %s: %s" % (str(exceptname), str(sys.exc_value))
        logging.error(err)
    os._exit(2)
    
# ----
# MAIN
# ----

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print >>sys.stderr, "ERROR, expecting argument: 'package.package.class' of decision engine to run."
        sys.exit(1)
    logging.basicConfig(level=logging.DEBUG, \
                format='%(asctime)s %(levelname)s [%(funcName)s] %(message)s')
    signal.signal(signal.SIGINT, sigint_handler)
    dee = DecisionEngineExerciser(sys.argv[1])
    setcontroller(dee)
    dee.run_forever()
