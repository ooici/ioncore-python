import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.services.cei.decisionengine import Engine

class DoNothingEngine(Engine):
    """
    A decision engine that does nothing.  This is useful for rapid
    engine development: launch the infrastructure with this engine
    and ssh in to the controller node and kill/relaunch the controller
    unix process as you develop.
    
    Before you kill the PID, save the twistd command into a shell script:
    
    echo '#!/bin/bash' > relaunch_controller.sh
    chmod +x relaunch_controller.sh
    echo "cd $PWD" >> relaunch_controller.sh
    CONTROLLER_PID=`cat *_epu_controller-service.pid`
    ps -p $CONTROLLER_PID -o cmd h >> relaunch_controller.sh
    
    -------------------------
    
    Change the engine classname in this cfg file to the one you are working on:
        nano -w res/config/*_epu_controller-ionservices.cfg
    
    -------------------------  
    
    kill `cat *_epu_controller-service.pid`
    rm *epu_controller-service.log
    ./relaunch_controller.sh
    
    """
    
    def __init__(self):
        super(DoNothingEngine, self).__init__()
        
    def initialize(self, control, state, conf=None):
        """Engine API method"""
        parameters = {"timed-pulse-irregular":2500}
        control.configure(parameters)

    def decide(self, control, state):
        """Engine API method"""
        instances = state.get_all("instance-state")
        log.debug("Decide called, # of instances: %d" % len(instances))
