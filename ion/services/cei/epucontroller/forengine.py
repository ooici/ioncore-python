class Control(object):
    """
    This is the superclass for any implementation of the control object that
    is passed to the decision engine.  The control object is a way for the
    engine to make requests to the EPU Controller, like to send launch
    requests to the Provisioner.
    
    The abc (abstract base class) module is not present in Python 2.5 but 
    Control should be treated as such.  It is not meant to be instantiated
    directly.
    
    """
    
    def __init__(self):
        pass
    
    def configure(self, parameters):
        """
        Give the engine the opportunity to offer input about how often it
        should be called or what specific events it would always like to be
        triggered after.
        
        See the decision engine implementer's guide for specific configuration
        options.
        
        @retval None
        @exception Exception illegal/unrecognized input
        
        """
        raise NotImplementedError
    
    def launch(self, deployable_type_id, launch_description):
        """
        Choose instance IDs for each instance desired, a launch ID and send
        appropriate message to Provisioner.
        
        @param deployable_type_id string identifier of the DP to launch
        @param launch_description See engine implementer's guide
        @retval tuple (launch_id, launch_description), see guide
        @exception Exception illegal input
        @exception Exception message not sent
        
        """
        raise NotImplementedError
    
    def destroy_instances(self, instance_list):
        """
        Terminate particular instances.
        
        @param instance_list list size >0 of instance IDs to terminate
        @retval None
        @exception Exception illegal input/unknown ID(s)
        @exception Exception message not sent
        
        """
        raise NotImplementedError
    
    def destroy_launch(self, launch_id):
        """
        Terminate an entire launch.
        
        @param launch_id launch to terminate
        @retval None
        @exception Exception illegal input/unknown ID
        @exception Exception message not sent
        
        """
        raise NotImplementedError

class LaunchItem(object):
    """
    Values of the launch_description dict are of this type
    It has simple Python attributes (no property decorators).
    The instance_ids list is not populated when the object is created.
    
    """
    
    def __init__(self, num_instances, allocation_id, site, data):
        """
        @param num_instances integer, count of nodes necessary
        @param allocation_id string (Provisioner must recognize)
        @param site string, where to launch (Provisioner must recognize)
        @param data dict of arbitrary k/v passed to node via contextualization 
        
        """
        
        # TODO: validation
        self.num_instances = int(num_instances)
        self.allocation_id = str(allocation_id)
        self.site = str(site)
        self.data = data
        self.instance_ids = []


class State(object):
    """
    This is the superclass for any implementation of the state object that
    is passed to the decision engine.  The state object is a way for the
    engine to find out relevant information that has been collected by the
    EPU Controller.
    
    The abc (abstract base class) module is not present in Python 2.5 but 
    State should be treated as such.  It is not meant to be instantiated
    directly.
    
    One can think of this as a collection of three dimensional tables, one
    three dimensional table for each type of StateItem.

    For each *type* of StateItem, there is a collection of data points for
    each unique *key* differentiated by *time*
    
    """
    
    def __init__(self):
        pass
    
    def get_all(self, typename):
        """
        Get all data about a particular type.
        
        @retval list(StateItem) StateItem instances that match the type
        or an empty list if nothing matches.
        @exception KeyError if typename is unknown
        
        """
        raise NotImplementedError
    
    def get(self, typename, key):
        """
        Get all data about a particular key of a particular type.
        
        @retval list(StateItem) StateItem instances that match the key query
        or an empty list if nothing matches.
        @exception KeyError if typename is unknown
        
        """
        raise NotImplementedError


class StateItem(object):
    """
    One data reading that the EPU Controller knows about.
    It has simple Python attributes (no property decorators).
    
    """
    
    def __init__(self, typename, key, time, value):
        """
        @param typename type of StateItem, well-known unique string (see guide)
        @param key unique identifier, depends on the type
        @param time integer,  unixtime data was obtained by EPU Controller
        @param value arbitrary object
        
        """
        
        # TODO: validation
        self.typename = str(typename)
        self.key = str(key)
        self.time = int(time)
        self.value = value
