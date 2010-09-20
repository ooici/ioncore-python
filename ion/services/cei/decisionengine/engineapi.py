class Engine(object):
    """
    This is the superclass for any implementation of the state object that
    is passed to the decision engine.  The state object is a way for the
    engine to find out relevant information that has been collected by the
    EPU Controller.
    
    The abc (abstract base class) module is not present in Python 2.5 but 
    Engine should be treated as such.  It is not meant to be instantiated
    directly.
    
    @note See the decision engine implementer's guide for more information.
    
    """
    
    def __init__(self):
        pass
    
    def initialize(self, control, state, conf=None):
        """
        Give the engine a chance to initialize.  The current state of the
        system is given as well as a mechanism for the engine to offer the
        controller input about how often it should be called.
        
        @note Must be invoked and return before the 'decide' method can
        legally be invoked.
        
        @param control instance of Control, used to request changes to system
        @param state instance of State, used to obtain any known information 
        @param conf None or dict of key/value pairs
        @exception Exception if engine cannot reach a sane state
        
        """
        raise NotImplementedError

    def decide(self, control, state):
        """
        Give the engine a chance to act on the current state of the system.
        
        @note May only be invoked once at a time.  
        @note When it is invoked is up to EPU Controller policy and engine
        preferences, see the decision engine implementer's guide.
        
        @param control instance of Control, used to request changes to system
        @param state instance of State, used to obtain any known information 
        @retval None
        @exception Exception if the engine has been irrevocably corrupted
        
        """
        raise NotImplementedError

