




class IonFiniteStateMachine():
    """
    """


    def __init__(self, states, events, state_handlers):

        self.states = states
        self.events = events
        self.state_handlers = state_handlers
        self.current_state = None
        self.previous_state = None


    def get_current_state(self):
        return self.current_state


    def start(self,state,params=None):
        
        
        if state not in self.states:
            return False
        
        self.current_state = state
        self.state_handlers[self.current_state]('EVENT_ENTER',params)
        return True

    def on_event(self,event,params=None):

        
        (success,next_state) = self.state_handlers[self.current_state](event,params)
        
        
        if next_state in self.states:
            self.on_transition(next_state,params)
                
        return success
            

            
    def on_transition(self,next_state,params):
        
        
        self.state_handlers[self.current_state]('EVENT_EXIT',params)
        self.previous_state = self.current_state
        self.current_state = next_state
        self.state_handlers[self.current_state]('EVENT_ENTER',params)

        






    
    
    
    