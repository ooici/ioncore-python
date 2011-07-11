from twisted.internet import defer

class ExchangeTypes:

    def __init__(self, controller):
        self.controller = controller
    
    @defer.inlineCallbacks
    def create_exchange_point(self, exchangespace, name):
        yield self.controller.create_exchange(
                 exchange=exchangespace + '.' + name,
                 type='topic', 
                 passive=False, 
                 durable=False,
                 auto_delete=True, 
                 internal=False, 
                 nowait=False        
        )
