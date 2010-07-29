from twisted.internet import reactor, protocol, task

class MyProtocol(protocol.Protocol):
    def connectionMade(self):
        self.factory.clientConnectionMade(self)
    def connectionLost(self, reason):
        self.factory.clientConnectionLost(self)

class MyFactory(protocol.Factory):
    protocol = MyProtocol
    def __init__(self):
        self.clients = []
        self.lc = task.LoopingCall(self.announce)
        self.lc.start(10)

    def announce(self):
        print '10 seconds for server'

print 'Experiment in session'
myfactory = MyFactory()
reactor.run()