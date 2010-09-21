"""
@file spawnable.py
@author Dorian Raymer
@date 03/30/10
"""
import os
import types

from zope.interface import implements
from zope.interface import Interface

from twisted.internet import reactor
from twisted.internet import defer
from twisted.python import reflect
from twisted.python import log
from twisted.plugin import IPlugin

from ion.core.cc import messaging
from ion.core.cc import container
from ion.core.cc.container import Container
from ion.core.cc.container import Id

store = Container.store



class Spawnable(object):
    """
    Spawnable represents something (some module, function, etc.) that can
    be instantiated such that, an instance is an active entity with an
    id/name that other entities can send messages to.

    The Spawnable class (not an instance of it) utilizes a few class
    attributes to remember and manage instances, as well as classmethods
    which act as constructors (the spawn methods) -- developers should not
    need to ever instantiate Spawnable themselves.

    Context:
        Local to container. Instances are managed/remembered by Id, which
        means something only in the context of this container.
    """

    idcount = 0
    progeny = {}

    def __init__(self, target, space):
        """
        @param target
        @param space The message [exchange] space to use (default
        Container.space)
        """
        self.target = target
        self.space = space # message space of this instance's mailbox
        Spawnable.idcount += 1
        self._id = Spawnable.idcount
        self.id = Id(self._id)
        self.name = target.name
        # self.receivers = {}
        self.consumers = []
        self.receiver = None
        # self.parent = parent
        # self.mailbox = defer.DeferredQueue() # maybe.. or maybe just no
        # buffering of messages here

    def info(self):
        """
        @brief attributes of a particular instance
         - name of code module
         - local spawnable id
         - alias names/ bindings to mailbox queue
         - consumer (other besides mailbox)
        """

    @classmethod
    def spawn_f(cls, f, space):
        """
        @brief Spawn an instance running a simple function
        @param f any callable function
        @param space container.MessageSpace instance
        @retval A defer.Deferred which fires with the instance id (container.Id)
        """
        r = Receiver(f.__name__)
        r.handle(f)
        s = cls(r, space)
        r.spawned = s
        cls.progeny[s.id] = s
        d = s.run()
        def _return_id(res, id):
            return id
        d.addErrback(log.err)
        return d.addCallback(_return_id, s.id)

    @classmethod
    def spawn_m(cls, m, space, spawnArgs=None):
        """
        @brief Spawn a python module
        @param m A module (<type 'module'>)
        @param space container.MessageSpace instance
        @param spawnArgs argument dict given to the factory on spawn
        @retval A defer.Deferred which fires with the instance id (container.Id)
        """
        if spawnArgs == None:
            spawnArgs = {}

        if hasattr(m, 'factory'):
            assert isinstance(m.factory, ProtocolFactory), "factory must be instance of ProtocolFactory"
            receiver = m.factory.build(spawnArgs)
            s = cls(receiver, space)
            receiver.spawned = s
            cls.progeny[s.id] = s
            d = s.run()
            def _return_id(res, id):
                return id
            d.addErrback(log.err)
            return d.addCallback(_return_id, s.id)
        elif hasattr(m, 'receiver'):
            # @todo this is a deprecated way of spawning. Remove in the long run
            s = cls(m.receiver, space)
            m.receiver.spawned = s
            cls.progeny[s.id] = s
            d = s.run()
            def _return_id(res, id):
                return id
            d.addErrback(log.err)
            return d.addCallback(_return_id, s.id)
        else:
            raise Exception("Must use Receiver in your module")

    @classmethod
    def spawn_mr(cls, receiver, space):
        """
        @brief Spawn an already instantiated Receiver (probably an instance from a module)
        @param receiver an instance of spawnable.Receiver
        @param space container.MessageSpace instance
        @retval A defer.Deferred which fires with the instance id (container.Id)
        """
        s = cls(receiver, space)
        # receiver.spawned = s
        cls.progeny[s.id] = s
        d = s.run()
        def _return_id(res, id):
            return id
        d.addErrback(log.err)
        return d.addCallback(_return_id, s.id)


    def _pre_run(self):
        """
        """


    def run(self):

        def _cb(consumer):
            """result will be consumer object
            """
            self.consumers.append(consumer)
            return consumer

        def _eb(reason):
            log.err(reason)
            return reason

        d = self.target.makeConsumer(self)

        d.addCallback(_cb)
        d.addErrback(_eb)
        return d


    def _post_run(self):
        """
        """

    def shutdown(self):
        """
        """

    def kill(self):
        """
        """
        for consumer in self.consumers:
            consumer.close()
        del Spawnable.progeny[self.id]

# move these messaging related things to Receiver
    def send(self, msg):
        """
        @param msg instance of carrot.backends.txamqp.Message (Integration)
        @note For message system to send messages to this spawnables mailbox.
        target is the receiver system/application protocol machine
        """
        self.target.deliver(msg.payload, msg)

    def receive(self, data, msg):
        """
        @note This used to be called by the underlying (carrot) messaging system, but
        now that the integration pipeline is in, the send method is what
        deliver-ers call on the targets they deliver to.
        This is now unused/the application could use this to control
        delivery.
        """
        self.target.deliver(data, msg)

    def _send_from(self, to_id, data):
        """
        XXX name convention (routing key/exchange)
        XXX use of header (reply to)
        """

class API(object):
    """Any Spanwable instance's interface to primitive functions
    """

    def __init__(self, id):
        """a Spawnables Id object
        """
        self.id = id


class BaseReceiver(object):
    """like base protocol

    This is where new consumers are made on behalf of an instance.
    Instances use this to choose a consumer to receive on.

    """

    # def __init__(self, name, type):
    def __init__(self):
        """
        @param name name in message space. Spawnable always creates a
        direct name (where name is id)
        @param type messaging pattern used by name
        @brief
        """
        self.name = name
        self.type = type
        self.spawned = None
        self.consumer = None


    def receive(self, consumer):
        """wait for a message on the given consumer
        This is the way a process dictates what consumer to get a message
        from next.
        """

    def send(self, to_id, data):
        self.spawned._send_from(to_id, data)

class Receiver(object):
    """
    This thing interprets message (objects) and figures out which handler
    function (defined by the application) to invoke.
    It is also the mediator between app code, and Spawnable/Container.

    An instance of Receiver uses name of a callable to deliver a message
    from the mailbox.

    @todo This might mest evolve into a Transceiver

    Context:
        This is the messaging perspective of an instance of Spawnable.
    """

    def __init__(self, label, name=None):
        """
        @param label descriptive label of the module or function
        @param name the actual name, if specified. else, use
        spawned id
        """
        self.label = label
        self.name = name
        self.handlers = {}
        self.spawned = None
        self.consumer = None

    @defer.inlineCallbacks
    def makeConsumer(self, spawned):
        self.spawned = spawned
        if not self.name:
            self.name = self.spawned.id.full
            name_config = messaging.process(self.spawned.id.full)
            name_config.update({'name_type':'process'})
            yield store.put(self.spawned.id.full, name_config)
        else:
            name_config = yield store.get(self.name)
            if not name_config:
                raise RuntimeError("Messaging name undefined: "+self.name)

        consumer = yield container.new_consumer(name_config, self.spawned)
        self.consumer = consumer
        defer.returnValue(consumer)

    def handle(self, f):
        self.handlers[f.__name__] = f

    def addHandler(self, f):
        self.handlers[f.__name__] = f

    def getHandler(self, key):
        handler_key = 'handler_%s' % key
        return self.handlers.get(handler_key, None)

    def deliver(self, data, msg):
        """Spawnable to Receiver Interface
        try delivering to a callable named self.name
        else, the receive function must be defined
        """
        if self.handlers.has_key(self.name):
            res = self.handlers[self.name](data)
        else:
            self.handlers['receive'](data, msg)

    @defer.inlineCallbacks
    def send(self, to_id, data):
        # self.spawned._send_from(to_id, data)
        # NOTE: Here, a deferred is returned and not yielded (runaway)!!!
        yield send(to_id, data)

class IProtocolFactory(Interface):

    def build(spawnArgs):
        """
        """

class ProtocolFactory(object):
    """allows a instance to create a new receiver.
    The spawnable does the real work, as it is owns the context.
    In the module code, an instance passes it's Id in, along with the name
    it wants to receive on.
    """
    implements(IProtocolFactory)

    receiver = Receiver

    def build(self, spawnArgs):
        receiver = self.receiver()
        return receiver


# Container API
@defer.inlineCallbacks
def send(to_name, data, space=None):
    """
    Sends a message
    @param to_name if int, local identifier (sequence number) of process;
            otherwise str or Id, global exchange name
    """
    # If int, interpret name as local identifier and convert to global
    if type(to_name) is int:
        to_name = container.Id(to_name).full
    name_config = yield store.get(str(to_name))
    if not name_config:
        pass
    # @todo I don't think we need this config dict in the way it is used here.
    # The only thing we need to know is exchange (given) and routing key.
    # In the future, different types of names might have different attributes.
    pub_config = {'routing_key' : str(to_name)}
    pubs = yield container.new_publisher(pub_config)
    yield pubs.send(data)
    pubs.close()

def ps():
    """list running instances
    """
    print 'Running processes: %d' % len(Spawnable.progeny)
    print 'id  label    name      space'
    print '---------------------------------'
    for id, s in Spawnable.progeny.iteritems():
        if id.full == s.target.name:
            print id.local, s.target.label, s.target.name, s.space.hostname

def ms():
    """list messaging info.
    @todo this should also report on the messaging system connections, etc.
    """
    print 'Messaging names: %s' % len(Spawnable.progeny)
    print 'id  queue  routing_key  exchange  state'
    print '---------------------------------------'
    # Do a group by receiver group (e.g. a service name)
    grps = {}
    for id, s in Spawnable.progeny.iteritems():
        grp = s.target.group if hasattr(s.target,'group') else '__other__'
        if not grp in grps:
            grpl = []
            grps[grp] = grpl
        else:
            grpl = grps[grp]
        grpl.append(s)
    for gname in sorted(grps.keys()):
        print gname
        grpl = grps[gname]
        for s in grpl:
            print " ", s.id.local, s.target.consumer.queue, s.target.consumer.routing_key, s.target.consumer.exchange

def spawn(m, space=None, spawnArgs=None):
    """spawn something (function or module).
    Space is message space; container has a default space

    Spawn uses a function as an entry point for running a module
    """
    if not space:
        space = Container.space
    if spawnArgs == None:
        spawnArgs = {}
    if type(m) is types.ModuleType:
        return Spawnable.spawn_m(m, space, spawnArgs)
    elif type(m) is types.FunctionType:
        return Spawnable.spawn_f(m, space)
    elif isinstance(m, Receiver):
        return Spawnable.spawn_mr(m, space)

def kill(id):
    """stop instance from running.
     - cancel messaging consumer
     - delete
    """
    if not isinstance(id, Id):
        id = Id(id)
    if Spawnable.progeny.has_key(id):
        Spawnable.progeny[id].kill()

def lookup(name):
    store = Store()
    return store.query(name)



factory = ProtocolFactory()


@defer.inlineCallbacks
def _test():
    yield Container.configure_messaging('dozer', {'name_type':'direct'})
    r = Receiver('dozer', 'dozer')
    def receive(a, b):
        b.ack()
        log.msg(a,b)
    r.handle(receive)
    id = yield spawn(r)
    defer.returnValue(id)
