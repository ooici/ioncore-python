"""
@author Dorian Raymer
@author Michael Meisinger
@brief Python Capability Container shell
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.messaging.receiver import Receiver

@defer.inlineCallbacks
def send(to_name, data, exchange_space=None):
    """
    Sends a message
    @param to_name if int, local identifier (sequence number) of process;
            otherwise str or Id, global exchange name
    """
    # If int, interpret name as local identifier and convert to global
    if type(to_name) is int:
        to_name = Id(to_name).full

    yield ioninit.container_instance.send(to_name, data, exchange_space)

@staticmethod
def ps():
    """list running instances
    """
    print 'Running processes: %d' % len(Spawnable.progeny)
    print 'id  label    name      space'
    print '---------------------------------'
    for id, s in Spawnable.progeny.iteritems():
        if id.full == s.target.name:
            print id.local, s.target.label, s.target.name, s.space.connection.hostname

@staticmethod
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

@staticmethod
def spawn(m, space=None, spawnArgs=None):
    """spawn something (function or module).
    Space is message space; container has a default space

    Spawn uses a function as an entry point for running a module
    """
    if not space:
        space = ioninit.container_instance.message_space
    if spawnArgs == None:
        spawnArgs = {}
    if type(m) is types.ModuleType:
        return Spawnable.spawn_m(m, space, spawnArgs)
    elif type(m) is types.FunctionType:
        return Spawnable.spawn_f(m, space)
    elif isinstance(m, Receiver):
        return Spawnable.spawn_mr(m, space)

@staticmethod
def kill(id):
    """stop instance from running.
     - cancel messaging consumer
     - delete
    """
    if not isinstance(id, Id):
        id = Id(id)
    if Spawnable.progeny.has_key(id):
        Spawnable.progeny[id].kill()

@staticmethod
def lookup(name):
    store = Store()
    return store.query(name)
