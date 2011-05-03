"""
@author Dorian Raymer
@author Michael Meisinger
@brief Python Capability Container shell functions
"""

import types
from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.id import Id
import ion.util.procutils as pu
from ion.core.process.process import ProcessDesc, Process

# The shell namespace
namespace = None

def info():
    print "Python Capability Container, "
    print "  Container id:", ioninit.container_instance.id
    print
    print "Available Functions:"
    print "  info(): This info text"
    print "  ps(): Process information"
    print "  ms(): Messaging information"
    print "  send(to,op,content): Send a message"
    print "  rpc_send(to,op,content): Send an RPC message"
    print "  spawn(module): Spawn a process from a module"
    print "  makeprocess(): Returns a new Process object (spawn is called but may not be done yet)"
    print "Variables:"
    print "  control: shell control"
    print "  procs: dict of local process names -> pid"
    print "  svcs: dict of local service declarations"

def ps():
    """
    List running instances
    """
    _update()
    procs = namespace['pids']
    len_name = 1
    len_pid  = 1
    len_sid  = 1
    buffer = 2
    for pid in sorted(procs.keys()):
        proc = procs[pid]
        lname = proc.proc_name.replace(ioninit.container_instance.id, "<LOCAL>")
        len_name = max(len_name, len(lname))
        len_pid = max(len_pid, len(str(proc.id)))
        len_sid = max(len_sid, len(str(proc.proc_supid)))
        
    len_name += buffer
    len_pid  += buffer
    len_sid  += buffer
    
    fmt_str = "%-" + str(len_name) + "s \t%-" + str(len_pid) + "s \t%-" + str(len_sid) + "s"
    
    print fmt_str % ('name', 'id', 'supervisor')
    print '-' * (len_name + len_pid + len_sid + (buffer*3))
    for pid in sorted(procs.keys()):
        proc = procs[pid]
        lname = proc.proc_name.replace(ioninit.container_instance.id, "<LOCAL>")
        print fmt_str % (lname, proc.id, proc.proc_supid)
    print '-' * (len_name + len_pid + len_sid + (buffer*3))
    print 'Running processes: %d' % len(procs)

def svc():
    """
    List system services
    """
    _update()
    svcs = namespace['svcs']
    for pk,p in svcs.iteritems():
        print "%s \t%s" % (pk, p['class'].__module__)

def ms(full=False):
    """list messaging info.
    @todo this should also report on the messaging system connections, etc.
    """
    if full:
        print 'id \tconfig'
    else:
        print 'id \ttype \trouting_key \tqueue'
    print '---------------------------------------'
    namestore = ioninit.container_instance.exchange_manager.exchange_space.store.kvs
    for name in sorted(namestore.keys()):
        namecfg = namestore[name]
        if full:
            print "%s \t%s" % (name, namecfg)
        else:
            print "%s \t%s \t%s \t%s" % (name, namecfg['name_type'], namecfg['routing_key'], namecfg['queue'])
    print 'Messaging names: %s' % len(namestore)

    # Do a group by receiver group (e.g. a service name)
    #grps = {}
    #for id, s in Spawnable.progeny.iteritems():
    #    grp = s.target.group if hasattr(s.target,'group') else '__other__'
    #    if not grp in grps:
    #        grpl = []
    #        grps[grp] = grpl
    #    else:
    #        grpl = grps[grp]
    #    grpl.append(s)
    #for gname in sorted(grps.keys()):
    #    print gname
    #    grpl = grps[gname]
    #    for s in grpl:
    #        print " ", s.id.local, s.target.consumer.queue, s.target.consumer.routing_key, s.target.consumer.exchange

@defer.inlineCallbacks
def send(to_name, op, content=None, headers=None, **kwargs):
    """
    Sends a message
    @param to_name if int, local identifier (sequence number) of process;
            otherwise str or Id, global exchange name
    """
    # If int, interpret name as local identifier and convert to global
    if type(to_name) is int:
        to_name = Id(to_name).full
    if content == None:
        content = {}

    _update()
    #procs = namespace['procs']
    #if to_name in procs: recv = procs[to_name]

    sup = yield ioninit.container_instance.proc_manager.create_supervisor()
    yield sup.send(to_name, op, content, headers, **kwargs)

@defer.inlineCallbacks
def rpc_send(to_name, op, content=None, headers=None, **kwargs):
    # If int, interpret name as local identifier and convert to global
    if type(to_name) is int:
        to_name = Id(to_name).full
    if content == None:
        content = {}

    _update()
    #procs = namespace['procs']
    #if to_name in procs: recv = procs[to_name]

    sup = yield ioninit.container_instance.proc_manager.create_supervisor()
    yield sup.rpc_send(to_name, op, content, headers, **kwargs)

def _get_target(name):
    _update()
    svcs = namespace['svcs']
    mod = name
    for p in svcs.keys():
        if p.startswith(name):
            mod = svcs[p]['class'].__module__
            name = p
            break
    return (mod, name)

def _get_node(node=None):
    agent = namespace['agent']
    if type(node) is int:
        for cid in agent.containers.keys():
            if cid.find(str(node)) >= 0:
                node = str(agent.containers[cid]['agent'])
                break
    return node

@defer.inlineCallbacks
def spawn(module, node=None, spawnargs=None, space=None):
    """spawn something (function or module).
    Space is message space; container has a default space

    Spawn uses a function as an entry point for running a module
    """
    sup = yield ioninit.container_instance.proc_manager.create_supervisor()

    modstr = None
    if type(module) is types.ModuleType:
        modstr = module.__name__
    elif type(module) is str:
        modstr = module

    (mod,name) = _get_target(modstr)
    if node != None:
        node = _get_node(node)
        sup.send(node,'spawn',{'module':mod})
    else:
        sup.spawn_child(ProcessDesc(name=name, module=mod))
    #
    #return ioninit.container_instance.proc_manager.spawn_process_local(
    #        modstr, space, spawnargs)

def kill(id):
    """stop instance from running.
     - cancel messaging consumer
     - delete
    """

def nodes():
    agent = namespace['agent']
    nodes = {}
    for c in agent.containers.values():
        nodes[str(c['node'])] = 1
    return nodes.keys()

#control.cc.cont = lambda: [str(k) for k in self.containers.keys()]
#control.cc.info = lambda: self.containers[str(Container.id)]
#control.cc.identify = lambda: self.send(self.ann_name, 'identify', '', {'quiet':True})
#control.cc.getinfo = lambda n: self.send(_get_node(n), 'get_info', '')
#control.cc.ping = lambda n: self.send(_get_node(n), 'ping', '', {'quiet':True})

def identify():
    toname = pu.get_scoped_name('cc_announce','system')
    send(toname, 'identify', '', {'quiet':True})


def _update():
    try:
        from ion.core.process import process
        from ion.core.ioninit import ion_config
        from ion.core.cc.cc_agent import CCAgent

        namespace['sup'] = ioninit.container_instance.proc_manager.supervisor
        namespace['agent'] = CCAgent.instance
        namespace['config'] = ion_config

        namespace['pids'] = ioninit.container_instance.proc_manager.process_registry.kvs
        namespace['procs'] = process.procRegistry.kvs
        namespace['svcs'] = process.processes

    except Exception:
        log.exception("Error updating CC shell namespace")

def makeprocess():
    p = Process()
    p.spawn()

    return p