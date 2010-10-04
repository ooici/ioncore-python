#!/usr/bin/env python

"""
@file ion/core/cc/cc_agent.py
@author Michael Meisinger
@brief capability container control process (agent)
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
import os

from twisted.internet import defer, reactor

from ion.core.cc.container import Container
from ion.core.messaging.receiver import Receiver

from ion.agents.resource_agent import ResourceAgent
from ion.core import ionconst, ioninit
from ion.core.base_process import BaseProcess, ProcessFactory, ProcessDesc
from ion.core.base_process import procRegistry, processes, receivers
from ion.core.ioninit import ion_config
from ion.core.supervisor import Supervisor
import ion.util.procutils as pu


class CCAgent(ResourceAgent):
    """
    Capability Container agent process interface
    """
    @defer.inlineCallbacks
    def plc_init(self):
        # Init self and container
        annName = 'cc_announce'
        self.ann_name = self.get_scoped_name('system', annName)
        self.start_time = pu.currenttime_ms()
        self.containers = {}
        self.contalive = {}
        self.last_identify = 0

        # Declare CC announcement name
        messaging = {'name_type':'fanout', 'args':{'scope':'system'}}
        yield ioninit.container_instance.configure_messaging(self.ann_name, messaging)
        log.info("Declared CC anounce name: "+str(self.ann_name))

        # Attach to CC announcement name
        annReceiver = Receiver(annName+'.'+self.receiver.label, self.ann_name)
        annReceiver.group = self.receiver.group
        self.ann_receiver = annReceiver
        self.ann_receiver.handle(self.receive)
        self.add_receiver(self.ann_receiver)
        annid = yield self.ann_receiver.activate()
        log.info("Listening to CC anouncements: "+str(annid))

        # Start with an identify request. Will lead to an announce by myself
        #@todo - Can not send a message to a base process which is not initialized!
        yield self.send(self.ann_name, 'identify', 'started', {'quiet':True})

        # Convenience HACK: Add a few functions to container shell
        self._augment_shell()

    @defer.inlineCallbacks
    def _send_announcement(self, event):
        """
        Send announce message to CC broadcast name
        """
        cdesc = {'node':str(os.uname()[1]),
                 'container-id':str(Container.id),
                 'agent':str(self.id.full),
                 'version':ionconst.VERSION,
                 'start-time':self.start_time,
                 'current-time':pu.currenttime_ms(),
                 'event':event}
        yield self.send(self.ann_name, 'announce', cdesc)

    def op_announce(self, content, headers, msg):
        """
        Service operation: announce a capability container
        """
        log.info("op_announce(): Received CC announcement: " + repr(content))
        contid = content['container-id']
        event = content['event']
        if event == 'started' or event == 'identify':
            self.containers[contid] = content
            self.contalive[contid] = int(pu.currenttime_ms())
        elif event == 'terminate':
            del self.containers[contid]
            del self.contalive[contid]

        log.info("op_announce(): Know about %s containers!" % (len(self.containers)))

    @defer.inlineCallbacks
    def op_identify(self, content, headers, msg):
        """
        Service operation: ask for identification; respond with announcement
        """
        log.info("op_identify(). Sending announcement")
        self._check_alive()

        # Set the new reference. All alive containers will respond afterwards
        self.last_identify = int(pu.currenttime_ms())

        reactor.callLater(3, self._check_alive)
        yield self._send_announcement('identify')

    def _check_alive(self):
        """
        Check through all containers if we have a potential down one.
        A container is deemed down if it has not responded since the preceding
        identify message.
        """
        for cid,cal in self.contalive.copy().iteritems():
            if cal<self.last_identify:
                log.info("Container %s missing. Deemed down, remove." % (cid))
                del self.containers[cid]
                del self.contalive[cid]

    @defer.inlineCallbacks
    def op_spawn(self, content, headers, msg):
        """
        Service operation: spawns a local module
        """
        procMod = str(content['module'])
        child = ProcessDesc(name=procMod.rpartition('.')[2], module=procMod)
        pid = yield self.spawn_child(child)
        yield self.reply_ok(msg, {'process-id':str(pid)})

    def op_start_node(self, content, headers, msg):
        pass

    def op_terminate_node(self, content, headers, msg):
        pass

    @defer.inlineCallbacks
    def op_ping(self, content, headers, msg):
        """
        Service operation: ping reply
        """
        yield self.reply_ok(msg, None, {'quiet':True})

    @defer.inlineCallbacks
    def op_get_info(self, content, headers, msg):
        """
        Service operation: replies with all kinds of local information
        """
        procsnew = processes.copy()
        for pn,p in procsnew.iteritems():
            cls = p.pop('class')
            p['classname'] = cls.__name__
            p['module'] = cls.__module__
        res = {'services':procsnew}
        procs = {}
        for rec in receivers:
            recinfo = {}
            recinfo['classname'] = rec.process.__class__.__name__
            recinfo['module'] = rec.process.__class__.__module__
            recinfo['label'] = rec.label
            procs[rec.name] = recinfo
        res['processes'] = procs
        yield self.reply_ok(msg, res)


    def _augment_shell(self):
        """
        Dirty little helper functions attached to the 'cc' object in the
        container shell. Quick spawn of processes and send
        """
        from ion.core.cc.shell import control
        if not hasattr(control, 'cc'):
            return
        log.info("Augmenting Container Shell...")
        control.cc.agent = self
        control.cc.config = ion_config
        control.cc.pids = procRegistry.kvs
        control.cc.svcs = processes
        control.cc.procs = receivers
        def send(recv, op, content=None, headers=None, **kwargs):
            if content == None: content = {}
            if recv in control.cc.pids: recv = control.cc.pids[recv]
            d = self.send(recv, op, content, headers, **kwargs)
        control.cc.send = send
        def rpc_send(recv, op, content=None, headers=None, **kwargs):
            if content == None: content = {}
            if recv in control.cc.pids: recv = control.cc.pids[recv]
            d = self.rpc_send(recv, op, content, headers, **kwargs)
        control.cc.rpc_send = rpc_send
        def _get_target(name):
            mod = name
            for p in control.cc.svcs.keys():
                if p.startswith(name):
                    mod = control.cc.svcs[p]['class'].__module__
                    name = p
                    break
            return (mod, name)
        def _get_node(node=None):
            if type(node) is int:
                for cid in self.containers.keys():
                    if cid.find(str(node)) >= 0:
                        node = str(self.containers[cid]['agent'])
                        break
            return node
        def spawn(name, node=None, args=None):
            (mod,name) = _get_target(name)
            if node != None:
                node = _get_node(node)
                self.send(node,'spawn',{'module':mod})
            else:
                d = self.spawn_child(ProcessDesc(name=name, module=mod))
        control.cc.spawn = spawn
        def svc():
            for pk,p in control.cc.svcs.iteritems():
                print pk, p['class'].__module__
        control.cc.svc = svc
        def ps():
            for r in control.cc.procs:
                print r.label, r.name
                setattr(control.cc, r.label, r.process)
        control.cc.ps = ps
        def nodes():
            nodes = {}
            for c in self.containers.values():
                nodes[str(c['node'])] = 1
            return nodes.keys()
        control.cc.nodes = nodes
        control.cc.cont = lambda: [str(k) for k in self.containers.keys()]
        control.cc.info = lambda: self.containers[str(Container.id)]
        control.cc.identify = lambda: self.send(self.ann_name, 'identify', '', {'quiet':True})
        control.cc.getinfo = lambda n: self.send(_get_node(n), 'get_info', '')
        control.cc.ping = lambda n: self.send(_get_node(n), 'ping', '', {'quiet':True})
        control.cc.help = "CC Helpers. ATTRS: agent, config, pids, svcs, procs, help FUNC: send, rpc_send, spawn, svc, ps, nodes, cont, info, identify, ping"

# Spawn of the process using the module name
factory = ProcessFactory(CCAgent)

"""
twistd -n --pidfile t1.pid cc -h amoeba.ucsd.edu -a sysname=mm res/scripts/newcc.py
twistd -n --pidfile t2.pid cc -h amoeba.ucsd.edu -a sysname=mm res/scripts/newcc.py

send (2, {'op':'identify','content':''})
send (2, {'op':'spawn','content':{'module':'ion.play.hello_service'}})
"""
