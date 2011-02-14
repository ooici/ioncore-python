#!/usr/bin/env python

"""
@file ion/core/cc/cc_agent.py
@author Michael Meisinger
@brief capability container control process (agent)
"""

import os

from twisted.internet import defer, reactor

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.agents.resource_agent import ResourceAgent
from ion.core import ionconst, ioninit
from ion.core.ioninit import ion_config
from ion.core.cc.container import Container
from ion.core.messaging.receiver import Receiver, FanoutReceiver
from ion.core.pack import app_supervisor
from ion.core.process.process import Process, ProcessFactory, ProcessDesc
import ion.util.procutils as pu
from ion.services.coi.exchange.agent_client import ExchangeManagementClient

CONF = ioninit.config(__name__)
CF_announce = CONF.getValue('announce', False)

class CCAgent(ResourceAgent):

    instance = None

    """
    Capability Container agent process interface
    """
    def plc_init(self):
        assert not CCAgent.instance, "CC agent already started"
        CCAgent.instance = self
        # Init self and container
        self.start_time = pu.currenttime_ms()
        self.containers = {}
        self.contalive = {}
        self.last_identify = 0
        self.exchange_management_client = None

    @defer.inlineCallbacks
    def plc_activate(self):
        # Declare CC announcement name
        annName = 'cc_announce'

        # Attach to CC announcement name
        self.ann_receiver = FanoutReceiver(name=annName,
                                           label=annName+'.'+self.receiver.label,
                                           scope=FanoutReceiver.SCOPE_SYSTEM,
                                           group=self.receiver.group,
                                           handler=self.receive)
        self.ann_name = yield self.ann_receiver.attach()

        log.info("Listening to CC anouncements: "+str(self.ann_name))

        if CF_announce:
            # Start with an identify request. Will lead to an announce by myself
            #@todo - Can not send a message to a base process which is not initialized!

            yield self._send_announcement('initialize')
        
        # self.exchange_management_client = ExchangeManagementClient(ioninit.container_instance)


    @defer.inlineCallbacks
    def plc_terminate(self):
        if CF_announce:
            yield self._send_announcement('terminate')

    @defer.inlineCallbacks
    def _send_announcement(self, event):
        """
        Send announce message to CC broadcast name
        """
        cdesc = {'node':str(os.uname()[1]),
                 'container-id':str(ioninit.container_instance.id),
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
        if event == 'initialize' or event == 'identify':
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
        yield self.reply_ok(msg, {'pong':'pong'}, {'quiet':True})

    @defer.inlineCallbacks
    def op_get_info(self, content, headers, msg):
        """
        Service operation: replies with all kinds of local information
        """
        #procsnew = processes.copy()
        #for pn,p in procsnew.iteritems():
        #    cls = p.pop('class')
        #    p['classname'] = cls.__name__
        #    p['module'] = cls.__module__
        #res = {'services':procsnew}
        #procs = {}
        #for rec in receivers:
        #    recinfo = {}
        #    recinfo['classname'] = rec.process.__class__.__name__
        #    recinfo['module'] = rec.process.__class__.__module__
        #    recinfo['label'] = rec.label
        #    procs[rec.name] = recinfo
        #res['processes'] = procs
        res = {}
        yield self.reply_ok(msg, res)

# Spawn of the process using the module name
factory = ProcessFactory(CCAgent)


# --- CC Application interface

# Functions required
@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):
    agent_proc = [
        {'name':'ccagent','module':__name__},
    ]

    appsup_desc = ProcessDesc(name='app-supervisor-'+app_definition.name,
                              module=app_supervisor.__name__,
                              spawnargs={'spawn-procs':agent_proc})
    supid = yield appsup_desc.spawn()

    res = (supid.full, [appsup_desc])
    defer.returnValue(res)

@defer.inlineCallbacks
def stop(container, state):

    #print "state", state
    supdesc = state[0]
    log.info("Terminating CC agent")
    yield supdesc.terminate()
    CCAgent.instance = None

"""
twistd -n --pidfile t1.pid cc -h amoeba.ucsd.edu -a sysname=mm res/scripts/newcc.py
twistd -n --pidfile t2.pid cc -h amoeba.ucsd.edu -a sysname=mm res/scripts/newcc.py

send (2, {'op':'identify','content':''})
send (2, {'op':'spawn','content':{'module':'ion.play.hello_service'}})
"""
