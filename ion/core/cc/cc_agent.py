#!/usr/bin/env python

"""
@file ion/core/cc/cc_agent.py
@author Michael Meisinger
@brief capability container control process
"""

import logging
import os

from twisted.internet import defer
import magnet
from magnet.container import Container
from magnet.spawnable import Receiver, spawn

from ion.agents.resource_agent import ResourceAgent
from ion.core import ionconst
from ion.core.base_process import BaseProcess, ProtocolFactory, ProcessDesc
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

        # Declare CC announcement name
        messaging = {'name_type':'fanout', 'args':{'scope':'system'}}
        yield Container.configure_messaging(self.ann_name, messaging)
        logging.info("Declared CC anounce name: "+str(self.ann_name))

        # Attach to CC announcement name
        annReceiver = Receiver(annName+'.'+self.receiver.label, self.ann_name)
        annReceiver.group = self.receiver.group
        self.ann_receiver = annReceiver
        self.ann_receiver.handle(self.receive)
        self.add_receiver(self.ann_receiver)
        annid = yield spawn(self.ann_receiver)
        logging.info("Listening to CC anouncements: "+str(annid))

        # Start with an identify request. Will lead to an announce by myself
        yield self.send(self.ann_name, 'identify', 'started', {'quiet':True})

    @defer.inlineCallbacks
    def _send_announcement(self, event):
        """
        Send announce message to CC broadcast name
        """
        cdesc = {'node':str(os.uname()[1]),
                 'container-id':str(Container.id),
                 'version':ionconst.VERSION,
                 'magnet':magnet.__version__,
                 'start-time':self.start_time,
                 'current-time':pu.currenttime_ms(),
                 'event':event}
        yield self.send(self.ann_name, 'announce', cdesc)

    def op_announce(self, content, headers, msg):
        """
        Service operation: announce a capability container
        """
        logging.info("op_announce(): Received CC announcement: " + repr(content))
        contid = content['container-id']
        event = content['event']
        if event == 'started' or event == 'identify':
            self.containers[contid] = content
        elif event == 'terminate':
            del self.containers[contid]
        logging.info("op_announce(): Know about %s containers!" % (len(self.containers)))


    @defer.inlineCallbacks
    def op_identify(self, content, headers, msg):
        """
        Service operation: ask for identification; respond with announcement
        """
        logging.info("op_identify(). Send announcement")
        yield self._send_announcement('identify')

    @defer.inlineCallbacks
    def op_spawn(self, content, headers, msg):
        """
        Service operation: spawns a local module
        """
        procMod = str(content['module'])
        child = ProcessDesc(name=procMod.rpartition('.')[2], procclass=procMod)
        pid = yield self.spawn_child(child)
        yield self.reply(msg, 'result', {'status':'OK', 'process-id':str(pid)})

    def op_start_node(self, content, headers, msg):
        pass

    def op_terminate_node(self, content, headers, msg):
        pass

    def op_get_node_id(self, content, headers, msg):
        pass


    def op_get_config(self, content, headers, msg):
        pass

# Spawn of the process using the module name
factory = ProtocolFactory(CCAgent)

"""
twistd -n --pidfile t1.pid magnet -h amoeba.ucsd.edu -a sysname=mm res/scripts/newcc.py
twistd -n --pidfile t2.pid magnet -h amoeba.ucsd.edu -a sysname=mm res/scripts/newcc.py

send (2, {'op':'identify','content':''})
send (2, {'op':'spawn','content':{'module':'ion.play.hello_service'}})
"""
