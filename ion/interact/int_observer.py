#!/usr/bin/env python

"""
@file ion/interact/int_observer.py
@author Michael Meisinger
@brief A process that observes interactions in the Exchange
"""
import string

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.messaging.receiver import FanoutReceiver
from ion.core.process.process import Process, ProcessFactory
import ion.util.procutils as pu

class InteractionObserver(Process):
    """
    @brief Process that observes ongoing interactions in the Exchange. Logs
        them to disk and makes them available in the local container (for
        testing) and on request.
    """

    def __init__(self, *args, **kwargs):
        """
        """
        Process.__init__(self, *args, **kwargs)

        self.max_msglog = 10000
        self.msg_log = []
        self.write_on_term = True

        # Create a receiver (inbound queue consumer) for service name
        self.msg_receiver = FanoutReceiver(
                name='#',
                label='InteractionObserver',
                process=self,
                handler=self.msg_receive)
        self.add_receiver(self.msg_receiver)

    @defer.inlineCallbacks
    def plc_init(self):
        yield self.msg_receiver.initialize()

    @defer.inlineCallbacks
    def plc_activate(self):
        yield self.msg_receiver.activate()

    @defer.inlineCallbacks
    def plc_terminate(self):
        yield self.msg_receiver.deactivate()
        yield self.msg_receiver.terminate()
        if self.write_on_term:
            f = open('msc.txt', 'w')
            f.write(self.writeout_msc())
            f.close()

    def msg_receive(self, payload, msg):
        self.log_message(payload)
        msg.ack()


    def log_message(self, hdrs):
        # Tuple of Timestamp (MS), type, message
        mhdrs = hdrs.copy()
        if 'content' in mhdrs:
            del mhdrs['content']
        msg_rec = (pu.currenttime_ms(), mhdrs)
        self.msg_log.append(msg_rec)

        hstr = "MSG %d: %s(%s) -> %s %s:%s:%s-%s; uid=%s, status=%s" % (msg_rec[0],
                mhdrs.get('sender',None),
                mhdrs.get('sender-name',None),
                mhdrs.get('receiver',None), mhdrs.get('protocol',None),
                mhdrs.get('performative',None), mhdrs.get('op',None), mhdrs.get('conv-seq',None),
                mhdrs.get('user-id',None), mhdrs.get('status',None))

        log.info(hstr)

        # Truncate if too long in increments of 100
        if len(self.msg_log) > self.max_msglog + 100:
            self.msg_log = self.msg_log[100:]

    def writeout_msc(self):
        msglog = []
        msglog.extend(self.msg_log)
        procs = []
        senders = []
        proc_alias = {}
        for msgtup in msglog:
            msg = msgtup[1]
            sid = msg.get('sender', '??')
            if sid.endswith('b'):
                sid = sid[:len(sid)-1]
            sid = string.replace(sid, ".", "_")
            sid = string.replace(sid, "-", "_")

            rec = msg.get('receiver')
            if rec.endswith('b'):
                rec = rec[:len(rec)-1]
            rec = string.replace(rec, ".", "_")
            rec = string.replace(rec, "-", "_")

            sname = msg.get('sender-name', None)
            sname = string.replace(sname, ".", "_")
            sname = string.replace(sname, "-", "_")
            sender = sname or sid

            if not sender in senders:
                proc_alias[sid] = sname

            if not sid in procs:
                procs.append(sid)

            if not rec in procs:
                procs.append(rec)

        senders = []
        for proc in procs:
            senders.append(proc_alias.get(proc, proc))

        msc = "msc {\n"
        sstr = ",".join(senders)
        msc += " %s;\n" % sstr

        for msgtup in msglog:
            msg = msgtup[1]
            sid = msg.get('sender', '??')
            if sid.endswith('b'):
                sid = sid[:len(sid)-1]
            sid = string.replace(sid, ".", "_")
            sid = string.replace(sid, "-", "_")
            sname = msg.get('sender-name', None)
            sname = string.replace(sname, ".", "_")
            sname = string.replace(sname, "-", "_")
            sname = sname or sid
            rec = msg.get('receiver')
            if rec.endswith('b'):
                rec = rec[:len(rec)-1]
            rec = string.replace(rec, ".", "_")
            rec = string.replace(rec, "-", "_")
            rname = proc_alias.get(rec, rec)
            mlabel = "%s:%s:%s:%s" % (msg.get('protocol',None),
                msg.get('performative',None), msg.get('op',None), msg.get('conv-seq',None))
            msc += ' %s -> %s [ label="%s" ];\n' % (sname, rname, mlabel)
        msc += "}\n"

        return msc



# Spawn off the process using the module name
factory = ProcessFactory(InteractionObserver)


class InteractionMonitor(InteractionObserver):
    """
    @brief Extension of the InteractionObserver that observes interactions of
        a specific process and monitors it for correctness.
    @note The tricky thing is to relate incoming and outgoing messages of a
        process.
    """


class ConversationMonitor(InteractionObserver):
    """
    @brief Extension of the InteractionMonitor that distinguishes and monitors
        conversations within the interactions of a specific for correctness.
        Such conversations need to comply to a conversation type, which must
        be specified in an electronic format (such as Scribble, FSM) that can
        be operationally enacted (i.e. followed message by message)
    """
