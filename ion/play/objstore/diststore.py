#!/usr/bin/env python

"""
@file ion/play/objstore/diststore.py
@author Michael Meisinger
@brief Simulates a distributed data store.
"""

#import ion.util.ionlog
#log = ion.util.ionlog.getLogger(__name__)

import random
import time

from twisted.internet import defer, reactor

from ion.data.store import Store
import ion.util.procutils as pu

# In a DS such as Cassandra, nodes and number of replicas are different. Load
# is partitioned. We just distribute all keys over #replicas nodes.

CONSISTENCY_ONE = 1
CONSISTENCY_QUORUM = 2
CONSISTENCY_ALL = 3

READ_REPAIR_RECENT = 1

class DDSSimulator(object):

    def __init__(self, clustername, numnodes):
        self.clustername = clustername
        self.numnodes = numnodes
        self.nodekvs = []
        self.numreads = 0
        self.numwrites = 0
        for i in range(numnodes):
            nodekvs = Store()
            self.nodekvs.append(nodekvs)

    @defer.inlineCallbacks
    def lock(self, key, node):
        lockkey = -1 * key
        yield self.write(lockkey, node, CONSISTENCY_QUORUM, node)
        lv = yield self.read(lockkey, clevel=CONSISTENCY_QUORUM, repair=READ_REPAIR_RECENT, node=node)
        has_lock = (lv == node)
        defer.returnValue(has_lock)

    @defer.inlineCallbacks
    def unlock(self, key, node):
        lockkey = -1 * key
        lv = yield self.read(lockkey, clevel=CONSISTENCY_QUORUM, repair=READ_REPAIR_RECENT, node=node)
        if lv == node:
            yield self.write(lockkey, None, CONSISTENCY_QUORUM, node)
        else:
            raise RuntimeError("Node %s does not have lock=%s" % (node, lv))

    @defer.inlineCallbacks
    def read(self, key, clevel=CONSISTENCY_ALL, repair=READ_REPAIR_RECENT, node=-1):
        #print "[%s] Read key=%s starting" % (node,key)
        if (clevel == CONSISTENCY_ONE):
            value = yield self._read_nodes(key, 1, repair=repair)
        elif (clevel == CONSISTENCY_QUORUM):
            value = yield self._read_nodes(key, self.numnodes / 2 + 1, repair=repair)
        elif (clevel == CONSISTENCY_ALL):
            value = yield self._read_nodes(key, self.numnodes, repair=repair)
        else:
            raise RuntimeError("Unknown consistency level")
        #print "[%s] Read key=%s=%s" % (node,key,value)
        defer.returnValue(value)

    @defer.inlineCallbacks
    def write(self, key, value, clevel=CONSISTENCY_ALL, node=-1):
        #print "[%s] Write key=%s value=%s starting" % (node,key, value)
        if (clevel == CONSISTENCY_ONE):
            yield self._write_nodes(key, value, 1)
        elif (clevel == CONSISTENCY_QUORUM):
            yield self._write_nodes(key, value, self.numnodes / 2 + 1)
        elif (clevel == CONSISTENCY_ALL):
            yield self._write_nodes(key, value, self.numnodes)
        else:
            raise RuntimeError("Unknown consistency level")
        #print "[%s] Write key=%s value=%s done" % (node,key, value)

    @defer.inlineCallbacks
    def _read_nodes(self, key, numnodes, repair):
        values = yield self._read_node_values(key, numnodes)
        value = yield self._read_repair(key, values, repair)
        defer.returnValue(value)

    @defer.inlineCallbacks
    def _read_node_values(self, key, numnodes):
        nodelist = self._create_nodelist(numnodes)
        values = {}
        for node in nodelist:
            nval = yield self._read_from_node(node, key)
            yield self._random_sleep()
            if not nval:
                nval = (None, 0)
            values[node] = nval
        defer.returnValue(values)

    @defer.inlineCallbacks
    def _read_repair(self, key, values, repair):
        #print "_read_repair %s" % values
        timevals = {}
        for (val, ts) in values.values():
            timevals[ts] = val
        valueset = set(timevals.values())
        if len(valueset) > 1:
            print "Read different values for key: %s" % key
            pass

        if (repair == READ_REPAIR_RECENT):
            xtime = max(sorted(timevals.keys()))
            value = timevals[xtime]
            if len(valueset) > 1:
                xvalue = (value, xtime)
                for node, (val, ts) in values.iteritems():
                    if val != value:
                        #print "Correcting node %s" % node
                        yield self._write_to_node(node, key, xvalue)
            defer.returnValue(value)
        else:
            raise RuntimeError("Unknown read repair method")

    @defer.inlineCallbacks
    def _write_nodes(self, key, value, numnodes):
        nodelist = self._create_nodelist(numnodes)
        xvalue = self._create_value(value)
        for node in nodelist:
            yield self._write_to_node(node, key, xvalue)

    def _create_nodelist(self, numnodes):
        """
        Creates a random list of nodes to access
        """
        nodelist = []
        assert numnodes <= self.numnodes
        while len(nodelist) < numnodes:
            node = self._select_node()
            if not node in nodelist:
                nodelist.append(node)
        #print "Accessing nodes %s" % nodelist
        return nodelist

    def _create_value(self, value):
        ctime = int(time.time() * 100000)
        return (value, ctime)

    def _select_node(self):
        cnum = random.randint(0,self.numnodes-1)
        return cnum

    def _random_sleep(self):
        return pu.asleep(random.random()/30)

    @defer.inlineCallbacks
    def _read_from_node(self, node, key):
        yield self.nodekvs[node].get(key)
        self.numreads += 1

    @defer.inlineCallbacks
    def _write_to_node(self, node, key, value):
        yield self.nodekvs[node].put(key, value)
        yield self._random_sleep()
        self.numwrites += 1

all_done = []

def exit_node(node):
    global all_done
    all_done[node] = True
    print "Node %s done" % node
    if (all(all_done)):
        reactor.stop()

@defer.inlineCallbacks
def do_sequence(dds, node):
    global all_done
    myspeed = random.random() * 3 + 1
    for i in range(20):
        key = random.randint(0, 5)
        value = random.randint(0, 1000000)
        yield pu.asleep(random.random()*myspeed/5)
        yield dds.write(key, value, CONSISTENCY_QUORUM, node=node+float(i)/100)
        yield dds.read(key, CONSISTENCY_QUORUM, READ_REPAIR_RECENT, node=node+float(i)/100)

    exit_node(node)

@defer.inlineCallbacks
def do_sequence_lock(dds, node):
    global all_done
    myspeed = random.random() * 3 + 1
    for i in range(20):
        key = random.randint(0, 5)
        value = random.randint(0, 1000000)
        yield pu.asleep(random.random()*myspeed/5)
        yield dds.write(key, value, CONSISTENCY_QUORUM, node=node+float(i)/100)
        yield dds.read(key, CONSISTENCY_QUORUM, READ_REPAIR_RECENT, node=node+float(i)/100)

    exit_node(node)

def do_sequences(dds, num):
    global all_done
    for i in range(num):
        all_done.append(False)
        if i == 1:
            reactor.callWhenRunning(do_sequence_lock, dds, i)
        else:
            reactor.callWhenRunning(do_sequence, dds, i)

if __name__ == '__main__':
    dds = DDSSimulator("mycluster", numnodes=5)
    reactor.callWhenRunning(do_sequences, dds, 200)
    print "Starting reactor"
    reactor.run( )
    print "Reactor stopped"
    print "Stats: DDS Writes=%s, reads=%s" % (dds.numwrites, dds.numreads)
