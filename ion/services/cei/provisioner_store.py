#!/usr/bin/env python

"""
@file ion/services/cei/provisioner_store.py
@author David LaBissoniere
@brief Provisioner storage abstraction
"""

import logging
import uuid
import time
from twisted.internet import defer

try: 
    import json
except ImportError:
    import simplejson as json

class ProvisionerStore(object):
    """Abstraction for data storage routines by provisioner
    """

    # Using a simple in-memory dict for now, until it is clear how
    # to use CEI datastore
    data = {}

    def put_record(self, record, newstate=None, timestamp=None):
        """Stores a record, optionally first updating state.
        """
        if newstate:
            record['state'] = newstate
        
        #these two are expected to be on every record
        launch_id = record['launch_id']
        state = record['state']

        #this one will be missing for launch records
        node_id = record.get('node_id', '')

        newid = str(uuid.uuid4())
        ts = str(timestamp or int(time.time() * 1e6))
        key = '|'.join([launch_id, node_id, state, ts, newid])
        self.data[key] = json.dumps(record)
        logging.debug('Added provisioner state: "%s"', key)
        return defer.succeed(key)

    def put_records(self, records, newstate=None, timestamp=None):
        """Stores a list of records, optionally first updating state.
        """
        ts = str(timestamp or int(time.time() * 1e6))
        return [self.put_record(r, newstate=newstate, timestamp=ts) 
                for r in records]

    def get_all(self, launch=None, node=None):
        """Retrieves the states about an instance or launch.

        States are returned in order.
        """
        prefix = ''
        if launch:
            prefix = '%s|' % launch
            if node:
                prefix += '%s|' % node
        #TODO uhhh. regex..? don't know what matching functionality we 
        # actually need here yet.

        matches = [(s[0], json.loads(s[1])) for s in self.data.iteritems() 
                if s[0].startswith(prefix)]
        matches.sort(reverse=True)
        records = [r[1] for r in matches]
        return defer.succeed(records)
