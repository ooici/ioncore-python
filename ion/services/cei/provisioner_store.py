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

    def put_state(self, node_id, state, record, timestamp=None):
        """Stores a state record about an instance or launch
        """
        newid = str(uuid.uuid4())
        timestamp = str(timestamp or int(time.time() * 1e6))
        key = '|'.join([node_id, state, timestamp, newid])
        self.data[key] = json.dumps(record)
        logging.debug('Added provisioner state: "%s"', key)
        return defer.succeed(key)

    def get_states(self, node_id, state=None, max_result=None):
        """Retrieves the states about an instance or launch.

        States are returned in order.
        """
        if state:
            prefix = '%s|%s|' % (node_id, state)
        else:
            prefix = '%s|' % node_id

        matches = [(s[0], json.loads(s[1])) for s in self.data.iteritems() 
                if s[0].startswith(prefix)]
        matches.sort(reverse=True)
        return defer.succeed(max_result and matches[:max_result] or matches)
