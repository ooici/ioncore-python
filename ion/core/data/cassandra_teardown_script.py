#!/usr/bin/env python
"""
@file ion/core/data/cassandra_schema_script.py
@author David Stuebe
@author Matt Rodriguez

This class creates a connection to a Cassandra cluster without using ION Resources. 
This is useful when bootstrapping the system, because the datastore and the resource registry
services are not running yet. 
"""

from ion.core.data import cassandra_bootstrap
from ion.core.data import storage_configuration_utility

from telephus.cassandra.ttypes import InvalidRequestException

import pprint
from ion.core import ioninit

from twisted.internet import defer
from twisted.internet import reactor
from twisted.internet.error import ReactorNotRunning
import sys
import traceback

CONF_NAME = 'ion.core.data.cassandra_teardown_script'

CONF = ioninit.config(CONF_NAME)

@defer.inlineCallbacks
def cassandra_configuration_script():

    uname = CONF.getValue('cassandra_username')
    pword = CONF.getValue('cassandra_password')

    keyspace =  CONF.getValue('sysname')


    print 'Configuration arguments to Cassandra Teardown Script:'
    print 'Username: "%s"' % str(uname)
    #print 'Password: "%s"' % str(pword)
    print 'Sysname(keyspace): "%s"' % str(keyspace)

    if keyspace is None:
       raise Exception('Unable to get required configuration arguments!')

    storage_conf = storage_configuration_utility.get_cassandra_configuration(keyspace)

    print 'Running teardown for storage conf:'
    pprint.pprint(storage_conf)

    bootstrap = cassandra_bootstrap.CassandraSchemaProvider(uname, pword, storage_conf, error_if_existing=False)

    bootstrap.connect()

    success = False
    try:
        yield bootstrap.client.system_drop_keyspace(keyspace)
        success = True
    except InvalidRequestException, ire:
        print 'No Keyspace to remove in setup: %s' % ire

    finally:
        bootstrap.disconnect()

    if success:
        print 'Cassandra Teardown Successful!'
    else:
        raise Exception('Cassandra Teardown Failed!')

def finish_test(status):
    try:
        reactor.stop()
    except ReactorNotRunning, rnr:
        print "Configuration aborted: %s" % rnr
    global exit_status
    exit_status = status

@defer.inlineCallbacks
def run_cassload():
    try:
        yield cassandra_configuration_script()
        yield finish_test(0)
    except Exception:
        traceback.print_exc(file=sys.stdout)
        yield finish_test(1)

def main():
    global exit_status
    exit_status = 4
    run_cassload()
    reactor.run()
    sys.exit(exit_status)
    
if __name__ == '__main__':
    main()
else:
    assert __name__ == CONF_NAME, 'Surprise - you moved me but did not change my CONF name'
