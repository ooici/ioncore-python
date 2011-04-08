#!/usr/bin/env python

"""
@file ion/zapps/association.py
@author David Stuebe
@author Tim LaRocque
@author Matt Rodriguez
@brief An association application that uses a datastore service with a Cassandra backend.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core.process.process import ProcessDesc

from ion.core.pack import app_supervisor

#from ion.core.ioninit import ion_config
from ion.core import ioninit

#-- CC Application interface

# Functions required
@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):

    
    # Get the username for cassandra from the command line. If not present get username from the kwargs for the app.
    uname = ioninit.cont_args.get('username', kwargs.get('username', None))
    pword = ioninit.cont_args.get('password', kwargs.get('password', None))


    if pword is None:
        print_pword = None
    else:
        print_pword = '****'
    log.info('Starting DataStore Service Instance: username: %s, password: %s' % (uname, print_pword))


    spawnargs = {'username':uname,
                  'password':pword,
                  }

    association_proc = [
        {'name':'association_service',
         'module':'ion.services.dm.inventory.association_service',
         'class':'AssociationService',
         'spawnargs':spawnargs
        }
        ]

    appsup_desc = ProcessDesc(name='app-supervisor-' + app_definition.name,
                              module=app_supervisor.__name__,
                              spawnargs={'spawn-procs':association_proc})
    supid = yield appsup_desc.spawn()

    res = (supid.full, [appsup_desc])






    defer.returnValue(res)

@defer.inlineCallbacks
def stop(container, state):
    log.info("state:" + str(state))
    supdesc = state[0]
    log.info("Terminating CC agent")
    yield supdesc.terminate()
