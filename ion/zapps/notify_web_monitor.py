

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core.process.process import ProcessDesc

from ion.core.pack import app_supervisor

#from ion.core.ioninit import ion_config
from ion.core import ioninit
from ion.core.cc.shell import control



# --- CC Application interface

# Functions required
@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):
    notify_proc = [
        {'name':'notify_web_monitor_1',
         'module':'ion.services.dm.distribution.notify_web_monitor',
         'class':'NotificationWebMonitorService',
         'spawnargs':{}
            }
        ]

    appsup_desc = ProcessDesc(name='app-supervisor-'+app_definition.name,
                              module=app_supervisor.__name__,
                              spawnargs={'spawn-procs':notify_proc})
    supid = yield appsup_desc.spawn()

    res = (supid.full, [appsup_desc])
    defer.returnValue(res)

@defer.inlineCallbacks
def stop(container, state):
    log.info("state:" +str(state) )
    supdesc = state[0]
    log.info("Terminating Notify Web Monitor")
    yield supdesc.terminate()

