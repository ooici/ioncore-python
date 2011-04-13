
from ion.core import ioninit

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core.object import object_utils
from ion.core.data.storage_configuration_utility import COMMIT_INDEXED_COLUMNS
from ion.core.process.process import ProcessDesc
from ion.core.pack import app_supervisor

# Functions required
@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):
    processes = [
        {'name':'index_store_service',
         'module':'ion.core.data.index_store_service',
         'class':'IndexStoreService',
                'spawnargs':{'indices':COMMIT_INDEXED_COLUMNS}},
        ]

    appsup_desc = ProcessDesc(name='app-supervisor-'+app_definition.name,
                              module=app_supervisor.__name__,
                              spawnargs={'spawn-procs':processes})
    supid = yield appsup_desc.spawn()

    res = (supid.full, [appsup_desc])

    defer.returnValue(res)

@defer.inlineCallbacks
def stop(container, state):
    supdesc = state[0]
    yield supdesc.terminate()
