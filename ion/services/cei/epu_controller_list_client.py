
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer
from ion.core.process.service_process import ServiceClient

class EPUControllerListClient(ServiceClient):
    """Client for querying EPUControllerListService
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "epu_controller_list"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def list(self):
        """Query the EPUControllerListService
        """
        yield self._check_init()
        log.debug("Sending EPU controller list query")
        (content, headers, msg) = yield self.rpc_send('list', {})
        defer.returnValue(content)

