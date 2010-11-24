import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer, reactor
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.process.process import ProcessFactory

class EPUControllerClient(ServiceClient):
    """
    Client for sending messages directly to an EPU Controller
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "epu_controller"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def reconfigure(self, newconf):
        """Triggers a reconfigure option.  This might not be implemented by
        the decision engine implementation that the EPU Controller is
        configured with.  The new configuration is interpreted in a very
        specific way, see the comments and/or documentation for the EPU
        controller (and in particular the decision engine that it is
        expected to be implemented with).
        
        @param newconf None or dict of key/value pairs
        """
        log.debug("Sending reconfigure request to EPU controller: '%s'" % self.target)
        yield self.send('reconfigure', newconf)

class EPUControllerClientSample(ServiceProcess):
    
    declare = ServiceProcess.service_declare(name='epu_reconfigure_sample', version='0.1.0', dependencies=[])
    
    def slc_init(self, proc=None, **kwargs):
        self.client = EPUControllerClient()
        reactor.callLater(5, self.send_reconfigure)
            
    @defer.inlineCallbacks
    def send_reconfigure(self):
        newconf = {"preserve_n":"%s" % self.spawn_args["preserve_n"]}
        self.client.reconfigure(newconf)

factory = ProcessFactory(EPUControllerClientSample)
