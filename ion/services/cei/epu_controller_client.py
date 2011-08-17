import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer
from ion.core.process.service_process import ServiceClient

class EPUControllerClient(ServiceClient):
    """
    Client for sending messages directly to an EPU Controller
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "epu_controller"
        self.force_service_exists = bool(kwargs.get('force_service_exists', False))
        self.epu_controller_name = kwargs['targetname']
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
        yield self._check_init()
        log.debug("Sending reconfigure request to EPU controller: '%s'" % self.target)
        yield self.send('reconfigure', newconf)

    @defer.inlineCallbacks
    def reconfigure_rpc(self, newconf):
        """See reconfigure()
        """
        yield self._check_init()
        yield self.rpc_send('reconfigure_rpc', newconf)
        defer.returnValue(None)

    @defer.inlineCallbacks
    def de_state(self):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('de_state', {})
        log.debug('DE state reply: '+str(content))
        defer.returnValue(str(content))

    @defer.inlineCallbacks
    def whole_state(self):
        yield self._check_init()
        service_exists = True
        if not self.force_service_exists:
            service_exists = yield self.does_service_exist(self.epu_controller_name)
        if not service_exists:
            log.debug("%s.whole_state: Returning static list for AIS unit testing" % self.epu_controller_name)
            de_state = 'STABLE_DE'   # from epu/epucontroller/de_states.py
            # following from epu/decisionengine/impls/npreserving.py & pu/decisionengine/impls/default_engine.py
            de_conf_report = "NpreservingEngine: preserves %d instances (%d unique), sites: %s, types: %s, allocations: %s" \
                        % (2, 1, ["ec2-east"], ["epu_work_consumer"], ["small"])
            instances = {"instance_id_01" : {"iaas_id" : 'i-12345678',
                                             "public_ip" : '1.2.3.4',
                                             "private_ip" : '5.6.7.8',
                                             "iaas_state" : '600-RUNNING',       # from epu/states.py
                                             "iaas_state_time" : 1312908413.77,
                                             "heartbeat_time" : None,
                                             "heartbeat_state" : "OK"            # from epu/epucontroller/health.py
                                             },
                         "instance_id_02" : {"iaas_id" : 'i-98765432',
                                             "public_ip" : '9.8.7.6',
                                             "private_ip" : None,
                                             "iaas_state" : '500-STARTED',       # from epu/states.py
                                             "iaas_state_time" : 1293833968.44,
                                             "heartbeat_time" : 1293833969,
                                             "heartbeat_state" : "ZOMBIE"        # from epu/epucontroller/health.py
                                             }}
            defer.returnValue({"de_state": de_state,
                               "de_conf_report": de_conf_report,
                               "instances": instances})
        log.debug("%s.whole_state: sending whole_state query to epu_controller" % self.epu_controller_name)
        (content, headers, msg) = yield self.rpc_send('whole_state', {})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def node_error(self, node_id):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('node_error', node_id)
        defer.returnValue(content)
