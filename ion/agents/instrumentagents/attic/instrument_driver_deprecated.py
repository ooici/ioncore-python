import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)


from twisted.internet import defer
from ion.core.process.process import Process, ProcessClient


"""
Used by the existing drivers...need to fix.
"""
publish_msg_type = {
    'Error':'Error',
    'StateChange':'StateChange',
    'ConfigChange':'ConfigChange',
    'Data':'Data',
    'Event':'Event'
}

class InstrumentDriver(Process):
    """
    A base driver class. This is intended to provide the common parts of
    the interface that instrument drivers should follow in order to use
    common InstrumentAgent methods. This should never really be instantiated.
    """
    def op_fetch_params(self, content, headers, msg):
        """
        Using the instrument protocol, fetch a parameter from the instrument
        @param content A list of parameters to fetch
        @retval A dictionary with the parameter and value of the requested
            parameter
        """

    def op_set_params(self, content, headers, msg):
        """
        Using the instrument protocol, set a parameter on the instrument
        @param content A dictionary with the parameters and values to set
        @retval A small dict of parameter and value on success, empty dict on
            failure
        """

    def op_execute(self, content, headers, msg):
        """
        Using the instrument protocol, execute the requested command
        @param command A list where the command name is the
            first item in the sub-list, and the arguments are the rest of
            the items in the sublists. For example:
            ['command1', 'arg1', 'arg2']
        @retval Result code of some sort
        """

    def op_configure_driver(self, content, headers, msg):
        """
        This method takes a dict of settings that the driver understands as
        configuration of the driver itself (ie 'target_ip', 'port', etc.). This
        is the bootstrap information for the driver and includes enough
        information for the driver to start communicating with the instrument.
        @param content A dict with parameters for the driver
        """

    def op_disconnect(self, content, headers, msg):
        """
        Disconnect from the instrument
        @param none
        """

class InstrumentDriverClient(ProcessClient):
    """
    The base class for the instrument driver client interface. This interface
    is designed to be used by the instrument agent to work with the driver.
    """

    @defer.inlineCallbacks
    def fetch_params(self, param_list):
        """
        Using the instrument protocol, fetch a parameter from the instrument
        @param param_list A list or tuple of parameters to fetch
        @retval A dictionary with the parameter and value of the requested
            parameter
        """
        print
        print 'in fetch'
        assert(isinstance(param_list, (list, tuple)))
        (content, headers, message) = yield self.rpc_send('fetch_params',
                                                          param_list)
        assert(isinstance(content, dict))
        defer.returnValue(content)


    @defer.inlineCallbacks
    def set_params(self, param_dict):
        """
        Using the instrument protocol, set a parameter on the instrument
        @param param_dict A dictionary with the parameters and values to set
        @retval A small dict of parameter and value on success, empty dict on
            failure
        """
        
        assert(isinstance(param_dict, dict))
        (content, headers, message) = yield self.rpc_send('set_params',
                                                          param_dict)
        assert(isinstance(content, dict))
        defer.returnValue(content)



    @defer.inlineCallbacks
    def execute(self, command):
        """
        Using the instrument protocol, execute the requested command
        @param command An ordered list of lists where the command name is the
            first item in the sub-list, and the arguments are the rest of
            the items in the sublists. For example:
            ['command1', 'arg1', 'arg2']
        @retval Result code of some sort
        """
        log.debug("Driver client executing command: %s", command)
        assert(isinstance(command, (list, tuple))), "Bad Driver client execute type"
        (content, headers, message) = yield self.rpc_send('execute',
                                                          command)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_status(self, arg):
        """
        Using the instrument protocol, gather status from the instrument
        @param arg The argument needed for gathering status
        @retval Result message of some sort
        """
        (content, headers, message) = yield self.rpc_send('get_status', arg)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def configure_driver(self, config_vals):
        """
        This method takes a dict of settings that the driver understands as
        configuration of the driver itself (ie 'target_ip', 'port', etc.). This
        is the bootstrap information for the driver and includes enough
        information for the driver to start communicating with the instrument.
        @param config_vals A dict with parameters for the driver
        """
        assert(isinstance(config_vals, dict))
        (content, headers, message) = yield self.rpc_send('configure_driver',
                                                          config_vals)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def initialize(self, arg):
        """
        Disconnect from the instrument
        @param none
        @retval Result code of some sort
        """
        #assert(isinstance(command, dict))
        log.debug("DHE: in initialize!")
        (content, headers, message) = yield self.rpc_send('initialize',
                                                          arg)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def disconnect(self, command):
        """
        Disconnect from the instrument
        @param none
        @retval Result code of some sort
        """
        #assert(isinstance(command, dict))
        log.debug("DHE: in IDC disconnect!")
        (content, headers, message) = yield self.rpc_send('disconnect',
                                                          command)
        defer.returnValue(content)
