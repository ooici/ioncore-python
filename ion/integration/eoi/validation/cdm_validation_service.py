#!/usr/bin/env python

"""
@file:   ion/integration/eoi/validation/cdm_validation_service.py
@author: Tim LaRocque
@brief:  CdmValidationService and CdmValidationClient definitions for validating
         user-provided datasets against OOICI Common Data Model requirements and
         Climate and Forecast conventions.
"""



# Imports: Builtin
import re, urllib


# Imports: Twisted
from twisted.internet import defer
from twisted.python.failure import Failure


# Imports: ION core
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.process.process import ProcessFactory
from ion.core.messaging.message_client import MessageClient
from ion.core.object import object_utils
#from ion.services.coi.resource_registry.resource_client import \
#    ResourceClient


# Imports: ION services
#from ion.services.dm.ingestion.ingestion import IngestionClient
#from ion.services.coi.datastore_bootstrap.ion_preload_config import TESTING_SIGNIFIER


# Imports: ION utils and configuration
from ion.util.os_process import OSProcess
from ion.util import ionlog, procutils as pu
from ion.core import ioninit

log = ionlog.getLogger(__name__)
CONF = ioninit.config(__name__)


# Imports: Message types
VALIDATION_REQUEST_TYPE = object_utils.create_type_identifier(object_id=7101, version=1)
VALIDATION_RESPONSE_TYPE = object_utils.create_type_identifier(object_id=7102, version=1)



class CdmValidationService(ServiceProcess):
    """
    """
    
    
    declare = ServiceProcess.service_declare(name='cdm_validation_service',
                                             version='0.1.0',
                                             dependencies=[])

        
    def __init__(self, *args, **kwargs):
        '''
        Initialize the CdmValidationService instance, init instance fields, etc.
        '''
        # Step 1: Delegate initialization to parent "ServiceProcess"
        log.info('')
        log.info('Initializing class instance...')
        ServiceProcess.__init__(self, *args, **kwargs)
        
        # Step 2: Create class attributes
        self._cfchecks_binary = None
        self._cfchecks_args = None


    @defer.inlineCallbacks
    def slc_init(self):
        '''
        Initialization upon Service spawning.
        '''
        log.debug(" -[]- Entered slc_init(); state=%s" % (str(self._get_state())))
        
        # Step 1: Delegate initialization to parent class
        yield defer.maybeDeferred(ServiceProcess.slc_init, self)
        
        # Step 2: Perform Initialization
        self.mc = MessageClient(proc=self)
#        self.rc = ResourceClient(proc=self)
        
        # Step 3:  @todo: Check the location of the cfchecker script
        
    
    @defer.inlineCallbacks
    def op_validate(self, content, headers, msg):
        """
        @todo: doc
        """
        yield
        log.info('<<<---@@@ (service) Received operation "validate".  Extracting data_url to perform validation')
        if not hasattr(content, 'MessageType') or content.MessageType != VALIDATION_REQUEST_TYPE:
            raise TypeError('The given content must be an instance or a wrapped instance of %s.  Given: %s' % (repr(VALIDATION_REQUEST_TYPE), type(content)))
        
        # Step 1: Get the data_url
        log.debug('op_validate(): Retrieving data_url...')
        data_url = str(content.data_url)
        

        # Step 2: Validate the URL against the CDM Validator WebService
        cdm_output = self.validate_cdm(data_url)
        cdm_resp = yield self.process_cdm_validation_output(cdm_output)
        
        
        # Step 3: Validate the URL against the CF Checks Script
        cf_resp = {}
        if cdm_resp['cdm_result']:
            cf_result = yield self.validate_cf(data_url)
            cf_resp   = self.process_cf_validation_output(cf_result)
            
            
        # Step 4: Combine outputs and respond to the originating message
        cdm_resp.update(cf_resp)
        result   = yield self.respond_to_originator(msg, **cdm_resp)
    

        log.debug('op_validate(): Validation complete')
        defer.returnValue(result)
    
    
    def validate_cdm(self, data_url):
        """
        """
        log.debug('')
        base_url = self.cdmvalidator_base_url
        command = self.cdmvalidator_command
        
        full_url = '%s/%s?URL=%s&xml=true' % (base_url, command, urllib.quote(data_url))
        
        f = None
        try:
            log.debug('validate_cdm(): Requesting validation from CDMValidator WS: \n\n"%s"\n\n' % full_url)
            f = urllib.urlopen(full_url)
            result = f.readlines()
            if isinstance(result, list):
                result = "".join(result)
        except IOError, ex:
            pass
        finally:
            if f:
                f.close()

        log.debug('')
        return result


    @defer.inlineCallbacks
    def validate_cf(self, data_url):
        """
        """
        log.debug('')
        try:
            # Send the url to the cfchecks script
            binary = self.cfchecks_binary
            args   = list(self.cfchecks_args)
            args.append(data_url)

            # @todo:  Error if binary is None
            proc = OSProcess(binary, args)
            
            # Start the process
            log.debug('validate_cf(): Requesting validation from CFChecks validation script: \n\n"%s %s"\n\n' % (binary, " ".join([str(item) for item in args])))
            res = yield proc.spawn()
            
            
        except (AttributeError, ValueError), ex:
            
            raise RuntimeError("validate_cf(): Received invalid spawn arguments from ionconfig" + str(ex))
        
        except OSError, ex:
            
            raise RuntimeError("validate_cf(): Failed to spawn the cfchecks script.  Error: %s" % (str(ex)))
        
        except StandardError, ex:
            
            # This is the same result as would be acquired through normal processing via OSProcess
            # ..  a StandardError is thrown when the process terminates with an exit code other than
            #     0. In this case, we still are only concerned with the results (inside ex.message)
            res = ex.message
            
        log.debug('')
        defer.returnValue(res)
        

    @property
    def cdmvalidator_base_url(self):
        url = CONF.getValue('cdmvalidator_base_url', None)
        log.info('Retrieved cdmvalidator base url: "%s"' % url)
        return url
        

    @property
    def cdmvalidator_command(self):
        cmd = CONF.getValue('cdmvalidator_command', None)
        log.info('Retrieved cdmvalidator command: "%s"' % cmd)
        return cmd
    
        
    @property
    def cfchecks_binary(self):
        binary = CONF.getValue('cfchecks_binary', None)
        log.info('Retrieved cfchecks binary: "%s"' % binary)
        return binary
    
    
    @property
    def cfchecks_args(self):
        args = CONF.getValue('cfchecks_args', [])
        log.info('Retrieved cfchecks args:   "%s"' % str(args))
        return args
    
    
    def process_cdm_validation_output(self, cdm_output):
        """
        """
        #---------------------------------------#
        # Process CdmValidator output...
        #---------------------------------------#
        log.info('process_cdm_validation_output(): Processing CDM Validator output...')

        # Parse cdm output to determine pass/fail
        cdm_axis_list = []
        for m in re.finditer(r'<axis.*?type=\"(?P<axis_type>.*?)\"', cdm_output):
            cdm_axis_list.append(m.groupdict()['axis_type'])
        log.debug('Available CDM axis:  %s' % cdm_axis_list)
        result = 'Time' in cdm_axis_list
        
        cdm_result = {'cdm_result':result, 'cdm_output':cdm_output}
        
        return cdm_result

    
    def process_cf_validation_output(self, cf_output):
        """
        """
        #---------------------------------------#
        # Process CFChecks output...
        #---------------------------------------#
        log.info('process_validation_output(): Processing CFChecks validation output...')
        
        # Step 1: Retrieve the output data
        exitcode = cf_output.get('exitcode', 0)
        outlines = cf_output.get('outlines', None)
        errlines = cf_output.get('errlines', None)

        outlines.extend(errlines)
        cf_output_string = '\n'.join(outlines)
        
        # Step 2: Parse the output to determine pass/fail
        cf_result_dict = {}
        for m in re.finditer(r'(?P<level>ERRORS|WARNINGS|INFORMATION).+?\: (?P<number>\d+)', cf_output_string):
            cf_result_dict['cf_%s' % m.groupdict()['level'].lower()] = int(m.groupdict()['number'])
        
        cf_result_dict['cf_exitcode'] = exitcode
        cf_result_dict['cf_output'] = cf_output_string
        
        
        return cf_result_dict
    
    
    @defer.inlineCallbacks
    def respond_to_originator(self, msg, **kwargs):
        """
        """
        log.debug('respond_to_originator(): Replying to caller...')
        #---------------------------------------#
        # Produce a response
        #---------------------------------------#
        log.info('respond_to_originator(): Building a response object for this validation request...')
        
        # Retrieve values from kwargs
        cdm_result = kwargs.get('cdm_result', False)
        cdm_output = kwargs.get('cdm_output', '')
        cf_output  = kwargs.get('cf_output', '')
        cf_errors  = kwargs.get('cf_errors', 0)
        cf_warnings = kwargs.get('cf_warnings', 0)
        cf_information = kwargs.get('cf_information', 0)
        cf_exitcode = kwargs.get('cf_exitcode', -1)
        
        
        # Build the response object
        response = yield self.mc.create_instance(VALIDATION_RESPONSE_TYPE)
        R = response.ResponseType
        response.response_type = R.CDM_FAILURE if not cdm_result else \
                                 R.CF_FAILURE if cf_errors else \
                                 R.PASS
        response.cf_output        = cf_output
        response.cdm_output       = cdm_output
        response.cf_error_count   = cf_errors
        response.cf_warning_count = cf_warnings
        response.cf_info_count    = cf_information

            
        log.info('@@@--->>> (service) Sending OK response to originator')
        res = yield self.reply_ok(msg, response)
            
            
        defer.returnValue(res)
        
        
class CdmValidationClient(ServiceClient):
    """
    Test client for direct (RPC) interaction with the CdmValidationService
    """
    
    def __init__(self, *args, **kwargs):
        # Step 1: Delegate initialization to parent "ServiceClient"
        kwargs['targetname'] = 'cdm_validation_service'
        ServiceClient.__init__(self, *args, **kwargs)
        
        # Step 2: Perform Initialization
        self.mc = MessageClient(proc=self.proc)
#        self.rc = ResourceClient(proc=self.proc)
        
    
    @defer.inlineCallbacks
    def validate(self, data_url):
        '''
        Builds the data_url into a cdm_validation_response message and sends it to the
        CdmValidationService to validate the dataset specified by that url
        '''
        # Ensure a Process instance exists to send messages FROM...
        #   ...if not, this will spawn a new default instance.
        yield self._check_init()
        
        # Create the validation request instance
        request = yield self.mc.create_instance(VALIDATION_REQUEST_TYPE)
        request.data_url = data_url
        
        # Invoke [op_]validate() on the target service 'cdm_validation_service' via RPC
        log.info("@@@--->>> (client) Sending 'validate' rpc to cdm_validation_service")

        (content, headers, msg) = yield self.rpc_send('validate', request, timeout=self.validation_timeout)
        log.info("<<<---@@@ (client) Retrieve validation reponse from cdm_validation_service")
        
        def response_type_pretty(self):
            reasons = {self.ResponseType.NONE:               'No reason',            
                       self.ResponseType.PASS:               'Validation Passed!',
                       self.ResponseType.CDM_FAILURE:        'CDM (time-axis) validation failed',
                       self.ResponseType.CF_FAILURE:         'CF compliance failed',
                       self.ResponseType.ERROR:              'ERROR.  View err_msg field for more'}
            return reasons[self.response_type]
        
        log.debug('')
        log.debug('')
        log.info('%-15s %s' % ('Is Valid:', content.response_type == content.ResponseType.PASS))
        log.info('%-15s %s' % ('Reason:', response_type_pretty(content)))
#        log.debug('%-15s \n\n%s\n' % ('CF Output:', content.cf_output))
#        log.debug('%-15s \n\n%s\n' % ('CDM Output:', content.cdm_output))
        log.debug('%-15s %i' % ('Error count:',   content.cf_error_count))
        log.debug('%-15s %i' % ('Warning count:', content.cf_warning_count))
        log.debug('%-15s %i' % ('Info count:',    content.cf_info_count))
        log.debug('')
        log.debug('')

        
        defer.returnValue(content)
        
        
    @property
    def validation_timeout(self):
        timeout = CONF.getValue('validation_timeout', 60)
        log.info('Retrieved validation timeout: %i' % timeout)
        return timeout
        
        
    
# Spawn of the process using the module name
factory = ProcessFactory(CdmValidationService)



'''

#----------------------------#
# Validation Setup
#----------------------------#
You MUST add the following entries to ionlocal.config...
[change the name of 'cfchecks_binary' where appropriate]

'ion.integration.eoi.validation.cdm_validation_service':{
    'validation_timeout' : 60,
    'cfchecks_binary' : '/Users/tlarocque/Development/OOI/code/ioncore-python/run_cf_checks',
    'cdmvalidator_base_url' : 'http://motherlode.ucar.edu:8080/cdmvalidator',
    'cdmvalidator_command' : 'validate',
}



#----------------------------#
# Application Startup
#----------------------------#
:: bash ::
bin/twistd -n cc -h amoeba.ucsd.edu -a sysname=eoitest res/apps/resource.app



#----------------------------#
# Validation Testing
#----------------------------#
:: py ::
from ion.integration.eoi.validation.cdm_validation_service import CdmValidationClient as cvc
spawn('cdm_validation_service')
client = cvc()


# PICK ONE of the following
#res_d = client.validate('http://tashtego.marine.rutgers.edu:8080/thredds/dodsC/cool/avhrr/bigbight')
res_d = client.validate('http://thredds1.pfeg.noaa.gov/thredds/dodsC/satellite/GR/ssta/1day')



res = res_d.result

'''












