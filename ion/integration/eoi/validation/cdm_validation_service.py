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


# Imports: ION core
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.process.process import ProcessFactory
from ion.core.messaging.message_client import MessageClient
from ion.core.object import object_utils


# Imports: ION utils, configuration and logging
import logging
from ion.core import ioninit
from ion.util import ionlog
from ion.util.os_process import OSProcess, OSProcessError
import os

log = ionlog.getLogger(__name__)
CONF = ioninit.config(__name__)


# Imports: Message types
VALIDATION_REQUEST_TYPE = object_utils.create_type_identifier(object_id=7101, version=1)
VALIDATION_RESPONSE_TYPE = object_utils.create_type_identifier(object_id=7102, version=1)



class CdmValidationService(ServiceProcess):
    """
    @brief: ServiceProcess used in validating user-provided datasets against OOICI Common Data Model requirements and Climate and Forecast conventions.
    """
    
    
    declare = ServiceProcess.service_declare(name='cdm_validation_service',
                                             version='0.1.0',
                                             dependencies=[])

        
    def __init__(self, *args, **kwargs):
        '''
        @brief: Initialize the CdmValidationService instance, init instance fields, etc.
        '''
        # Step 1: Delegate initialization to parent "ServiceProcess"
        log.info('')
        log.info('Initializing class instance...')
        ServiceProcess.__init__(self, *args, **kwargs)
        
        # Step 2: Create class attributes
        self._cfchecks_binary = None
        self._cfchecks_args = None


    def slc_init(self):
        '''
        @brief: Initialization upon Service spawning.  Sets up clients, etc.
        
        @attention Nothing to yield -- this is NOT an inlineCallback
        '''
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug(" -[]- Entered slc_init(); state=%s" % (str(self._get_state())))
        
        # Step 1: Perform Initialization
        self.mc = MessageClient(proc=self)
        
    
    @defer.inlineCallbacks
    def op_validate(self, content, headers, msg):
        """
        @brief: Validate the given cdm_validation_request (GPB{7101}) against the common data model (CDM) and then
                against Climate and Forecast (CF) conventions.  CDM validation is inacted against the CDM Validation
                Web Service provided by the config as 'cdmvalidator_base_url' using the command provided by
                'cdmvalidator_command' (usu. 'validate').  CF validation is inacted against the CfChecks python
                script provided by the config as 'cfchecks_binary'.
                
                Here is an example setup of the CdmValidationService's configuration:
                
                        'ion.integration.eoi.validation.cdm_validation_service':{
                            'validation_timeout' : 60,
                            'cfchecks_binary' : '/Users/tlarocque/Development/OOI/code/ioncore-python/run_cf_checks',
                            'cdmvalidator_base_url' : 'http://motherlode.ucar.edu:8080/cdmvalidator',
                            'cdmvalidator_command' : 'validate',
                        }
                
        @note:  If CDM validation fails, CF Checking will be skipped.
        
        @return: Sends an instance of a cdm_validation_response (GPB{7102}) resource to the remote caller.
        """
        yield # some paths do not yield in this method -- when these are traversed, generator unwinding will fail
        log.info('<<<---@@@ (service) Received operation "validate".  Extracting data_url to perform validation')
        if not hasattr(content, 'MessageType') or content.MessageType != VALIDATION_REQUEST_TYPE:
            raise TypeError('The given content must be an instance or a wrapped instance of %s.  Given: %s' % (repr(VALIDATION_REQUEST_TYPE), type(content)))
        
        # Step 1: Get the data_url
        log.debug('op_validate(): Retrieving data_url...')
        data_url = str(content.data_url)
        

        # Step 2: Validate the URL against the CDM Validator WebService
        try:
            cdm_output = self.validate_cdm(data_url)
            cdm_resp = yield self.process_cdm_validation_output(cdm_output)
        except Exception, ex:
            log.warn('CDM Validation or validation output processing failed:  Cause: %s' % str(ex))
            cdm_resp = {'cdm_result':False, 'exception':'Could not perform CDM Validation.  Please check the CDM Validator configuration.  Inner exception: %s' % ex}
        
        # Step 3: Validate the URL against the CF Checks Script
        cf_resp = {}
        if cdm_resp['cdm_result']:
            try:
                cf_result = yield self.validate_cf(data_url)
                cf_resp   = self.process_cf_validation_output(cf_result)
            except Exception, ex:
                log.warn('CF Validation or validation output procssing failed.  Cause: %s' % str(ex))
                cf_resp = {'exception':'Could not perform CF Validation.  Please check the CF Checker configuration.  Inner exception: %s' % ex}
            
            
            
        # Step 4: Combine outputs and respond to the originating message
        cdm_resp.update(cf_resp)
        response = yield self.build_response_msg(msg, **cdm_resp)
    

        log.info('@@@--->>> (service) Sending OK response to originator')
        yield self.reply_ok(msg, response)
    
    
    def validate_cdm(self, data_url):
        """
        @brief: Validates the given data_url against the CDM Validation Webservice
        @param data_url: The url to validate
        
        @return: The resultant XML from the CDM Validation Service
        @see:    CdmValidationService.op_validate()
        """
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
        finally:
            if f:
                f.close()

        return result


    @defer.inlineCallbacks
    def validate_cf(self, data_url):
        """
        @brief: Validates the given data_url against the CfChecks python script
        @param data_url: The url to validate
        
        @return: The resultant string data from the CfChecks Python script
        @see:    CdmValidationService.op_validate()
        """
        log.debug('')
        try:
            # Send the url to the cfchecks script
            binary = self.cfchecks_binary
            args   = list(self.cfchecks_args)
            args.append(data_url)

            # @todo:  Error if binary is None
            if None == binary or not os.path.exists(binary):
                raise OSError("CfChecks binary (given by configuration as 'cfchecks_binary' does not specify a valid filepath: '%s'" % binary)
            proc = OSProcess(binary, args)
            
            # Start the process
            if log.getEffectiveLevel() <= logging.DEBUG:
                log.debug('validate_cf(): Requesting validation from CFChecks validation script: \n\n"%s %s"\n\n' % (binary, " ".join([str(item) for item in args])))
            res = yield proc.spawn()
            
        except (AttributeError, ValueError), ex:
            raise RuntimeError("validate_cf(): Received invalid spawn arguments from ionconfig.  Inner Exception: %s"  % str(ex))

        except OSError, ex:
            raise OSError("validate_cf(): Failed to spawn the cfchecks script.  Cause: %s" % (str(ex)))
        
        except OSProcessError, ex:
            # This exception is thrown by OSProcess when the underlying binary terminates with an
            #     exit code other than 0. In this case, we are only concerned with the results
            #     (inside ex.message) and will later return the error condition to higher application
            #     levels 
            res = ex.message
            
            
        defer.returnValue(res)
        

    @property
    def cdmvalidator_base_url(self):
        """
        @return: The value of the field 'cdmvalidator_base_url' from the CdmValidationService configuration
        """ 
        url = CONF.getValue('cdmvalidator_base_url', None)
        log.info('Retrieved cdmvalidator base url: "%s"' % url)
        return url
        

    @property
    def cdmvalidator_command(self):
        """
        @return: The value of the field 'cdmvalidator_command' from the CdmValidationService configuration
        """ 
        cmd = CONF.getValue('cdmvalidator_command', None)
        log.info('Retrieved cdmvalidator command: "%s"' % cmd)
        return cmd
    
        
    @property
    def cfchecks_binary(self):
        """
        @return: The value of the field 'cfchecks_binary' from the CdmValidationService configuration
        """ 
        binary = CONF.getValue('cfchecks_binary', None)
        log.info('Retrieved cfchecks binary: "%s"' % binary)
        return binary
    
    
    @property
    def cfchecks_args(self):
        """
        @return: The value of the field 'cfchecks_args' from the CdmValidationService configuration
        """ 
        args = CONF.getValue('cfchecks_args', [])
        if log.getEffectiveLevel() <= logging.INFO:
            log.info('Retrieved cfchecks args:   "%s"' % str(args))
        return args
    
    
    def process_cdm_validation_output(self, cdm_output):
        """
        @brief: Parses the output from the CDM Validation WebService to determine passage/failure
        @param cdm_output: The XML response from the CDM Validation WebService as returned from
                           CdmValidationService.validate_cdm()
        @note:  Currently this mechanism simply ensures that the dataset being validated contains
                time axis information -- if it does not, it cannot be handled by the system

        @return: A dictionary containing values for the keys:
                   'cdm_output'
                   'cdm_result'
        """
        #---------------------------------------#
        # Process CdmValidator output...
        #---------------------------------------#
        log.info('process_cdm_validation_output(): Processing CDM Validator output...')

        cdm_result = {}

        # Check for failed HTTP status
        if re.search('(?m)Error report.*?(?=HTTP Status ([0-9]+))', cdm_output):
            
            err_code = re.search('(?m)Error report.*?(?=HTTP Status ([0-9]+))', cdm_output).group(1)
            exception = ''

            if err_code == '404':
                exception = 'HTTP Status 404: CDM Validator command may be invalid.  Please check CDM Validator configuration.'
            elif err_code == '500':
                exception = 'HTTP Status 500: Internal Server Error.  The given Dataset location may be invalid.'
            else:
                exception = 'HTTP Status %s: CDM Validation failed.  Please review the cdm_output' % err_code
                
            result = False
            cdm_result['exception']  = exception
     
        else:
            
            # Parse cdm output to determine pass/fail
            cdm_axis_list = []
            for m in re.finditer(r'<axis.*?type=\"(?P<axis_type>.*?)\"', cdm_output):
                cdm_axis_list.append(m.groupdict()['axis_type'])
            if log.getEffectiveLevel() <= logging.DEBUG:
                log.debug('Available CDM axis:  %s' % cdm_axis_list)
            result = 'Time' in cdm_axis_list
        
        
        cdm_result['cdm_result'] = result
        cdm_result['cdm_output'] = cdm_output
        
        
        return cdm_result

    
    def process_cf_validation_output(self, cf_output):
        """
        @brief: Parses the output from the CfChecks Python script to determine the number of
                errors, warnings, and info messages associated with the dataset being validated.
        
        @param cf_output: The string response from the CfChecks Python script as returned from
                          CdmValidationService.validate_cf()
        
        @return: A dictionary containing values for the keys:
                   'cf_errors'
                   'cf_warnings'
                   'cf_infomation'
                   'cf_exitcode'
                   'cf_output'
        """
        #---------------------------------------#
        # Process CFChecks output...
        #---------------------------------------#
        log.info('process_validation_output(): Processing CFChecks validation output...')
        
        # Step 1: Retrieve the output data
        exitcode = cf_output.get('exitcode', 0)
        outlines = cf_output.get('outlines', [])
        errlines = cf_output.get('errlines', [])

        outlines.extend(errlines)
        cf_output_string = '\n'.join(outlines)
        
        # Step 2: Parse the output to determine pass/fail
        cf_result_dict = {}
        for m in re.finditer(r'(?P<level>ERRORS|WARNINGS|INFORMATION).+?\: (?P<number>\d+)', cf_output_string):
            cf_result_dict['cf_%s' % m.groupdict()['level'].lower()] = int(m.groupdict()['number'])
        
        cf_result_dict['cf_exitcode'] = exitcode
        cf_result_dict['cf_output'] = cf_output_string
        
        #--------------------------------------------------------------------------------
        #
        # @attention:      IMPORTANT NOTE!!
        #
        #     exitcode 0 means success
        #     exitcode 255 is returned from shell when cfchecks returns an invalid exit code.
        #     .. this is expected behavior because when there are no errors but warnings are
        #        present, cfchecks will return a negative exit code -- negative exit codes
        #        are considered invalid by shell -- ergo exitcode 255 means warnings ONLY
        #
        #        There was concern that cfchecks may produce a response of 255 when it
        #        in fact has failed, and the CdmValidationService may incorrectly report
        #        success.  This WILL NOT happen, however, because in this case, the analysis
        #        of the cfchecks output will reveal the actual number of errors, allowing an
        #        accurate pass/fail response
        #--------------------------------------------------------------------------------
        #
        if exitcode == 255: exitcode = 0
        
        if exitcode != 0 and cf_result_dict.get('cf_errors', 0) == 0:
            # There are no cf_errors because processing terminated with
            # an exitcode..
            cf_result_dict['exception'] = 'Recieved failure exitcode (%i) during CF Validation.  Please check the CF Checker configuration.  Inner exception: %s' % (exitcode, cf_output_string)
            cf_result_dict['cf_errors'] = 1
        
        return cf_result_dict
    
    
    @defer.inlineCallbacks
    def build_response_msg(self, msg, **kwargs):
        """
        @brief: Constructs a cdm_validation_response (GPB{7102}) resource from the parameters passed through kwargs.
        @param kwargs: Parameters used in building the cdm_validation_response object.  These are the results from
                       the methods process_cdm_validation_output() and process_cf_validation_output()
                       
                       Accepted parameters are the following:
                           exception
                           cdm_output
                           cdm_result
                           cf_output
                           cf_exitcode
                           cf_errors
                           cf_warnings
                           cf_information
                           cf_exitcode
                       
        @note: Example Use:
        
                       cdm_res = self.process_cdm_validation_output(cdm_output)
                       cf_res  = self.process_cf_validation_output(cf_output)
                       cdm_res.update(cf_res)
                       
                       response_resource = yield self.build_response_msg(**cdm_res)
                       
        @return: a deferred containing the cdm_validation_response resource
        """
        log.debug('build_response_msg(): Replying to caller...')
        #---------------------------------------#
        # Produce a response
        #---------------------------------------#
        log.info('build_response_msg(): Building a response object for this validation request...')
        
        # Retrieve values from kwargs
        exception   = kwargs.get('exception', None)
        cdm_output  = kwargs.get('cdm_output', '')
        cdm_result  = kwargs.get('cdm_result', False)
        cf_output   = kwargs.get('cf_output', '')
        cf_exitcode = kwargs.get('cf_exitcode', 0) 
        cf_errors   = kwargs.get('cf_errors', 0)
        cf_warnings = kwargs.get('cf_warnings', 0)
        cf_information = kwargs.get('cf_information', 0)
        cf_exitcode = kwargs.get('cf_exitcode', -1)
        
        
        # Build the response object
        response = yield self.mc.create_instance(VALIDATION_RESPONSE_TYPE)
        R = response.ResponseType
        response.response_type = R.ERROR if exception else \
                                 R.CDM_FAILURE if not cdm_result else \
                                 R.CF_FAILURE if cf_errors else \
                                 R.PASS
        response.cdm_output       = cdm_output
        response.cf_output        = cf_output
        response.cf_error_count   = cf_errors
        response.cf_warning_count = cf_warnings
        response.cf_info_count    = cf_information
        response.err_msg          = exception or ''

        defer.returnValue(response)
        
        
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


        if log.getEffectiveLevel() == logging.INFO:        
            log.info('%-25s %s' % ('Passes Validation:', content.response_type == content.ResponseType.PASS))
            log.info('%-25s %s' % ('Response:',          response_type_pretty(content)))
            log.info('%-25s %s' % ('Error Message:',     content.err_msg))
        elif log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('')
            log.debug('')
            log.debug('%-25s %s' % ('Passes Validation:', content.response_type == content.ResponseType.PASS))
            log.debug('%-25s %s' % ('Response:',          response_type_pretty(content)))
            log.debug('%-25s %i' % ('CF Error count:',    content.cf_error_count))
            log.debug('%-25s %i' % ('CF Warning count:',  content.cf_warning_count))
            log.debug('%-25s %i' % ('CF Info count:',     content.cf_info_count))
            log.debug('%-25s %s' % ('Error Message:',     content.err_msg))
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
res_d = client.validate('http://geoport.whoi.edu/thredds/dodsC/usgs/data0/rsignell/data/oceansites/OS_NTAS_2010_R_M-1.nc')
res_d = client.validate('http://thredds1.pfeg.noaa.gov/thredds/dodsC/satellite/GR/ssta/1day')
res_d = client.validate('http://tashtego.marine.rutgers.edu:8080/thredds/dodsC/cool/avhrr/bigbight')



res = res_d.result

'''












