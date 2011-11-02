#!/usr/bin/env python

"""
@file ion/services/sa/data_process_management/data_process_management.py
@author
@brief Data processing services enable the derivation of information from lower level information,
on a continuous data streaming basis, for instance for the generation of derived data products.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger (__name__)
from twisted.internet import defer
from twisted.python import reflect
import ion.util.procutils as pu
from ion.services.dm.transformation.transformation_service import TransformationClient
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient

class ProcessErrorCodes:
    OK = ['OK']
    INVALID_TRANSFORM_ID = ['ERROR_INVALID_TRANSFORM_ID','ID number of supplied transaction is not valid.']
    INVALID_TRANSFORM_PARAM = ['ERROR_TRANSFORM_PARAM','Parameter provided to the transformation is not valid.']
    INVALID_CREATOR_DEF = ['ERROR_CREATOR_DEF', 'Definition for the process creator is not valid.']
    INVALID_SCHED_DEF = ['ERROR_SCHEDULE_DEF', 'Definition for transform schedule is not valid.']
    INVALID_PROCESS_NAME = ['ERROR_DUPLICATE_PROCESS', 'Process name already exists.']

    @classmethod
    def is_ok (cls, x):
        """
        Success test functional synonym. Will need iterable type checking if success codes get
        additional info in the future.
        @param x a str, tuple or list to match to an error code success value.
        @retval True if x is a success value, False otherwise.
        """
        x = cls.get_list_val(x)
        return x == cls.OK

    @classmethod
    def get_list_val(cls,x):
        """
        Convert error code values to lists. The messaging framework can
        convert lists to tuples. Allow for simple strings to be compared also.
        """

        assert(isinstance(x,(str,tuple,list))), 'Expected a str, tuple or list \
        error code value.'

        # Object is a list, return unmodified.
        if isinstance(x,list):
            return x

        # Object is a string, return length 1 list with string as the value.
        elif isinstance(x,str):
            return list((x,))

        # Object is a tuple, return a tuple with same elements.
        else:
            return list(x)


class SchedDefCodes:
    ON_NEW_DATA = ['ON_NEW_DATA', 'On receipt of new input data.']
    EVERY_MINUTE = ['EVERY_MINUTE', 'Once every minute ,at the defined number of seconds.']
    ONE_HZ = ['1hz', 'Once every second.']
    HRS_WAIT = ['HOURS_WAIT', 'Wait for the specified number of hours.']
    MINS_WAIT = ['MINUTES_WAIT', 'Wait for the specified number of minutes.']
    SECS_WAIT =['SECONDS_WAIT', 'Wait for the specified number of seconds.']


class DataProcessManagementService (ServiceProcess):

    # Declaration of service
    declare = ServiceProcess.service_declare (name='data_process_mgmt', version='0.1.0', dependencies=[])

    def __init__(self, *args, **kwargs):
        """ """
        log.debug ('DataProcessManagementService.__init__()')
        ServiceProcess.__init__ (self, *args, **kwargs)
        self.transformClient = TransformationClient (proc=self)

    @defer.inlineCallbacks
    def op_attach_process (self, request, headers, msg):
        """ """
        assert (isinstance (request, dict))
        response = self.attach_process (**request)  # Unpack dict to kwargs
        yield self.reply_ok (msg, response)

    @defer.inlineCallbacks
    def op_define_data_process (self, request, headers, msg):
        """ """
        response = self.define_data_process (**request)  # Unpack dict to kwargs
        yield self.reply_ok (msg, response)

    @defer.inlineCallbacks
    def op_get_data_process (self, request, headers, msg):
        """ """
        response = self.get_data_process (**request)  # Unpack dict to kwargs
        yield self.reply_ok (msg, response)


    def attach_process (self, process='default'):

        # Connect the process to the input and output streams

        # Call DM:DataTransformMgmtSvc:DefineTransform to configure

        # Call DM:DataTransformMgmtSvc:BindTransform to connect transform and execute



        return

    @defer.inlineCallbacks
    def define_data_process (self, process='default', params={}):
        """ """
        # TODO: Coordinate orchestration with CEI:ProcessMgmtSvc to define a process
        log.debug ("DataProcessManagementService.define_data_process  params: %s", str (params))
        reply = self.validate_before_define_transform (params)
        dataProcessID = pu.create_guid()

        # Define transform: Register the data process, create and store the resource and associations
        if ProcessErrorCodes.is_ok (reply['success']):
            transformAttr = {'dataProcessID': dataProcessID,
                             'transformID': params['transformID'],
                             'transformParams': params['transformParams'],
                             'creatorDetails': params['creatorDetails']}
            log.debug ("     Calling transformClient.define_transform()")
            result = yield self.transformClient.define_transform (process=process,
                                                                  params=transformAttr)
            reply['success'] = result['success']
            reply['result'] = result['result']
            reply['process'] = params['process']

        # Bind transform: starts subscription to the input parameters?
        if ProcessErrorCodes.is_ok (reply['success']):
            log.debug ("     Calling transformClient.bind_transform()")
            result = yield self.transformClient.bind_transform (process=process)
            reply['success'] = result['success']
            reply['result'] = result['result']

        # Schedule transform: With a good transform that is bound, schedule its execution pattern
        if ProcessErrorCodes.is_ok (reply['success']):
            log.debug ("     Calling transformClient.schedule_transform()")
            result = yield self.transformClient.schedule_transform (process=process,
                                                                    params=params['scheduleDetails'])

        log.debug ("REPLY: " + str (reply))
        defer.returnValue (content=[reply])

    def validate_before_define_transform (self, params):
        """
        List of input parameters
        process         Unique name for the data process
                        Example: ScalarOffsetToTemp_A52
        transformID     ID of the existing transform definition being applied
                        Example: Transform_Scalar
                               - has input variable 1: inTemp = float streaming from a data product
                               - has input variable 2: scalarOffset = float value added to inTemp
        transformParams List of input parameters needed to execute the transform
                        Defined as: (InputVariableName, DataProductID, DataProductVariableName)
                        Example: [('inTemp', '94075689-3189-44B4-A5C2-5703C525A385', 'tempF'),
                                  ('scalarOffset', 'None', '6.3')]
        creatorDetails  Details about who created the process {'name' 'url' 'email' 'instutition')
                        Example: {'name': 'Maurice', 'url': 'www.maurice.com',
                                  'email': 'mmm@maurice.com', 'institution': 'UCSD'}
        scheduleDetails Definition of when the process should be executed
                        Defined as: (schedDefID, {params, ...})
                        Example: (SchedDefCodes.EVERY_MINUTE, {'startSec': 15})
        """
        log.debug ("DataProcessManagementService.validate_before_define_transform  params: %s", str (params))
        reply = {'success': ProcessErrorCodes.OK,
                 'result': None}

        if 'transformID' in params:
            # TODO: Verify that the transformID exists
            #       if bad transformID:
            #           reply['success'] = ProcessErrorCodes.INVALID_TRANSFORM_ID
            pass
        else:
            # Must have valid transformID, otherwise what transform to apply?
            params['transformID'] = ""
        if not params['transformID']:
            reply['success'] = ProcessErrorCodes.INVALID_TRANSFORM_ID

        # It is possible that a transform would have no parameters;
        #   if transformID should have had params, it will be caught in the validation...
        if 'transformParams' not in params:
            params['transformParams'] = {}
        # TODO: Validate transformation parameters against the specified transform
        #       if bad param or params:
        #          reply['success'] = ProcessErrorCodes.INVALID_TRANSFORM_PARAM

        if 'creatorDetails' in params:
            # TODO: Validate creator detail parameters
            #       if bad creator params:
            #           reply['success'] = ProcessErrorCodes.INVALID_CREATOR_DEF
            pass
        else:
            # Must have creator details
            reply['success'] = ProcessErrorCodes.INVALID_CREATOR_DEF

        if 'scheduleDetails' in params:
            # TODO: Validate schedule detail parameters
            #       if bad schedule details:
            #           reply['success'] = ProcessErrorCodes.INVALID_SCHED_DEF
            pass
        else:
            # OK if no schedule defintion, default is execute on new data
            params['scheduleDetails'] = (SchedDefCodes.ON_NEW_DATA, {})

        return reply

    def get_data_process(self, process='default'):

        # Locate a process based on metadata filters



        return



class DataProcessManagementServiceClient(ServiceClient):

    """
    This is a service client for DataProcessManagementServices.
    """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "data_process_mgmt"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def attach_process(self, process='default'):
        (content, headers, msg) = yield self.rpc_send('attach_process', {'process':process})
        defer.returnValue(content)

    @defer.inlineCallbacks
    def define_data_process(self, process='default'):
        log.debug ("DataProcessManagementServiceClient: define_data_process()")
        (content, headers, msg) = yield self.rpc_send ('define_data_process', {'process':process})
        defer.returnValue (content)

    @defer.inlineCallbacks
    def get_data_process(self, process='default'):
        (content, headers, msg) = yield self.rpc_send('get_data_process', {'process':process})
        defer.returnValue(content)


# Spawn of the process using the module name
factory = ProcessFactory(DataProcessManagementService)

  
