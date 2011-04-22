#!/usr/bin/env python
"""
@file ion/agents/instrumentagents/instrument_agent_constants.py
@author Edward Hunter
@brief Constants associated with instrument agents and drivers.
"""

###############################################################################
# Common driver elements. Below are the constants intended for all instrument
# specific driver implementations, and part of the driver implementation
# framework. 
##############################################################################

"""
Common driver channels.
"""
driver_channel_list = [
    'CHAN_INSTRUMENT',
    'CHAN_TEMPERATURE',
    'CHAN_PRESSURE',
    'CHAN_CONDUCTIVITY'
]

"""
Common driver commands.
"""
driver_command_list = [
    'DRIVER_CMD_ACQUIRE_SAMPLE',
    'DRIVER_CMD_START_AUTO_SAMPLING',
    'DRIVER_CMD_STOP_AUTO_SAMPLING',
    'DRIVER_CMD_TEST',
    'DRIVER_CMD_CALIBRATE',
    'DRIVER_CMD_RESET',
    'DRIVER_CMD_GET',
    'DRIVER_CMD_SET',
    'DRIVER_CMD_GET_STATUS', 
    'DRIVER_CMD_GET_PARAM_METADATA', 
    'DRIVER_CMD_UPDATE_PARAMS',
    'DRIVER_CMD_TEST_ERRORS'
]

"""
Common driver states.
"""
driver_state_list = [
    'STATE_UNCONFIGURED',
    'STATE_DISCONNECTED',
    'STATE_CONNECTING',
    'STATE_DISCONNECTING',
    'STATE_CONNECTED',
    'STATE_ACQUIRE_SAMPLE',
    'STATE_UPDATE_PARAMS',
    'STATE_SET',
    'STATE_AUTOSAMPLE',
    'STATE_TEST',
    'STATE_CALIBRATE'
]

"""
Common driver events.
"""
driver_event_list = [
    'EVENT_CONFIGURE',
    'EVENT_INITIALIZE',
    'EVENT_CONNECT',
    'EVENT_CONNETION_COMPLETE',
    'EVENT_CONNECTION_FAILED',
    'EVENT_DISCONNECT',
    'EVENT_DISCONNECT_COMPLETE',
    'EVENT_PROMPTED',
    'EVENT_DATA_RECEIVED',
    'EVENT_COMMAND_RECEIVED',
    'EVENT_RESPONSE_TIMEOUT',
    'EVENT_SET',
    'EVENT_ACQUIRE_SAMPLE',
    'EVENT_START_AUTOSAMPLE',
    'EVENT_STOP_AUTOSAMPLE',
    'EVENT_TEST',
    'EVENT_STOP_TEST'
    'EVENT_CALIBRATE',
    'EVENT_RESET'
    ]


###############################################################################
# Instrument agent constants.
##############################################################################

"""
Observatory state names.
"""
ci_state_list = [
    'CI_STATE_UNKNOWN',
    'CI_STATE_POWERED_DOWN',
    'CI_STATE_UNINITIALIZED',
    'CI_STATE_INACTIVE',
    'CI_STATE_STOPPED',
    'CI_STATE_IDLE',
    'CI_STATE_OBSERVATORY_MODE',
    'CI_STATE_DIRECT_ACCESS_MODE'
]

"""
Observatory transition names.
"""
ci_transition_list = [
    'CI_TRANS_INITIALIZE',
    'CI_TRANS_RESET',
    'CI_TRANS_GO_ACTIVE',
    'CI_TRANS_GO_INACTIVE',
    'CI_TRANS_CLEAR',
    'CI_TRANS_RESUME',
    'CI_TRANS_RUN',
    'CI_TRANS_PAUSE',
    'CI_TRANS_OBSERVATORY_MODE',
    'CI_TRANS_DIRECT_ACCESS_MODE'
    
]


"""
Observatory commands names.
"""
ci_command_list = [
    'CI_CMD_STATE_TRANSITION',
    'CI_CMD_TRANSMIT_DATA'
]

"""
Parameter names for instrument agents.
"""
ci_param_list = [
    'CI_PARAM_EVENT_PUBLISHER_ORIGIN',
    'CI_PARAM_DRIVER_ADDRESS',
    'CI_PARAM_RESOURCE_ID',
    'CI_PARAM_TIME_SOURCE',
    'CI_PARAM_CONNECTION_METHOD',
    'CI_PARAM_DEFAULT_TRANSACTION_TIMEOUT',
    'CI_PARAM_MAX_TRANSACTION_TIMEOUT',
    'CI_PARAM_TRANSACTION_EXPIRE_TIMEOUT'
]

"""
List of observatory status names.
"""
ci_status_list = [
    'CI_STATUS_AGENT_STATE',
    'CI_STATUS_CHANNEL_NAMES',
    'CI_STATUS_INSTRUMENT_CONNECTION_STATE',
    'CI_STATUS_ALARMS',
    'CI_STATUS_TIME_STATUS',
    'CI_STATUS_BUFFER_SIZE',
    'CI_STATUS_AGENT_VERSION',
    'CI_STATUS_DRIVER_VERSION'    
]

"""
Agent parameter and metadata types.
"""
ci_param_types = [
    'CI_TYPE_DATATYPE',             # This type.
    'CI_TYPE_INT',                  # int.
    'CI_TYPE_FLOAT',                # float.
    'CI_TYPE_BOOL',                 # bool.
    'CI_TYPE_STRING',               # str.
    'CI_TYPE_INT_RANGE',            # (int,int).
    'CI_TYPE_FLOAT_RANGE',          # (float,float).
    'CI_TYPE_TIMESTAMP',            # (int seconds,int nanoseconds).
    'CI_TYPE_TIME_DURATION',        # TBD.
    'CI_TYPE_PUBSUB_TOPIC_DICT',    # dict of topic strings.
    'CI_TYPE_RESOURCE_ID',          # str (possible validation).
    'CI_TYPE_ADDRESS',              # str (possible validation).
    'CI_TYPE_ENUM'                  # str with valid values.
]


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

"""
Publish message types.
"""
publish_msg_types = [
    'PUBLISH_MSG_ERROR',                        
    'PUBLISH_MSG_STATE_CHANGE',
    'PUBLISH_MSG_CONIFG_CHANGE',
    'PUBLISH_MSG_DATA',
    'PUBLISH_MSG_EVENT'    
]


"""
Time source of device fronted by agent.
"""
time_sources = [
    'TIME_NOT_SPECIFIED',                       #
    'TIME_PTP_DIRECT',                          # IEEE 1588 PTP connection directly supported.
    'TIME_NTP_UNICAST',                         # NTP unicast to the instrument.
    'TIME_NTP_BROADCAST',                       # NTP broadcast to the instrument.
    'TIME_LOCAL_OSCILLATOR',                    # Device has own clock.
    'TIME_DRIVER_SET_INTERVAL'                  # Driver sets clock at interval.
]

"""
Connection method to agent and device.
"""
connection_methods = [
    'CONNECTION_NOT_SPECIFIED',                #
    'CONNECTION_OFFLINE',                      # Device offline.
    'CONNECTION_CABLED_OBSERVATORY',           # Accessible through cabled observatory, available full time.
    'CONNECTION_SHORE_NETWORK',                # Connected through full time shore connection.
    'CONNECTION_PART_TIME_SCHEDULED',          # Comes online on scheduled basis. Outages normal.
    'CONNECTION_PART_TIME_RANDOM'              # Comes online as needed. Outages normal.
]

"""
Observatory alarm conditions.
"""
status_alarms = {
    'ALARM_CANNOT_PUBLISH'              : ('ALARM_CANNOT_PUBLISH','Attempted to publish but cannot.'),
    'ALARM_INSTRUMENT_UNREACHABLE'      : ('ALARM_INSTRUMENT_UNREACHABLE','Instrument cannot be contacted when it should be.'),
    'ALARM_MESSAGING_ERROR'             : ('ALARM_MESSAGING_ERROR','Error when sending messages.'),
    'ALARM_HARDWARE_ERROR'              : ('ALARM_HARDWARE_ERROR','Hardware problem detected.'),
    'ALARM_UNKNOWN_ERROR'               : ('ALARM_UNKNOWN_ERROR','An unknown error has occurred.')   
}
    
"""
Names of observatory and device capability lists.
"""
capabilities_list = [
    'CAP_OBSERVATORY_COMMANDS',         # Common and specific observatory command names.
    'CAP_OBSERVATORY_PARAMS',           # Common and specific observatory parameter names.
    'CAP_OBSERVATORY_STATUSES',         # Common and specific observatory status names.
    'CAP_METADATA',                     # Common and specific metadata names.
    'CAP_DEVICE_COMMANDS',              # Common and specific device command names.
    'CAP_DEVICE_PARAMS',                # Common and specific device parameter names.
    'CAP_DEVICE_STATUSES'               # Common and specific device status names.
]

"""
Parameter names for agent and device metadata.
"""
metadata_list = [
    'META_DATATYPE',
    'META_PHYSICAL_PARAMETER_TYPE',
    'META_MINIMUM_VALUE',
    'META_MAXIMUM_VALUE',    
    'META_UNITS',
    'META_UNCERTAINTY',
    'META_LAST_CHANGE_TIMESTAMP',
    'META_WRITABLE',
    'META_VALID_VALUES',    
    'META_FRIENDLY_NAME',
    'META_DESCRIPTION'
]

###############################################################################
# Error constants.
##############################################################################

"""
Agent errors.
"""
errors = {
    'INVALID_DESTINATION'       : ['ERROR','INVALID_DESTINATION','Intended destination for a message or operation is not valid.'],
    'TIMEOUT'                   : ['ERROR','TIMEOUT','The message or operation timed out.'],
    'NETWORK_FAILURE'           : ['ERROR','NETWORK_FAILURE','A network failure has been detected.'],
    'NETWORK_CORRUPTION'        : ['ERROR','NETWORK_CORRUPTION','A message passing through the network has been determined to be corrupt.'],
    'OUT_OF_MEMORY'	        : ['ERROR','OUT_OF_MEMORY','There is no more free memory to complete the operation.'],
    'LOCKED_RESOURCE'	        : ['ERROR','LOCKED_RESOURCE','The resource being accessed is in use by another exclusive operation.'],
    'RESOURCE_NOT_LOCKED'       : ['ERROR','RESOURCE_NOT_LOCKED','Attempted to unlock a free resource.'],
    'RESOURCE_UNAVAILABLE'      : ['ERROR','RESOURCE_UNAVAILABLE','The resource being accessed is unavailable.'],
    'TRANSACTION_REQUIRED'      : ['ERROR','TRANSACTION_REQUIRED','The operation requires a transaction with the agent.'],
    'UNKNOWN_ERROR'             : ['ERROR','UNKNOWN_ERROR','An unknown error has been encountered.'],
    'PERMISSION_ERROR'          : ['ERROR','PERMISSION_ERROR','The user does not have the correct permission to access the resource in the desired way.'],
    'INVALID_TRANSITION'        : ['ERROR','INVALID_TRANSITION','The transition being requested does not apply for the current state.'],
    'INCORRECT_STATE'           : ['ERROR','INCORRECT_STATE','The operation being requested does not apply to the current state.'],
    'UNKNOWN_TRANSITION'        : ['ERROR','UNKNOWN_TRANSITION','The specified state transition does not exist.'],
    'CANNOT_PUBLISH'	        : ['ERROR','CANNOT_PUBLISH','An attempt to publish has failed.'],
    'INSTRUMENT_UNREACHABLE'    : ['ERROR','INSTRUMENT_UNREACHABLE','The agent cannot communicate with the device.'],
    'MESSAGING_ERROR'           : ['ERROR','MESSAGING_ERROR','An error has been encountered during a messaging operation.'],
    'HARDWARE_ERROR'            : ['ERROR','HARDWARE_ERROR','An error has been encountered with a hardware element.'],
    'WRONG_TYPE'                : ['ERROR','WRONG_TYPE','The type of operation is not valid in the current state.'],
    'INVALID_COMMAND'           : ['ERROR','INVALID_COMMAND','The command is not valid in the given context.'],    
    'UNKNOWN_COMMAND'           : ['ERROR','UNKNOWN_COMMAND','The command is not recognized.'],
    'UNKNOWN_CHANNEL'           : ['ERROR','UNKNOWN_CHANNEL','The channel is not recognized.'],
    'INVALID_CHANNEL'           : ['ERROR','INVALID_CHANNEL','The channel is valid for the requested command.'],
    'NOT_IMPLEMENTED'           : ['ERROR','NOT_IMPLEMENTED','The command is not implemented.'],
    'INVALID_TRANSACTION_ID'    : ['ERROR','INVALID_TRANSACTION_ID','The transaction ID is not a valid value.'],
    'INVALID_DRIVER'            : ['ERROR','INVALID_DRIVER','Driver or driver client invalid.'],
    'GET_OBSERVATORY_ERR'       : ['ERROR','GET_OBSERVATORY_ERR','Could not retrieve all parameters.'],
    'EXE_OBSERVATORY_ERR'       : ['ERROR','EXE_OBSERVATORY_ERR','Could not execute observatory command.'],
    'SET_OBSERVATORY_ERR'       : ['ERROR','SET_OBSERVATORY_ERR','Could not set all parameters.'],
    'PARAMETER_READ_ONLY'       : ['ERROR','PARAMETER_READ_ONLY','Parameter is read only.'],
    'INVALID_PARAMETER'         : ['ERROR','INVALID_PARAMETER','The parameter is not available.'],
    'REQUIRED_PARAMETER'        : ['ERROR','REQUIRED_PARAMETER','A required parameter was not specified.'],
    'INVALID_PARAM_VALUE'       : ['ERROR','INVALID_PARAM_VALUE','The parameter value is out of range.'],
    'INVALID_METADATA'          : ['ERROR','INVALID_METADATA','The metadata parameter is not available.'],
    'NO_PARAM_METADATA'         : ['ERROR','NO_PARAM_METADATA','The parameter has no associated metadata.'],
    'INVALID_STATUS'            : ['ERROR','INVALID_STATUS','The status parameter is not available.'],
    'INVALID_CAPABILITY'        : ['ERROR','INVALID_CAPABILITY','The capability parameter is not available.'],
    'BAD_DRIVER_COMMAND'        : ['ERROR','BAD_DRIVER_COMMAND','The driver did not recognize the command.'],
    'EVENT_NOT_HANDLED'         : ['ERROR','EVENT_NOT_HANDLED','The current state did not handle a received event.'],
    'GET_DEVICE_ERR'            : ['ERROR','GET_DEVICE_ERR','Could not retrieve all parameters from the device.'],
    'EXE_DEVICE_ERR'            : ['ERROR','EXE_DEVICE_ERR','Could not execute device command.'],
    'SET_DEVICE_ERR'            : ['ERROR','SET_DEVICE_ERR','Could not set all device parameters.'],
    'ACQUIRE_SAMPLE_ERR'        : ['ERROR','ACQUIRE_SAMPLE_ERR','Could not acquire a data sample.']
  }
