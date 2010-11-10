


class ResponseCodes():
    
    
    """
    Define some constants used in messaging:
    """
    MSG_STATUS = 'status'
    MSG_RESULT = 'result'
    MSG_RESPONSE = 'response'
    MSG_EXCEPTION = 'exception'    
    
    """
    Only used by process.py for uncaught exceptions. Do not catch generic exceptions
    in an ION service - only those which are expected. The Error status and reply error
    are intended only to deal with fatal - unexpected errors.
    """
    ION_ERROR = 'ERROR' 
    """
    Generic OK message added 
    """
    ION_OK = 'OK' 
    
    """
    Generic SUCCESS message for response code default.
    """
    ION_SUCCESS = 'ION SUCCESS'
    
    """
    Suggested list of possible infrastructure level errors
    """
    ION_INVALID_DESTINATION = 'ION ERROR: Invalid Destination'#	Intended destination for a message or operation is not valid
    ION_TIMEOUT = 'ION ERROR: Message Timeout'	# The rpc message timed out
    ION_NETWORK_FAILURE = 'ION ERROR: Network Failure' #	A network failure has been detected
    ION_NETWORK_CORRUPTION = 'ION ERROR: Network Corruption' # A message passing through the network has been determined to be corrupt
    ION_OUT_OF_MEMORY	= 'ION ERROR: Out of Memory' #There is no more free memory to complete the operation
    ION_RESOURCE_UNAVAILABLE = 'ION ERROR: Resource Unavailable' # The resource being accessed is unavailable
    ION_PERMISSION_ERROR = 'ION ERROR: Permission Error' #	The user does not have the correct permission to accessed in the resource in the desired way
    ION_RECEIVER_ERROR = 'ION ERROR: Process Receiver Error'
    
    """
    Applicatiaon level error responses for caught (expected) exceptions
    """
    APP_LOCKEDRESOURCE = 'APPLICATION ERROR: Locked Resource' # The resource being accessed is in use by another exclusive operation
    APP_TIMEOUT = 'APPLICATION ERROR: Time out' # The service operation timed out - be careful with this one - it is the message that should time out really!
    APP_INVALID_KEY = 'APPLICATION ERROR: Invalid Key'
    APP_RESOURCE_STATE_DIVERGED = 'APPLICATION ERROR: Resource State Has Diverged' # A branch of this resource is in a divergent state and must be merged.

    APP_RESOURCE_NOT_FOUND = 'APPLICATION ERROR: Resource Not Found'

    APP_FAILED = 'APPLICATION ERROR: A failure message for demo purposes' # Do not create generic failures!