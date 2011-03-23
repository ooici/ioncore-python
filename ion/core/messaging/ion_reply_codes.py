


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

    BAD_REQUEST = 400
    UNAUTHORIZED = 401
