#
# AIS ObjectIDs
#
from ion.core.object import object_utils

# AIS GPBs from ion-object-definitions/net/ooici/integration/ais/ais_request_response.proto
AIS_REQUEST_MSG_TYPE = object_utils.create_type_identifier(object_id=9001, version=1)
"""
message ApplicationIntegrationServiceRequestMsg{
    enum _MessageTypeIdentifier {
      _ID = 9001;
      _VERSION = 1;
    }
    
    // The message parameters object
    optional net.ooici.core.link.CASRef message_parameters_reference = 1;
}
"""

AIS_RESPONSE_MSG_TYPE = object_utils.create_type_identifier(object_id=9002, version=1)
"""
message ApplicationIntegrationServiceResponseMsg{
    enum _MessageTypeIdentifier {
      _ID = 9002;
      _VERSION = 1;
    }
    
    // The message parameters object
    repeated net.ooici.core.link.CASRef message_parameters_reference = 1;
    
    optional string result = 2;
}
"""

AIS_RESPONSE_ERROR_TYPE = object_utils.create_type_identifier(object_id=9003, version=1)
"""
message ApplicationIntegrationServiceError{
    enum _MessageTypeIdentifier {
      _ID = 9003;
      _VERSION = 1;
    }
    
    optional string error_num = 1;
    
    optional string error_str = 2;
}
"""

# FindDataResources GPBs from ion-object-definitions/net/ooici/integration/ais/findDataResources/find_data_resources.proto
FIND_DATA_RESOURCES_MSG_TYPE = object_utils.create_type_identifier(object_id=9031, version=1)
"""
message FindDataResourcesMsg {
   enum _MessageTypeIdentifier {
       _ID = 9031;
       _VERSION = 1;	
   }

   required Spatial  spatial  = 1;
   required Temporal temporal = 2;
}
"""

# RegisterUser GPBs from ion-object-definitions/net/ooici/integration/ais/registerUser/register_user.proto
REGISTER_USER_TYPE = object_utils.create_type_identifier(object_id=9101, version=1)
"""
message RegisterIonUser {
   enum _MessageTypeIdentifier {
       _ID = 9101;
       _VERSION = 1;
   }
   // objects in a protofile are called messages
   optional string certificate=1;
   optional string rsa_private_key=2;
}
"""

UPDATE_USER_EMAIL_TYPE = object_utils.create_type_identifier(object_id=9102, version=1)
"""
message UpdateUserEmail {
   enum _MessageTypeIdentifier {
       _ID = 9102;
       _VERSION = 1;
   }
   // objects in a protofile are called messages
   optional string email_address=1;
}
"""

UPDATE_USER_DISPATCH_QUEUE_TYPE = object_utils.create_type_identifier(object_id=9103, version=1)
"""
message UpdateUserDispatcherQueue {
   enum _MessageTypeIdentifier {
       _ID = 9103;
       _VERSION = 1;
   }
   optional string queue_name=1;
}
"""

OOI_ID_TYPE = object_utils.create_type_identifier(object_id=9104, version=1)
"""
message OoiId {
   enum _MessageTypeIdentifier {
       _ID = 9104;
       _VERSION = 1;
   }

   optional string ooi_id=1;
}
"""

