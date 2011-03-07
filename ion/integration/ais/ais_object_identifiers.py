#
# AIS ObjectIDs
#
from ion.core.object import object_utils

AIS_REQUEST_MSG_TYPE = object_utils.create_type_identifier(object_id=9001, version=1)
AIS_RESPONSE_MSG_TYPE = object_utils.create_type_identifier(object_id=9002, version=1)

# FindDataResources GPBs
FIND_DATA_RESOURCES_MSG_TYPE = object_utils.create_type_identifier(object_id=9131, version=1)
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
# RegisterUser GPBs
REGISTER_USER_TYPE = object_utils.create_type_identifier(object_id=9101, version=1)
"""
message RegisterUser {
   enum _MessageTypeIdentifier {
       _ID = 9101;
       _VERSION = 1;
   }
   // objects in a protofile are called messages
   optional string certificate=1;
   optional string rsa_private_key=2;
}
"""

UPDATE_USER_TYPE = object_utils.create_type_identifier(object_id=9102, version=1)
"""
message UpdateUser {
   enum _MessageTypeIdentifier {
       _ID = 9102;
       _VERSION = 1;
   }
   // objects in a protofile are called messages
   optional string certificate=1;
   optional string rsa_private_key=2;
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


