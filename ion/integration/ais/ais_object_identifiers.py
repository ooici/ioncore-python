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
    
    optional int32 error_num = 1;
    
    optional string error_str = 2;
}
"""

# AisDataResourceSummaryMsg GPBs from ion-object-definitions/net/ooici/integration/ais/ais_data_resource_summary.proto
AIS_DATA_RESOURCE_SUMMARY_MSG_TYPE = object_utils.create_type_identifier(object_id=9021, version=1)
"""
message AisDataResourceSummaryMsg {
   enum _MessageTypeIdentifier {
       _ID = 9021;
       _VERSION = 1;	
   }

   optional string user_ooi_id      = 1;
   optional string data_resource_id = 2; 
   optional string title            = 3; 
   optional string institution      = 4;
   optional string source           = 5; 

}
"""

# FindDataResources GPBs from ion-object-definitions/net/ooici/integration/ais/findDataResources/find_data_resources.proto
FIND_DATA_RESOURCES_REQ_MSG_TYPE = object_utils.create_type_identifier(object_id=9031, version=1)
"""
message FindDataResourcesReqMsg {
   enum _MessageTypeIdentifier {
       _ID = 9031;
       _VERSION = 1;	
   }

   optional string user_ooi_id = 1;
   optional double minLatitude = 2;
   optional double maxLatitude = 3;
   optional double minLongitude = 4;
   optional double maxLongitude = 5;
   optional double minDepth = 6;
   optional double maxDepth = 7;
   optional double minTime = 8;
   optional double maxTime = 9;
   optional string identity = 10; // THIS FIELD IS TEMPORARY!!
   }
"""

# FindDataResources GPBs from ion-object-definitions/net/ooici/integration/ais/findDataResources/find_data_resources.proto
FIND_DATA_RESOURCES_RSP_MSG_TYPE = object_utils.create_type_identifier(object_id=9032, version=1)
"""
message FindDataResourcesRspMsg {
   enum _MessageTypeIdentifier {
       _ID = 9032;
       _VERSION = 1;	
   }

   repeated net.ooici.core.link.CASRef dataResourceSummary = 1;
}
"""

# GetDataResourceDetail GPBs from ion-object-definitions/net/ooici/integration/ais/getDataResourceDetail/get_data_resource_detail.proto
GET_DATA_RESOURCE_DETAIL_REQ_MSG_TYPE = object_utils.create_type_identifier(object_id=9033, version=1)
"""
message GetDataResourceDetailReqMsg {
   enum _MessageTypeIdentifier {
       _ID = 9033;
       _VERSION = 1;	
   }

   optional string data_resource_id  = 1;
}
"""

# GetDataResourceDetail GPBs from ion-object-definitions/net/ooici/integration/ais/findDataResources/get_data_resource_detail.proto
GET_DATA_RESOURCE_DETAIL_RSP_MSG_TYPE = object_utils.create_type_identifier(object_id=9034, version=1)
"""
message GetDataResourceDetailRspMsg {
   enum _MessageTypeIdentifier {
       _ID = 9034;
       _VERSION = 1;	
   }

   //repeated <put payload here>
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

SUBSCRIPTION_INFO_TYPE = object_utils.create_type_identifier(object_id=9201, version=1)
"""
message SubscriptionInfo {
   enum _MessageTypeIdentifier {
       _ID = 9201;
       _VERSION = 1;
   }

   optional string user_ooi_id=1;
   optional string queue_id=1;
}
"""

CREATE_DATA_RESOURCE_REQ_TYPE = object_utils.create_type_identifier(object_id=9211, version=1)
UPDATE_DATA_RESOURCE_REQ_TYPE = object_utils.create_type_identifier(object_id=9211, version=1)
"""
message DataResourceCreateUpdateRequest {
    enum _MessageTypeIdentifier {
        _ID = 9211;
        _VERSION = 1;
    }
    optional string data_resource_id       = 1;
    optional string provider               = 2;
    optional string format                 = 3;
    optional string protocol               = 4;
    optional string type                   = 5;
    optional string title                  = 6;
    optional string data_format            = 7;
    optional string data_type              = 8;
    optional string naming_authority       = 9;
    optional string summary                = 10;
    optional string creator_user_id        = 11;
    optional string publisher_id           = 12;

}
"""

CREATE_DATA_RESOURCE_RSP_TYPE = object_utils.create_type_identifier(object_id=9212, version=1)
UPDATE_DATA_RESOURCE_RSP_TYPE = object_utils.create_type_identifier(object_id=9212, version=1)
"""
message DataResourceCreateUpdateResponse {
    enum _MessageTypeIdentifier {
        _ID = 9212;
        _VERSION = 1;
    }

    optional string data_resource_id  = 1;
}
"""

DELETE_DATA_RESOURCE_REQ_TYPE = object_utils.create_type_identifier(object_id=9213, version=1)
"""
message DataResourceDeleteRequest {
    enum _MessageTypeIdentifier {
        _ID = 9213;
        _VERSION = 1;
    }

    optional string data_resource_id  = 1;
}
"""

DELETE_DATA_RESOURCE_RSP_TYPE = object_utils.create_type_identifier(object_id=9214, version=1)
"""
message DataResourceDeleteResponse {
    enum _MessageTypeIdentifier {
        _ID = 9214;
        _VERSION = 1;
    }

    optional bool success  = 1;
}
"""

