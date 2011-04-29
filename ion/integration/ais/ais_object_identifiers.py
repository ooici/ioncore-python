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

    optional int32 result = 2;
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

# AisDataResourceMetadata GPBs from ion-object-definitions/net/ooici/integration/ais/ais_data_resource_metadata.proto
AIS_DATASET_METADATA_TYPE = object_utils.create_type_identifier(object_id=9021, version=1)
"""
message AisDatasetMetadata {
   enum _MessageTypeIdentifier {
       _ID = 9021;
       _VERSION = 1;
   }

   optional string user_ooi_id      = 1;
   optional string data_resource_id = 2;
   optional string title            = 3;
   optional string institution      = 4;
   optional string source           = 5;
   optional string references       = 6;
   optional string conventions      = 7;
   optional string summary          = 8;
   optional string comment          = 9;
   optional string ion_time_coverage_start = 10;
   optional string ion_time_coverage_end   = 11;
   optional double ion_geospatial_lat_min  = 12;
   optional double ion_geospatial_lat_max  = 13;
   optional double ion_geospatial_lon_min  = 14;
   optional double ion_geospatial_lon_max  = 15;
   optional double ion_geospatial_vertical_min      = 16;
   optional double ion_geospatial_vertical_max      = 17;
   optional string ion_geospatial_vertical_positive = 18;

}
"""

AIS_DATASOURCE_METADATA_TYPE = object_utils.create_type_identifier(object_id=9022, version=1)
"""
message AisDatasourceMetadata {
   enum _MessageTypeIdentifier {
       _ID = 9022;
       _VERSION = 1;
   }

   optional net.ooici.services.sa.SourceType source_type = 1;
   repeated string property   = 2;
   repeated string station_id = 3;

   optional net.ooici.services.sa.RequestType request_type = 4;
   optional double top    = 5;
   optional double bottom = 6;
   optional double left   = 7;
   optional double right  = 8;
   optional string base_url    = 9;
   optional string dataset_url = 10;
   optional string ncml_mask   = 11;
   optional uint64 max_ingest_millis = 12;

   //'start_time' and 'end_time' are expected to be in the
   // ISO8601 Date Format (yyyy-MM-dd'T'HH:mm:ss'Z')
   optional string start_time = 13;
   optional string end_time   = 14;
   optional string institution_id = 15;

}
"""

# AisDataVariable GPBs from ion-object-definitions/net/ooici/integration/ais/ais_data_variable.proto
AIS_DATA_VARIABLE_TYPE = object_utils.create_type_identifier(object_id=9023, version=1)
"""
message AisDataVariableType {
   enum _MessageTypeIdentifier {
       _ID = 9023;
       _VERSION = 1;
   }

   optional string units         = 1;
   optional string standard_name = 2;
   optional string long_name     = 3;
   repeated string other_attributes = 4;

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

FIND_DATA_RESOURCES_RSP_MSG_TYPE = object_utils.create_type_identifier(object_id=9032, version=1)
"""
message FindDataResourcesRspMsg {
   enum _MessageTypeIdentifier {
       _ID = 9032;
       _VERSION = 1;
   }

   repeated net.ooici.integration.ais.aisDataResourceMetadata.AisDatasetMetadata dataResourceSummary = 1;
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

GET_DATA_RESOURCE_DETAIL_RSP_MSG_TYPE = object_utils.create_type_identifier(object_id=9034, version=1)
"""
message GetDataResourceDetailRspMsg {
   enum _MessageTypeIdentifier {
       _ID = 9034;
       _VERSION = 1;
   }
   optional string data_resource_id = 1;
   optional net.ooici.integration.ais.common.aisDataResourceMetadata.AisDatasetMetadataType dataResourceSummary = 2;
   optional net.ooici.integration.ais.common.aisDataResourceMetadata.AisDatasourceMetadataType source = 3;
   repeated net.ooici.integration.ais.common.aisDataVariable.AisDataVariableType variable = 4;
}

"""

# CreateDownloadURL GPB from ion-object-definitions/net/ooici/integration/ais/createDownloadURL/create_download_url.proto
CREATE_DOWNLOAD_URL_REQ_MSG_TYPE = object_utils.create_type_identifier(object_id=9035, version=1)
"""
message CreateDownloadURLReqMsg {
   enum _MessageTypeIdentifier {
       _ID = 9035;
       _VERSION = 1;
   }

   optional string user_ooi_id  = 1;

}
"""

CREATE_DOWNLOAD_URL_RSP_MSG_TYPE = object_utils.create_type_identifier(object_id=9036, version=1)
"""
message CreateDownloadURLRspMsg {
   enum _MessageTypeIdentifier {
       _ID = 9036;
       _VERSION = 1;
   }

   optional string download_url = 1;

}
"""

# NameValuePairType GPB from ion-object-definitions/net/ooici/integration/ais/common/name_value_pair_type.proto
NAME_VALUE_PAIR_TYPE = object_utils.create_type_identifier(object_id=9125, version=1)
"""
message NameValuePairType {
   enum _MessageTypeIdentifier {
       _ID = 9125;
       _VERSION = 1;
   }

   optional string name = 1;
   optional string value = 2;
}
"""

# RegisterUser GPBs from ion-object-definitions/net/ooici/integration/ais/registerUser/register_user.proto
REGISTER_USER_REQUEST_TYPE = object_utils.create_type_identifier(object_id=9101, version=1)
"""
message RegisterIonUserRequest {
   enum _MessageTypeIdentifier {
       _ID = 9101;
       _VERSION = 1;
   }
   // objects in a protofile are called messages
   optional string certificate=1;
   optional string rsa_private_key=2;
}
"""

UPDATE_USER_PROFILE_REQUEST_TYPE = object_utils.create_type_identifier(object_id=9102, version=1)
"""
message UpdateUserProfileRequest {
   enum _MessageTypeIdentifier {
       _ID = 9102;
       _VERSION = 1;
   }
   // objects in a protofile are called messages
   optional string user_ooi_id=1;
   optional string email_address=2;
   repeated net.ooici.integration.ais.common.aisNameValuePairType.NameValuePairType profile=3;
}
"""

REGISTER_USER_RESPONSE_TYPE = object_utils.create_type_identifier(object_id=9103, version=1)
"""
message RegisterIonUserResponse {
   enum _MessageTypeIdentifier {
       _ID = 9103;
       _VERSION = 1;
   }

   optional string ooi_id=1;
   optional bool user_is_admin=2;
   optional bool user_already_registered=3;
   optional bool user_is_early_adopter=4;
   optional bool user_is_data_provider=5;
   optional bool user_is_marine_operator=6;
}
"""

GET_USER_PROFILE_REQUEST_TYPE = object_utils.create_type_identifier(object_id=9104, version=1)
"""
message GetUserProfileRequest {
   enum _MessageTypeIdentifier {
       _ID = 9104;
       _VERSION = 1;
   }

   optional string user_ooi_id=1;
}
"""

GET_USER_PROFILE_RESPONSE_TYPE = object_utils.create_type_identifier(object_id=9105, version=1)
"""
message GetUserProfileResponse {
   enum _MessageTypeIdentifier {
       _ID = 9105;
       _VERSION = 1;
   }

   optional string email_address=1;
   repeated net.ooici.integration.ais.common.aisNameValuePairType.NameValuePairType profile=2;
}
"""

# ManageResources GPBs from ion-object-definitions/net/ooici/integration/ais/manageResources/manage_resources.proto
GET_RESOURCE_TYPES_RESPONSE_TYPE = object_utils.create_type_identifier(object_id=9120, version=1)
"""
message GetResourceTypesResponse {
   enum _MessageTypeIdentifier {
       _ID = 9120;
       _VERSION = 1;
   }

   repeated net.ooici.core.link.CASRef resource_types_list = 1;
}
"""

GET_RESOURCES_OF_TYPE_REQUEST_TYPE = object_utils.create_type_identifier(object_id=9121, version=1)
"""
message GetResourcesOfTypeRequest {
   enum _MessageTypeIdentifier {
       _ID = 9121;
       _VERSION = 1;
   }

   optional string resource_type = 1;
}
"""

RESOURCE_TYPE = object_utils.create_type_identifier(object_id=9122, version=1)
"""
message Resource {
   enum _MessageTypeIdentifier {
       _ID = 9122;
       _VERSION = 1;
   }

   repeated string attribute = 1;
}
"""

GET_RESOURCES_OF_TYPE_RESPONSE_TYPE = object_utils.create_type_identifier(object_id=9123, version=1)
"""
message GetResourcesOfTypeResponse {
   enum _MessageTypeIdentifier {
       _ID = 9123;
       _VERSION = 1;
   }

   repeated string column_names = 1;
   repeated net.ooici.integration.ais.manageResources.Resource resources = 2;
}
"""

GET_RESOURCE_REQUEST_TYPE = object_utils.create_type_identifier(object_id=9124, version=1)
"""
message GetResourceRequest {
   enum _MessageTypeIdentifier {
       _ID = 9124;
       _VERSION = 1;
   }

   optional string ooi_id = 1;
}
"""

GET_RESOURCE_RESPONSE_TYPE = object_utils.create_type_identifier(object_id=9126, version=1)
"""

message GetResourceResponse {
   enum _MessageTypeIdentifier {
       _ID = 9126;
       _VERSION = 1;
   }

   repeated net.ooici.integration.ais.common.aisNameValuePairType.NameValuePairType resource = 1;
}
"""

SUBSCRIPTION_INFO_TYPE = object_utils.create_type_identifier(object_id=9201, version=1)
"""
message SubscriptionInfoType {
    enum _MessageTypeIdentifier {
      _ID = 9201;
      _VERSION = 1;
    }

    // The message parameters object
    optional string user_ooi_id = 1;
    optional string data_src_id = 2;
    enum SubscriptionType {
         EMAIL = 0;
         DISPATCHER = 1;
         EMAILANDDISPATCHER = 2;
     }
     optional SubscriptionType subscription_type = 3 [default = EMAIL];
     enum AlertsFilter {
          UPDATES = 0;
          DATASOURCEOFFLINE = 1;
          UPDATESANDDATASOURCEOFFLINE = 2;
     }
     optional AlertsFilter email_alerts_filter = 4;
     optional AlertsFilter dispatcher_alerts_filter = 5;
     optional string dispatcher_script_path = 6;
}
"""

CREATE_DATA_RESOURCE_REQ_TYPE = object_utils.create_type_identifier(object_id=9211, version=1)
"""
message DataResourceCreateRequest {
    enum _MessageTypeIdentifier {
        _ID = 9211;
        _VERSION = 1;
    }

    optional SourceType source_type       = 1;

    // ISO8601 Date Format (yyyy-MM-dd'T'HH:mm:ss'Z'
    optional string start_time            = 2;
    optional string end_time              = 3;
    optional uint64 update_interval_msec  = 4;
    repeated string property              = 5;
    repeated string station_id            = 6;

    optional RequestType request_type     = 7;
    optional double top                   = 8;
    optional double bottom                = 9;
    optional double left                  = 10;
    optional double right                 = 11;
    optional string base_url              = 12;
    optional string dataset_url           = 13;
    optional string ncml_mask             = 14;
    optional string institution_id        = 15;

}
"""

CREATE_DATA_RESOURCE_RSP_TYPE = object_utils.create_type_identifier(object_id=9212, version=1)
"""
message DataResourceCreateResponse {
    enum _MessageTypeIdentifier {
        _ID = 9212;
        _VERSION = 1;
    }

    optional string data_source_id  = 1;
    optional string data_set_id     = 2;
    optional string association_id  = 3;
}
"""


DELETE_DATA_RESOURCE_REQ_TYPE = object_utils.create_type_identifier(object_id=9213, version=1)
"""
message DataResourceDeleteRequest {
    enum _MessageTypeIdentifier {
        _ID = 9213;
        _VERSION = 1;
    }

    repeated string data_resource_id  = 1;
}
"""

DELETE_DATA_RESOURCE_RSP_TYPE = object_utils.create_type_identifier(object_id=9214, version=1)
"""
message DataResourceDeleteResponse {
    enum _MessageTypeIdentifier {
        _ID = 9214;
        _VERSION = 1;
    }

    repeated string successfully_deleted_id  = 1;
}
"""



UPDATE_DATA_RESOURCE_REQ_TYPE = object_utils.create_type_identifier(object_id=9215, version=1)
"""
message DataResourceUpdateRequest {
    enum _MessageTypeIdentifier {
        _ID = 9215;
        _VERSION = 1;
    }

    optional string user_id               = 1;
    optional uint64 update_interval_msec  = 2;
    optional string institution_id        = 3;
}
"""


UPDATE_DATA_RESOURCE_RSP_TYPE = object_utils.create_type_identifier(object_id=9216, version=1)
"""
message DataResourceCreateUpdateResponse {
    enum _MessageTypeIdentifier {
        _ID = 9216;
        _VERSION = 1;
    }

    optional bool success = 1;
}
"""


SUBSCRIBE_DATA_RESOURCE_REQ_TYPE = object_utils.create_type_identifier(object_id=9203, version=1)
"""
message SubscriptionCreateReqMsg {
    enum _MessageTypeIdentifier {
      _ID = 9203;
      _VERSION = 1;
    }
    
    optional net.ooici.integration.ais.common.aisSubscriptionInfo.SubscriptionInfoType subscriptionInfo = 1;
}
"""

SUBSCRIBE_DATA_RESOURCE_RSP_TYPE = object_utils.create_type_identifier(object_id=9204, version=1)
"""
message SubscriptionCreateRspMsg {
    enum _MessageTypeIdentifier {
      _ID = 9204;
      _VERSION = 1;
    }

    optional bool success = 1;
}
"""

DELETE_SUBSCRIPTION_REQ_TYPE  = object_utils.create_type_identifier(object_id=9205, version=1)
"""
message SubscriptionDeleteReqMsg {
    enum _MessageTypeIdentifier {
      _ID = 9205;
      _VERSION = 1;
    }

    optional net.ooici.integration.ais.common.aisSubscriptionInfo.SubscriptionInfoType subscriptionInfo = 1;
}
"""

DELETE_SUBSCRIPTION_RSP_TYPE  = object_utils.create_type_identifier(object_id=9206, version=1)
"""
message SubscriptionDeleteRspMsg {
    enum _MessageTypeIdentifier {
      _ID = 9206;
      _VERSION = 1;
    }

    optional bool success = 1;
}
"""


GET_SUBSCRIPTION_LIST_REQ_TYPE = object_utils.create_type_identifier(object_id=9207, version=1)
"""
message SubscriptionInfoListReqMsg {
    enum _MessageTypeIdentifier {
      _ID = 9207;
      _VERSION = 1;
    }

    optional string user_ooi_id = 1;

}
"""

GET_SUBSCRIPTION_LIST_RESP_TYPE = object_utils.create_type_identifier(object_id=9208, version=1)
"""
message SubscriptionInfoListRspMsg {
    enum _MessageTypeIdentifier {
      _ID = 9208;
      _VERSION = 1;
    }

    repeated SubscriptionInfoReqMsg subscription = 1;
}
"""

UPDATE_SUBSCRIPTION_REQ_TYPE  = object_utils.create_type_identifier(object_id=9209, version=1)
"""
message SubscriptionUpdateReqMsg {
    enum _MessageTypeIdentifier {
      _ID = 9209;
      _VERSION = 1;
    }

    optional net.ooici.integration.ais.common.aisSubscriptionInfo.SubscriptionInfoType subscriptionInfo = 1;
}
"""

UPDATE_SUBSCRIPTION_RSP_TYPE  = object_utils.create_type_identifier(object_id=9210, version=1)
"""
message SubscriptionUpdateRspMsg {
    enum _MessageTypeIdentifier {
      _ID = 9210;
      _VERSION = 1;
    }

    optional bool success = 1;
}
"""

FIND_DATA_SUBSCRIPTIONS_REQ_TYPE = object_utils.create_type_identifier(object_id=9218, version=1)
"""
message SubscriptionFindReqMsg {
    enum _MessageTypeIdentifier {
      _ID = 9217;
      _VERSION = 1;
    }

    optional string user_ooi_id = 1;
    optional net.ooici.integration.ais.common.aisDataBounds.AisDataBoundsType dataBounds = 2;
}
"""

FIND_DATA_SUBSCRIPTIONS_RSP_TYPE = object_utils.create_type_identifier(object_id=9219, version=1)
"""
message SubscriptionFindRspMsg {
    enum _MessageTypeIdentifier {
      _ID = 9218;
      _VERSION = 1;
    }

    repeated SubscriptionFindRspPayloadType data = 1;
    //repeated net.ooici.integration.ais.common.aisSubscriptionInfo.SubscriptionInfoType subscriptionInfo = 1;
}
"""

