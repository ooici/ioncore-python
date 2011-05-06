#!/usr/bin/env python

"""
@file ion/services/coi/datastore_bootstrap/ion_preload_config.py
@author David Stuebe

@Brief This module contains defined constants and configuration dictionaries which are used in the data store and other
services to provide and access core data schema objects and resources. These can be extended for testing purposes.

To add a new entry in an existing list please use ion.util.procutils.create_guid() to generate a new ID_CFG for each
new entry. Then follow the pattern to create a resource which will be filled in by the CONTENT_CFG dictionary.

If you have a more complex, nested resource, you must create a function to generate that resource. Follow the example
in the ION_DATASETS section...

'ion.services.coi.datastore_bootstrap.ion_preload_config':{
    # Path to files relative to ioncore-python directory!
    # Get files from:  http://ooici.net/ion_data/
    'sample_traj_dataset' : '../../ion_data/SOS_Test.tar.gz',
    'sample_station_dataset' : '../../ion_data/USGS_Test.tar.gz'
},


"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.object.object_utils import create_type_identifier

from ion.services.coi.datastore_bootstrap import dataset_bootstrap

from ion.core import ioninit
CONF = ioninit.config(__name__)

### Constants used in defining the basic configuration which is preloaded into the datastore:
ID_CFG = 'id'
TYPE_CFG = 'type'
NAME_CFG = 'name'
PREDICATE_CFG = 'predicate'
DESCRIPTION_CFG = 'description'
CONTENT_CFG = 'content'
CONTENT_ARGS_CFG = 'content_args'
PRELOAD_CFG = 'preload'
OWNER_ID = 'owner'

# Set some constants used system wide!:

# These name must also be changed in the datastore Zapp!

### THESE ARE REQUIRED OBJECTS
ION_PREDICATES_CFG = 'ion_predicates'
ION_RESOURCE_TYPES_CFG = 'ion_resource_types'
ION_IDENTITIES_CFG = 'ion_identities'

### THESE ARE FOR TESTING AND DEVELOPMENT
ION_DATASETS_CFG = 'ion_datasets'
ION_AIS_RESOURCES_CFG = 'ion_ais_resources'


### Defined Resource Types
topic_res_type_name = 'topic_resource_type'
dataset_res_type_name = 'dataset_resource_type'
identity_res_type_name = 'identity_resource_type'
datasource_res_type_name = 'datasource_resource_type'
dispatcher_res_type_name = 'dispatcher_resource_type'
resource_type_type_name = 'resource_type_type'
default_resource_type_name = 'default_resource_type'
exchange_space_rtn = 'exchange_space_resource_type'
exchange_point_rtn = 'exchange_point_resource_type'
publisher_rtn = 'publisher_resource_type'
subscriber_rtn = 'subscriber_type'
queue_rtn = 'queue_type'
subscription_res_type_name = 'subscription_resource_type'
instrument_res_type_name = 'instrument_resource_type'
instrument_agent_res_type_name = 'instrument_agent_resource_type'
dispatcher_res_type_name = 'dispatcher_resource_type'
dispatcher_workflow_res_type_name = 'dispatcher_workflow_resource_type'

datasource_schedule_rtn = 'dataresource_schedule_type'

resource_type_type = create_type_identifier(object_id=1103, version=1)
# Data structure used by datastore intialization
ION_RESOURCE_TYPES={
resource_type_type_name:{ID_CFG:'173A3188-E290-42BE-8776-8717077DD207',
                     TYPE_CFG:resource_type_type,
                     NAME_CFG:resource_type_type_name,
                     DESCRIPTION_CFG:'The resource type is meta description of a class of resource',
                     CONTENT_CFG:{'object_identifier':1103,
                                  'object_version':1,
                                  'meta_description':'protomessage?'}
                     },


topic_res_type_name:{ID_CFG:'3BD84B48-073E-4833-A62B-0DE4EC106A34',
                     TYPE_CFG:resource_type_type,
                     NAME_CFG:topic_res_type_name,
                     DESCRIPTION_CFG:'A topic resource is used by the pubsub controller service to represent a topic on which messages can be sent',
                     CONTENT_CFG:{'object_identifier':2317,
                                  'object_version':1,
                                  'meta_description':'protomessage?'}
                     },

exchange_space_rtn:{ID_CFG:'5bf51324-0bd8-43a6-9551-4dbaf6ccd1a2',
                    TYPE_CFG:resource_type_type,
                    NAME_CFG:exchange_space_rtn,
                    DESCRIPTION_CFG:'An exchange space resource, pubsub controller',
                    CONTENT_CFG: {
                        'object_identifier':2315,
                        'object_version':1,
                        'meta_description':'protomessage?'
                    }},

exchange_point_rtn:{ID_CFG:'c092163e-995b-40ef-9ff2-d49c1dccf8c5',
                    TYPE_CFG:resource_type_type,
                    NAME_CFG:exchange_point_rtn,
                    DESCRIPTION_CFG:'An exchange point resource, pubsub controller',
                    CONTENT_CFG: {
                        'object_identifier':2316,
                        'object_version':1,
                        'meta_description':'protomessage?'
                    }},
publisher_rtn:{ID_CFG:'d4c17990-a7d0-47a7-911a-138ee7bfb112',
                    TYPE_CFG:resource_type_type,
                    NAME_CFG:publisher_rtn,
                    DESCRIPTION_CFG:'A publisher resource, pubsub controller',
                    CONTENT_CFG: {
                        'object_identifier':2318,
                        'object_version':1,
                        'meta_description':'protomessage?'
                    }},

subscriber_rtn:{ID_CFG:'bdf80fd1-8088-4860-87e5-b04676320edc',
                    TYPE_CFG:resource_type_type,
                    NAME_CFG:exchange_space_rtn,
                    DESCRIPTION_CFG:'A subscriber resource, pubsub controller',
                    CONTENT_CFG: {
                        'object_identifier':2319,
                        'object_version':1,
                        'meta_description':'protomessage?'
                    }},

dataset_res_type_name:{ID_CFG:'487594C6-3D10-4DAA-A8FF-83E1E0EFB964',
                       TYPE_CFG:resource_type_type,
                       NAME_CFG:dataset_res_type_name,
                       DESCRIPTION_CFG:'A dataset resource contains science data using the CDM data model',
                       CONTENT_CFG:{'object_identifier':10001,
                                    'object_version':1,
                                  'meta_description':'protomessage?'}
                    },

identity_res_type_name:{ID_CFG:'9C457C32-5982-4044-A3ED-6DBDB5E3EB5C',
                       TYPE_CFG:resource_type_type,
                       NAME_CFG:identity_res_type_name,
                       DESCRIPTION_CFG:'An identity resource contains login information for a user',
                       CONTENT_CFG:{'object_identifier':1401,
                                    'object_version':1,
                                    'meta_description':'protomessage?'}
                        },
datasource_res_type_name:{ID_CFG:'B8B7BB73-F578-4604-B3B3-088D28F9A7DC',
                       TYPE_CFG:resource_type_type,
                       NAME_CFG:datasource_res_type_name,
                       DESCRIPTION_CFG:'A data source resource contains information about an source of data - metadata about the input to a dataset',
                       CONTENT_CFG:{'object_identifier':4503,
                                    'object_version':1,
                                    'meta_description':'protomessage?'}
                        },

queue_rtn : {ID_CFG: 'EEE94F63-CD27-4F7B-9DAA-FD8782B66AE1',
             TYPE_CFG: resource_type_type,
             NAME_CFG:queue_rtn,
             DESCRIPTION_CFG:'A resource for queues inside the PSC',
             CONTENT_CFG:{'object_identifier':2321,
                          'object_version':1,
                          'meta_description':'protomessage?'}
            },

subscription_res_type_name:{ID_CFG:'94989414-3BD1-4688-ADC7-B942F04E2997',
                       TYPE_CFG:resource_type_type,
                       NAME_CFG:subscription_res_type_name,
                       DESCRIPTION_CFG:'A subscription resource links a user to a data source',
                       CONTENT_CFG:{'object_identifier':9201,
                                    'object_version':1,
                                  'meta_description':'protomessage?'}
                    },

instrument_res_type_name:{ID_CFG:'403D63E5-8B22-4766-9B19-54AC26639C27',
                       TYPE_CFG:resource_type_type,
                       NAME_CFG:instrument_res_type_name,
                       DESCRIPTION_CFG:'A instrument resource represents an instance of scientific equipment',
                       CONTENT_CFG:{'object_identifier':4301,
                                    'object_version':1,
                                  'meta_description':'protomessage?'}
                    },

instrument_agent_res_type_name:{ID_CFG:'4D8487E5-5937-4B1E-BFA5-39113C9A323C',
                       TYPE_CFG:resource_type_type,
                       NAME_CFG:instrument_agent_res_type_name,
                       DESCRIPTION_CFG:'A instrument agent resource represents a controller for an instance of scientific equipment',
                       CONTENT_CFG:{'object_identifier':4302,
                                    'object_version':1,
                                  'meta_description':'protomessage?'}
                    },

dispatcher_res_type_name:{ID_CFG:'2E92128B-3EB5-4D07-A3DC-E2AD64504835',
                       TYPE_CFG:resource_type_type,
                       NAME_CFG:dispatcher_res_type_name,
                       DESCRIPTION_CFG:'A dispatcher represents local software component',
                       CONTENT_CFG:{'object_identifier':7002,
                                    'object_version':1,
                                  'meta_description':'protomessage?'}
                    },

dispatcher_workflow_res_type_name:{ID_CFG:'B2296B26-75F0-4E70-BD01-CB8887BCD714',
                       TYPE_CFG:resource_type_type,
                       NAME_CFG:dispatcher_workflow_res_type_name,
                       DESCRIPTION_CFG:'A dispatcher workflow represents a script for the dispatcher resource',
                       CONTENT_CFG:{'object_identifier':7003,
                                    'object_version':1,
                                  'meta_description':'protomessage?'}
                    },


datasource_schedule_rtn:{ID_CFG:'3E49B5EF-2D60-4DE1-B554-F30BBF1AD508',
                     TYPE_CFG:resource_type_type,
                     NAME_CFG:datasource_schedule_rtn,
                     DESCRIPTION_CFG:'A resource to hold the schedule task id associated with a datasource',
                     CONTENT_CFG:{'object_identifier':9217,
                                  'object_version':1,
                                  'meta_description':'protomessage?'}
                     },



#=======================================
#========= DEFAULT TYPE ================
#=======================================

default_resource_type_name:{ID_CFG:'422ADE3C-D820-437F-8BD3-7D8793591EB0',
                     TYPE_CFG:resource_type_type,
                     NAME_CFG:default_resource_type_name,
                     DESCRIPTION_CFG:'A type to catch unregistered types!',
                     CONTENT_CFG:{'object_identifier':-1,
                                  'object_version':-1,
                                  'meta_description':'protomessage?'}
                     },



}

# Extract Resource ID_CFGs for use in services and tests
TOPIC_RESOURCE_TYPE_ID         = ION_RESOURCE_TYPES[topic_res_type_name][ID_CFG]
EXCHANGE_SPACE_RES_TYPE_ID     = ION_RESOURCE_TYPES[exchange_space_rtn][ID_CFG]
EXCHANGE_POINT_RES_TYPE_ID     = ION_RESOURCE_TYPES[exchange_point_rtn][ID_CFG]
PUBLISHER_RES_TYPE_ID          = ION_RESOURCE_TYPES[publisher_rtn][ID_CFG]
SUBSCRIBER_RES_TYPE_ID         = ION_RESOURCE_TYPES[subscriber_rtn][ID_CFG]
QUEUE_RES_TYPE_ID              = ION_RESOURCE_TYPES[queue_rtn][ID_CFG]
SUBSCRIPTION_RES_TYPE_ID       = ION_RESOURCE_TYPES[subscription_res_type_name][ID_CFG]
INSTRUMENT_RES_TYPE_ID         = ION_RESOURCE_TYPES[instrument_res_type_name][ID_CFG]
INSTRUMENT_AGENT_RES_TYPE_ID   = ION_RESOURCE_TYPES[instrument_agent_res_type_name][ID_CFG]
DATASET_RESOURCE_TYPE_ID       = ION_RESOURCE_TYPES[dataset_res_type_name][ID_CFG]
IDENTITY_RESOURCE_TYPE_ID      = ION_RESOURCE_TYPES[identity_res_type_name][ID_CFG]
DATASOURCE_RESOURCE_TYPE_ID    = ION_RESOURCE_TYPES[datasource_res_type_name][ID_CFG]
DISPATCHER_RESOURCE_TYPE_ID    = ION_RESOURCE_TYPES[dispatcher_res_type_name][ID_CFG]
RESOURCE_TYPE_TYPE_ID          = ION_RESOURCE_TYPES[resource_type_type_name][ID_CFG]

DATARESOURCE_SCHEDULE_TYPE_ID  = ION_RESOURCE_TYPES[datasource_schedule_rtn][ID_CFG]

DEFAULT_RESOURCE_TYPE_ID       = ION_RESOURCE_TYPES[default_resource_type_name][ID_CFG]

##### Define Predicates #####:

# Predicate names:
has_a_name = 'has_a'
is_a_name = 'is_a'
type_of_name = 'type_of'
owned_by_name = 'owned_by'
has_life_cycle_state_name = 'has_life_cycle_state'

TERMINOLOGY_TYPE = create_type_identifier(object_id=14, version=1)

# Data structure used by datastore intialization
ION_PREDICATES={
has_a_name:{ID_CFG:'C22A454D-389E-4BA6-88BC-CEDD93B5C87E',
            TYPE_CFG:TERMINOLOGY_TYPE,
            PREDICATE_CFG:has_a_name},

is_a_name:{ID_CFG:'60029609-FD0C-4DE3-8E52-9F5DDAD9A9A8',
            TYPE_CFG:TERMINOLOGY_TYPE,
            PREDICATE_CFG:is_a_name},

type_of_name:{ID_CFG:'F30A45F8-331D-4D44-AECC-746DA81B012F',
            TYPE_CFG:TERMINOLOGY_TYPE,
            PREDICATE_CFG:type_of_name},

owned_by_name:{ID_CFG:'734CE3E6-90ED-4642-AD46-7C2E67BDA798',
            TYPE_CFG:TERMINOLOGY_TYPE,
            PREDICATE_CFG:owned_by_name},

has_life_cycle_state_name:{ID_CFG:'ffe5c79e-58b5-493b-b409-0280c86ba0c7',
            TYPE_CFG:TERMINOLOGY_TYPE,
            PREDICATE_CFG:has_life_cycle_state_name},

}


# Extract Resource ID_CFGs for use in services and tests
HAS_A_ID = ION_PREDICATES[has_a_name][ID_CFG]
IS_A_ID = ION_PREDICATES[is_a_name][ID_CFG]
TYPE_OF_ID = ION_PREDICATES[type_of_name][ID_CFG]
OWNED_BY_ID = ION_PREDICATES[owned_by_name][ID_CFG]

HAS_LIFE_CYCLE_STATE_ID = ION_PREDICATES[has_life_cycle_state_name][ID_CFG]


##### Define Identities #####:

# Dataset names
anonymous_name = 'ANONYMOUS'
root_name = 'ROOT'
myooici_name = 'myooici'

identity_type = create_type_identifier(object_id=1401, version=1)
ION_IDENTITIES = {
anonymous_name:{ID_CFG:'A3D5D4A0-7265-4EF2-B0AD-3CE2DC7252D8',
                          TYPE_CFG:identity_type,
                          NAME_CFG:anonymous_name,
                          DESCRIPTION_CFG:'The anonymous user is the identity used by any unregistered user.',
                          CONTENT_CFG:{'subject':'ss',
                                       'certificate':'',
                                       'rsa_private_key':'',
                                       'name':'',
                                       'institution':'',
                                       'email':'',
                                       'authenticating_organization':''}
                        },

myooici_name:{ID_CFG:'A7B44115-34BC-4553-B51E-1D87617F12E0',
                          TYPE_CFG:identity_type,
                          NAME_CFG:myooici_name,
                          DESCRIPTION_CFG:'The first test user - poor sole!.',
                          CONTENT_CFG:{'subject':'/DC=org/DC=cilogon/C=US/O=Google/CN=test user A501',
                                       'certificate':
'''MIIEUzCCAzugAwIBAgICBgIwDQYJKoZIhvcNAQELBQAwazETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRww
GgYDVQQDExNDSUxvZ29uIE9wZW5JRCBDQSAxMB4XDTExMDQyMTE5MzMyMVoXDTExMDQyMjA3Mzgy
MVowZTETMBEGCgmSJomT8ixkARkTA29yZzEXMBUGCgmSJomT8ixkARkTB2NpbG9nb24xCzAJBgNV
BAYTAlVTMQ8wDQYDVQQKEwZHb29nbGUxFzAVBgNVBAMTDnRlc3QgdXNlciBBNTAxMIIBIjANBgkq
hkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAu+SQwAWMAY/+6eZjcirp0YfhKdgM06uZmTU9DPJqcNXF
ROFCeGEkg2jzgfcK5NiT662YbQkxETWDl4XZazmbPv787XJjYnbF8XErztauE3+caWNOpob2yPDt
mk3F0I0ullSbqsxPvsYAZNEveDBFzxCeeO+GKFQnw12ZYo968RcyZW2Fep9OQ4VfpWQExSA37FA+
4KL0RfZnd8Vc1ru9tFPw86hEstzC0Lt5HuXUHhuR9xsW3E5xY7mggHOrZWMQFiUN8WPnrHSCarwI
PQDKv8pMQ2LIacU8QYzVow74WUjs7hMd3naQ2+QgRd7eRc3fRYXPPNCYlomtnt4OcXcQSwIDAQAB
o4IBBTCCAQEwDAYDVR0TAQH/BAIwADAOBgNVHQ8BAf8EBAMCBLAwEwYDVR0lBAwwCgYIKwYBBQUH
AwIwGAYDVR0gBBEwDzANBgsrBgEEAYKRNgEDAzBsBgNVHR8EZTBjMC+gLaArhilodHRwOi8vY3Js
LmNpbG9nb24ub3JnL2NpbG9nb24tb3BlbmlkLmNybDAwoC6gLIYqaHR0cDovL2NybC5kb2Vncmlk
cy5vcmcvY2lsb2dvbi1vcGVuaWQuY3JsMEQGA1UdEQQ9MDuBEW15b29pY2lAZ21haWwuY29thiZ1
cm46cHVibGljaWQ6SUROK2NpbG9nb24ub3JnK3VzZXIrQTUwMTANBgkqhkiG9w0BAQsFAAOCAQEA
Omon3wMV3RFzs28iqs+r1j9WxLSvQXRXtk3BMNNmrobDspb2rodiNGMeVxGD2oGSAfh1Mn/l+vDE
1333XzQ3BGkucaSSBOTll5ZBqf52w/ru/dyrJ2GvHbIrKv+QkpKuP9uB0eJYi1n7+q/23rBR5V+E
+LsnTG8BcuzpFxtlY4SKIsijHNV+5y2+hfGHiNGfAr3X8FfwjIfmqBroCRc01ix8+jMnvplLr5rp
Wkkk8zr1nuzaUjNA/8G+24UBNSgLYOUP/xH2GlPUiAP4tZX+zGsOVkYkbyc67M4TLyD3hxuLbDCU
Aw3E0TjYpPxuQ8OsJ1LdECRfHgHFfd5KtG8BgQ==''',
                                       'rsa_private_key':
'''MIIEowIBAAKCAQEAu+SQwAWMAY/+6eZjcirp0YfhKdgM06uZmTU9DPJqcNXFROFCeGEkg2jzgfcK
5NiT662YbQkxETWDl4XZazmbPv787XJjYnbF8XErztauE3+caWNOpob2yPDtmk3F0I0ullSbqsxP
vsYAZNEveDBFzxCeeO+GKFQnw12ZYo968RcyZW2Fep9OQ4VfpWQExSA37FA+4KL0RfZnd8Vc1ru9
tFPw86hEstzC0Lt5HuXUHhuR9xsW3E5xY7mggHOrZWMQFiUN8WPnrHSCarwIPQDKv8pMQ2LIacU8
QYzVow74WUjs7hMd3naQ2+QgRd7eRc3fRYXPPNCYlomtnt4OcXcQSwIDAQABAoIBAE7JjC0I5mlt
US4RbpfcCMnU2YTrVI2ZwkGtQllgeWOxMBQvBOlniqET7DAOQGIvsu87jtQB67JUp0ZtWPsOX9vt
nm+O7L/IID6a/wyvlrUUaKkEfGF17Jvb8zYl8JH/8Y4WEmRvYe0UJ+wej3Itg8hNJrZ9cdsNVtMk
N4JNufbH0+s2t+nZPm7jLNbXfdP6CIiyTB6OIB9M3JRKed5lpFOOsTB0HNgBFGaZvmmzWpGQJ6wQ
YsEWbMiFrB4e8qutfF+itzq5cyMrMVsAJiecMfc/j1gv+77wSi3x6tqYWgLsk5jZBNm99UM/nxWp
Xl+091gN7aha9DQ1WmCpG+D6h4kCgYEA7AuKIn/m4riQ7PsuGKNIU/h8flsO+op5FUP0NBRBY8Mc
LTon/QBcZTqpkWYblkz/ME8AEuPWKsPZQrCO9sCFRBMk0L5IZQ43kr2leB43iHDhc+OsjDB0sV8M
oEWCI4BFu7wrtbmYTqJhQaHBh0lu3jWmKnaMkWIXsF2nvqDt7VcCgYEAy8brqFssASiDFJsZB1kK
AzVkM0f43/+51fzdPW6YnrxOMt3nQqzUOF1FlmvMog/fRPjcfcttdjVu12s9DljB0AaMoBRxmKcj
/mIvxPNrTBhAHeqowZ0XyCtgEl8c+8sZUi1hUmnCIDFvi9LKXbX/mnXp0aKqWD03Hnbm/o3vaC0C
gYEAmrcFl49V+o0XEP2iPSvpIIDiuL9elgFlU/byfaA5K/aa5VoVE9PEu+Uzd8YBlwZozXU6iycj
HWy5XujzC/EsaG5T1y6hrPsgmeIMLys/IwM6Awfb9RddpVSzpelpX3OYQXEZBUfc+M2eCbLIcrBD
JwrrGzIQ+Mne1Q7OADjjOokCgYABgHbOJ9XcMFM+/KGjlzlmqqcRZa9k3zqcZB+xSzZevR6Ka24/
5Iwv2iggIq1AaIOJu5fMaYpl+6DUf5rUlzzebp3stBneOSUfw9N8TRr2VZtrXQZfXuwE8qTjncXV
6TpHi8QS2mqu2A5tZmFNbYDzv3i4rc05l0HnvJKZP6yLBQKBgERpUxpX4r5Obi8PNIECZ4ucTlhT
KJpn8B+9GrIjTqs+ae0oRfbSo1Jt/SDts/c6DYaT2RZma7JVosWd2aOAw9k69zMObHlJrcHGmb3l
eCc/SSPAJvor9B8dBoTQZbaAF4js/wffMl2Qg1WuFfyRQIAhHYO1I9aibqcJmSwDKmsL''',
                                       'name':'myooici',
                                       'institution':'OOICI',
                                       'email':'myooici@gmail.com',
                                       'authenticating_organization':'Google'}
                        },


root_name:{ID_CFG:'E15CADEA-4605-4AFD-AF80-8FC3BC54D2A3',
                          TYPE_CFG:identity_type,
                          NAME_CFG:root_name,
                          DESCRIPTION_CFG:'The root user is the super administrator.',
                          CONTENT_CFG:{'subject':'aa',
                                       'certificate':'',
                                       'rsa_private_key':'',
                                       'name':'',
                                       'institution':'',
                                       'email':'',
                                       'authenticating_organization':''}
                        },


}

ROOT_USER_ID = ION_IDENTITIES[root_name][ID_CFG]
ANONYMOUS_USER_ID = ION_IDENTITIES[anonymous_name][ID_CFG]
MYOOICI_USER_ID = ION_IDENTITIES[myooici_name][ID_CFG]



##### Define Datasets and data sources #####:

# Dataset names
profile_dataset_name = 'sample_profile_dataset'
profile_data_source_name = 'sample_profile_datasource'

traj_dataset_name = 'sample_traj_dataset'
traj_data_source_name = 'sample_traj_datasource'

station_dataset_name = 'sample_station_dataset'
station_data_source_name = 'sample_station_datasource'

### BIG DATASET - A 3D Grid!
hycom_dataset_name = 'sample_hycom_dataset'
hycom_data_source_name = 'sample_hycom_datasource'

### BIG DATASET - A 3D Grid split into pieces (bounded arrays)!
hycom_split_dataset_name = 'sample_split_hycom_dataset'
hycom_split_data_source_name = 'sample_split_hycom_datasource'

ntas1_dataset_name = 'samples_ntas_rt_mooring1_dataset'
ntas1_data_source_name = 'samples_ntas_rt_mooring1_datasource'

ntas2_dataset_name = 'samples_ntas_rt_mooring2_dataset'
ntas2_data_source_name = 'samples_ntas_rt_mooring2_datasource'

whots1_dataset_name = 'samples_whots_nrt_mooring1_dataset'
whots1_data_source_name = 'samples_whots_nrt_mooring1_datasource'

whots2_dataset_name = 'samples_whots_nrt_mooring2_dataset'
whots2_data_source_name = 'samples_whots_nrt_mooring2_datasource'

moanalua_rain_dataset_name = 'sample_rain_gauge_dataset'
moanalua_rain_data_source_name = 'sample_rain_gauge_datasource'

choptank_river_dataset_name = 'sample_choptank_river_dataset'
choptank_river_data_source_name = 'sample_choptank_river_datasource'

connecticut_river_dataset_name = 'sample_connecticut_river_dataset'
connecticut_river_data_source_name = 'sample_connecticut_river_datasource'


# Resource Byte Array file locations
### profile dataset is generated from code!
trj_dataset_loc = CONF.getValue(traj_dataset_name, None)
stn_dataset_loc = CONF.getValue(station_dataset_name, None)

moanalua_rain_dataset_loc = CONF.getValue(moanalua_rain_dataset_name, None)
choptank_river_dataset_loc = CONF.getValue(choptank_river_dataset_name, None)
connecticut_river_dataset_loc = CONF.getValue(connecticut_river_dataset_name, None)

ntas1_dataset_loc = CONF.getValue(ntas1_dataset_name, None)
ntas2_dataset_loc = CONF.getValue(ntas2_dataset_name, None)

whots1_dataset_loc = CONF.getValue(whots1_dataset_name, None)
whots2_dataset_loc = CONF.getValue(whots2_dataset_name, None)

hycom_dataset_loc = CONF.getValue(hycom_dataset_name, None)
hycom_split_dataset_loc = CONF.getValue(hycom_split_dataset_name, None)


DATASET_TYPE = create_type_identifier(object_id=10001, version=1)
DATASOURCE_TYPE = create_type_identifier(object_id=4503, version=1)
# Data structure used by datastore intialization
TESTING_SIGNIFIER = '3319A67F'
ION_DATASETS={
profile_dataset_name:{ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E047F1',
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:profile_dataset_name,
                      DESCRIPTION_CFG:'An example of a profile dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_profile_dataset,
                      },

traj_dataset_name:{ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E047F3',
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:traj_dataset_name,
                      DESCRIPTION_CFG:'An example of a trajectory dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_byte_array_dataset,
                      CONTENT_ARGS_CFG:{'filename':trj_dataset_loc},
                      },

station_dataset_name:{ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E047F4',
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:station_dataset_name,
                      DESCRIPTION_CFG:'An example of a station dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_byte_array_dataset,
                      CONTENT_ARGS_CFG:{'filename':stn_dataset_loc},
                      },

hycom_split_dataset_name:{
                      ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E04800',
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:hycom_split_dataset_name,
                      DESCRIPTION_CFG:'An example of a HYCOM 3d grid model dataset split into multiple bounded arrays',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_byte_array_dataset,
                      CONTENT_ARGS_CFG:{'filename':hycom_split_dataset_loc},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG]
                      },


hycom_dataset_name:{
                      ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E04801',
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:hycom_dataset_name,
                      DESCRIPTION_CFG:'An example of a HYCOM 3d grid model dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_byte_array_dataset,
                      CONTENT_ARGS_CFG:{'filename':hycom_dataset_loc},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG]
                      },


ntas1_dataset_name:{
                      ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E04802',
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:ntas1_dataset_name,
                      DESCRIPTION_CFG:'An example of an NTAS Real Time Mooring Data System Dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_byte_array_dataset,
                      CONTENT_ARGS_CFG:{'filename':ntas1_dataset_loc},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG]
                      },

ntas2_dataset_name:{
                      ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E04803',
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:ntas2_dataset_name,
                      DESCRIPTION_CFG:'An example of an NTAS Real Time Mooring Data System Dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_byte_array_dataset,
                      CONTENT_ARGS_CFG:{'filename':ntas2_dataset_loc},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG]
                      },

whots1_dataset_name:{
                      ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E04804',
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:whots1_dataset_name,
                      DESCRIPTION_CFG:'An example of a WHOTS Near Real Time Mooring Data System Dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_byte_array_dataset,
                      CONTENT_ARGS_CFG:{'filename':whots1_dataset_loc},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG]
                      },

whots2_dataset_name:{
                      ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E04805',
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:whots2_dataset_name,
                      DESCRIPTION_CFG:'An example of a WHOTS Near Real Time Mooring Data System Dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_byte_array_dataset,
                      CONTENT_ARGS_CFG:{'filename':whots2_dataset_loc},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG]
                      },

moanalua_rain_dataset_name:{
                      ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E04806',
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:moanalua_rain_dataset_name,
                      DESCRIPTION_CFG:'An example of a rain gauge dataset from moanalua',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_byte_array_dataset,
                      CONTENT_ARGS_CFG:{'filename':moanalua_rain_dataset_loc},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG]
                      },

choptank_river_dataset_name:{
                      ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E04807',
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:choptank_river_dataset_name,
                      DESCRIPTION_CFG:'An example of a usgs stream gauge Dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_byte_array_dataset,
                      CONTENT_ARGS_CFG:{'filename':choptank_river_dataset_loc},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG]
                      },

connecticut_river_dataset_name:{
                      ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E04808',
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:connecticut_river_dataset_name,
                      DESCRIPTION_CFG:'An example of a usgs stream gauge Dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_byte_array_dataset,
                      CONTENT_ARGS_CFG:{'filename':connecticut_river_dataset_loc},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG]
                      },

}



ION_DATA_SOURCES ={

profile_data_source_name:{ID_CFG:TESTING_SIGNIFIER + '-91F3-424F-8E69-4F28C4E047F2',
                      TYPE_CFG:DATASOURCE_TYPE,
                      NAME_CFG:profile_data_source_name,
                      DESCRIPTION_CFG:'An example of a data source for the profile dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_profile_data_source_resource,
                      CONTENT_ARGS_CFG:{'associated_dataset_id':ION_DATASETS[profile_dataset_name][ID_CFG]}
                      },

traj_data_source_name:{ID_CFG:TESTING_SIGNIFIER + '-91F3-424F-8E69-4F28C4E047F5',
                      TYPE_CFG:DATASOURCE_TYPE,
                      NAME_CFG:traj_data_source_name,
                      DESCRIPTION_CFG:'An example of a data source for the trajectory dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_traj_data_source,
                      CONTENT_ARGS_CFG:{'associated_dataset_id':ION_DATASETS[traj_dataset_name][ID_CFG]}
                      },

station_data_source_name:{ID_CFG:TESTING_SIGNIFIER + '-91F3-424F-8E69-4F28C4E047F6',
                      TYPE_CFG:DATASOURCE_TYPE,
                      NAME_CFG:station_data_source_name,
                      DESCRIPTION_CFG:'An example of a data source for the station dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_station_data_source,
                      CONTENT_ARGS_CFG:{'associated_dataset_id':ION_DATASETS[station_dataset_name][ID_CFG]}
                      },

hycom_split_data_source_name:{ID_CFG:TESTING_SIGNIFIER + '-91F3-424F-8E69-4F28C4E04800',
                      TYPE_CFG:DATASOURCE_TYPE,
                      NAME_CFG:hycom_split_data_source_name,
                      DESCRIPTION_CFG:'An example of a data source for a Hycom model dataset split into multiple bounded arrays',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_hycom_data_source,
                      CONTENT_ARGS_CFG:{'associated_dataset_id':ION_DATASETS[hycom_split_dataset_name][ID_CFG]},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG]
                      },

hycom_data_source_name:{ID_CFG:TESTING_SIGNIFIER + '-91F3-424F-8E69-4F28C4E04801',
                      TYPE_CFG:DATASOURCE_TYPE,
                      NAME_CFG:hycom_data_source_name,
                      DESCRIPTION_CFG:'An example of a data source for a Hycom model dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_hycom_data_source,
                      CONTENT_ARGS_CFG:{'associated_dataset_id':ION_DATASETS[hycom_dataset_name][ID_CFG]},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG]
                      },

ntas1_data_source_name:{ID_CFG:TESTING_SIGNIFIER + '-91F3-424F-8E69-4F28C4E04802',
                      TYPE_CFG:DATASOURCE_TYPE,
                      NAME_CFG:ntas1_data_source_name,
                      DESCRIPTION_CFG:'An example of a data source for the NTAS RT dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_ntas1_data_source,
                      CONTENT_ARGS_CFG:{'associated_dataset_id':ION_DATASETS[ntas1_dataset_name][ID_CFG]},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG]
                      },

ntas2_data_source_name:{ID_CFG:TESTING_SIGNIFIER + '-91F3-424F-8E69-4F28C4E04803',
                      TYPE_CFG:DATASOURCE_TYPE,
                      NAME_CFG:ntas2_data_source_name,
                      DESCRIPTION_CFG:'An example of a data source for the NTAS RT dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_ntas2_data_source,
                      CONTENT_ARGS_CFG:{'associated_dataset_id':ION_DATASETS[ntas2_dataset_name][ID_CFG]},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG]
                      },

whots1_data_source_name:{ID_CFG:TESTING_SIGNIFIER + '-91F3-424F-8E69-4F28C4E04804',
                      TYPE_CFG:DATASOURCE_TYPE,
                      NAME_CFG:whots1_data_source_name,
                      DESCRIPTION_CFG:'An example of a data source for the WHOTS NRT dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_whots1_data_source,
                      CONTENT_ARGS_CFG:{'associated_dataset_id':ION_DATASETS[whots1_dataset_name][ID_CFG]},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG]
                      },

whots2_data_source_name:{ID_CFG:TESTING_SIGNIFIER + '-91F3-424F-8E69-4F28C4E04805',
                      TYPE_CFG:DATASOURCE_TYPE,
                      NAME_CFG:whots2_data_source_name,
                      DESCRIPTION_CFG:'An example of a data source for the WHOTS NRT dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_whots2_data_source,
                      CONTENT_ARGS_CFG:{'associated_dataset_id':ION_DATASETS[whots2_dataset_name][ID_CFG]},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG]
                      },

moanalua_rain_data_source_name:{ID_CFG:TESTING_SIGNIFIER + '-91F3-424F-8E69-4F28C4E04806',
                      TYPE_CFG:DATASOURCE_TYPE,
                      NAME_CFG:moanalua_rain_data_source_name,
                      DESCRIPTION_CFG:'An example of a rain gauge data source from moanalua',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_moanalua_data_source,
                      CONTENT_ARGS_CFG:{'associated_dataset_id':ION_DATASETS[moanalua_rain_dataset_name][ID_CFG]},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG]
                      },

choptank_river_data_source_name:{ID_CFG:TESTING_SIGNIFIER + '-91F3-424F-8E69-4F28C4E04807',
                      TYPE_CFG:DATASOURCE_TYPE,
                      NAME_CFG:choptank_river_data_source_name,
                      DESCRIPTION_CFG:'An example of a usgs stream gauge Dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_choptank_river_data_source,
                      CONTENT_ARGS_CFG:{'associated_dataset_id':ION_DATASETS[choptank_river_dataset_name][ID_CFG]},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG]
                      },

connecticut_river_data_source_name:{ID_CFG:TESTING_SIGNIFIER + '-91F3-424F-8E69-4F28C4E04808',
                      TYPE_CFG:DATASOURCE_TYPE,
                      NAME_CFG:connecticut_river_data_source_name,
                      DESCRIPTION_CFG:'An example of a usgs stream gauge Dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_connecticut_river_data_source,
                      CONTENT_ARGS_CFG:{'associated_dataset_id':ION_DATASETS[connecticut_river_dataset_name][ID_CFG]},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG]
                      },


}


# Extract Resource ID_CFGs for use in services and tests
SAMPLE_PROFILE_DATASET_ID = ION_DATASETS[profile_dataset_name][ID_CFG]
SAMPLE_PROFILE_DATA_SOURCE_ID = ION_DATA_SOURCES[profile_data_source_name][ID_CFG]

SAMPLE_TRAJ_DATASET_ID = ION_DATASETS[traj_dataset_name][ID_CFG]
SAMPLE_TRAJ_DATA_SOURCE_ID = ION_DATA_SOURCES[traj_data_source_name][ID_CFG]

SAMPLE_STATION_DATASET_ID = ION_DATASETS[station_dataset_name][ID_CFG]
SAMPLE_STATION_DATA_SOURCE_ID = ION_DATA_SOURCES[station_data_source_name][ID_CFG]

SAMPLE_HYCOM_DATASET_ID = ION_DATASETS[hycom_dataset_name][ID_CFG]
SAMPLE_HYCOM_DATA_SOURCE_ID = ION_DATA_SOURCES[hycom_data_source_name][ID_CFG]

SAMPLE_SPLIT_HYCOM_DATASET_ID = ION_DATASETS[hycom_split_dataset_name][ID_CFG]
SAMPLE_SPLIT_HYCOM_DATA_SOURCE_ID = ION_DATA_SOURCES[hycom_split_data_source_name][ID_CFG]

SAMPLE_NTAS1_DATASET_ID = ION_DATASETS[ntas1_dataset_name][ID_CFG]
SAMPLE_NTAS1_DATA_SOURCE_ID = ION_DATA_SOURCES[ntas1_data_source_name][ID_CFG]

SAMPLE_NTAS2_DATASET_ID = ION_DATASETS[ntas2_dataset_name][ID_CFG]
SAMPLE_NTAS2_DATA_SOURCE_ID = ION_DATA_SOURCES[ntas2_data_source_name][ID_CFG]

SAMPLE_WHOTS1_DATASET_ID = ION_DATASETS[whots1_dataset_name][ID_CFG]
SAMPLE_WHOTS1_DATA_SOURCE_ID = ION_DATA_SOURCES[whots1_data_source_name][ID_CFG]

SAMPLE_WHOTS2_DATASET_ID = ION_DATASETS[whots2_dataset_name][ID_CFG]
SAMPLE_WHOTS2_DATA_SOURCE_ID = ION_DATA_SOURCES[whots2_data_source_name][ID_CFG]

SAMPLE_MOANALUA_RAIN_GAUGE_DATASET_ID = ION_DATASETS[moanalua_rain_dataset_name][ID_CFG]
SAMPLE_MOANALUA_RAIN_GAUGE_DATA_SOURCE_ID = ION_DATA_SOURCES[moanalua_rain_data_source_name][ID_CFG]

SAMPLE_CHOPTANK_RIVER_GAUGE_DATASET_ID = ION_DATASETS[choptank_river_dataset_name][ID_CFG]
SAMPLE_CHOPTANK_RIVER_GAUGE_DATA_SOURCE_ID = ION_DATA_SOURCES[choptank_river_data_source_name][ID_CFG]

SAMPLE_CONNECTICUT_RIVER_GAUGE_DATASET_ID = ION_DATASETS[connecticut_river_dataset_name][ID_CFG]
SAMPLE_CONNECTICUT_RIVER_GAUGE_DATA_SOURCE_ID = ION_DATA_SOURCES[connecticut_river_data_source_name][ID_CFG]



#### Define AIS Resources that should be preloaded for testing purposes:

# Define types that will be created:
TOPIC_TYPE = create_type_identifier(object_id=2317, version=1)
### Note - Topics contain IDRef pointers to other resources.
### We need to create a Topic generator function if these IDRefs are required for AIS testing.

# Define resource names that will be crated:
example_topic1_name = 'example_topic1'

# Define the configuration dictionary for the resources
ION_AIS_RESOURCES={
example_topic1_name:{ID_CFG:'341FF107-5E42-4C8E-A30B-4A65A5675E63',
                      TYPE_CFG:TOPIC_TYPE,
                      NAME_CFG:profile_dataset_name,
                      DESCRIPTION_CFG:'An example of a topic resource',
                      CONTENT_CFG:{'exchange_space_name':'swap meet',
                                       'exchange_point_name':'science_data',
                                       'topic_name':'important science data'}
                      },

    }

# Extract Resource ID_CFGs for use in services and tests
EXAMPLE_TOPIC1_ID = ION_AIS_RESOURCES[example_topic1_name][ID_CFG]




def generate_reference_instance(proc=None, resource_id=None):
    """
    Helper method to create a resource type identifier. Uses the process work bench
    to create an ID_CFGRef object which can be used in find, get and association operations

    @param proc is a process instance which is in an active state.
    @param resource_id is a resource id defined in this file.
    """

    #@ TODO Complete this method... Should be about ten lines.
    

class TypeMap(dict):

    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)

        type_map = {}
        for type_name, description in ION_RESOURCE_TYPES.items():
            type_cfg = description.get(CONTENT_CFG)
            obj_type_id = type_cfg.get('object_identifier')
            type_map[obj_type_id] = description.get(ID_CFG)

        self.update(type_map)


    def get(self, key):
        '''
        Get the resource type given an object type id #
        '''

        return dict.get(self,key, DEFAULT_RESOURCE_TYPE_ID)



class PredicateMap(dict):

    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)

        predicate_map = {}
        for predicate_name, description in ION_PREDICATES.items():
            preidcate_cfg = description.get(CONTENT_CFG)
            predicate_map[description.get(ID_CFG)] =  preidcate_cfg

        self.update(predicate_map)


    def get(self, key):
        '''
        Get the resource type given an object type id #
        '''

        return dict.get(self,key, 'Unknown Predicate!')





