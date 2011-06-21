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
LCS_CFG = 'life cycle state'

COMMISSIONED = 'commissioned'


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
has_role_name = 'has_role'

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

has_role_name:{ID_CFG:'eed4b68d-58b5-493b-b409-1291d97cb1d8',
            TYPE_CFG:TERMINOLOGY_TYPE,
            PREDICATE_CFG:has_role_name},

}


# Extract Resource ID_CFGs for use in services and tests
HAS_A_ID = ION_PREDICATES[has_a_name][ID_CFG]
IS_A_ID = ION_PREDICATES[is_a_name][ID_CFG]
TYPE_OF_ID = ION_PREDICATES[type_of_name][ID_CFG]
OWNED_BY_ID = ION_PREDICATES[owned_by_name][ID_CFG]

HAS_LIFE_CYCLE_STATE_ID = ION_PREDICATES[has_life_cycle_state_name][ID_CFG]

HAS_ROLE_ID = ION_PREDICATES[has_role_name][ID_CFG]

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
                          CONTENT_CFG:{'subject':'/DC=org/DC=cilogon/C=US/O=Google/CN=anonymous',
                                       'certificate':'',
                                       'rsa_private_key':'',
                                       'name':'Anonymous User',
                                       'institution':'OOICI',
                                       'email':'ooici-anonymous@ucsd.edu',
                                       'authenticating_organization':''}
                        },

myooici_name:{ID_CFG:'A7B44115-34BC-4553-B51E-1D87617F12E0',
                          TYPE_CFG:identity_type,
                          NAME_CFG:myooici_name,
                          DESCRIPTION_CFG:'The first test user - poor sole!.',
                          CONTENT_CFG:{'subject':'/DC=org/DC=cilogon/C=US/O=Google/CN=OOI-CI OOI A552',
                                       'certificate':
'''MIIEVDCCAzygAwIBAgICCQ4wDQYJKoZIhvcNAQELBQAwazETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRww
GgYDVQQDExNDSUxvZ29uIE9wZW5JRCBDQSAxMB4XDTExMDYwMzIxMDIxOFoXDTExMDYwNDA5MDcx
OFowZjETMBEGCgmSJomT8ixkARkTA29yZzEXMBUGCgmSJomT8ixkARkTB2NpbG9nb24xCzAJBgNV
BAYTAlVTMQ8wDQYDVQQKEwZHb29nbGUxGDAWBgNVBAMTD09PSS1DSSBPT0kgQTU1MjCCASIwDQYJ
KoZIhvcNAQEBBQADggEPADCCAQoCggEBAMIbdvzufLyoedYoWaKW8OISLcC8GfvpvhnUmrM9prEI
NHYwSfXuVlqVGHXtRUfPJj0Its+TQf7myOH5gsApqwX2MqP5QcJyO2aNWRNkTmK3XPC7gWI0Hcd5
qgwzzK3Sn6UKRjmoEcjL2vm9NaNIg8TMkj04lAG3Re59+v5uLq+cltced2QKKpxdU8EWtGMQozAu
AYaJM1avcX51ea122z49LrNCJ+2dFcpklYF61C6/A9guKkXGhk0KM+n8JU1pyKlpvmI/p8wVbgs5
GnmCaUdsyUnblXAKP3pioC1LJMRzm15YP6GLGyo8lRQviIR9efKLXoVS3PisC7eoCcyUOTcCAwEA
AaOCAQUwggEBMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgSwMBMGA1UdJQQMMAoGCCsGAQUF
BwMCMBgGA1UdIAQRMA8wDQYLKwYBBAGCkTYBAwMwbAYDVR0fBGUwYzAvoC2gK4YpaHR0cDovL2Ny
bC5jaWxvZ29uLm9yZy9jaWxvZ29uLW9wZW5pZC5jcmwwMKAuoCyGKmh0dHA6Ly9jcmwuZG9lZ3Jp
ZHMub3JnL2NpbG9nb24tb3BlbmlkLmNybDBEBgNVHREEPTA7gRFteW9vaWNpQGdtYWlsLmNvbYYm
dXJuOnB1YmxpY2lkOklETitjaWxvZ29uLm9yZyt1c2VyK0E1NTIwDQYJKoZIhvcNAQELBQADggEB
AAW2n6oHSRBK3hoO/7628SLh0WCesmISKzqZRm1K6EuYiLpLsgfLOZWqu27UmuxlrBNDYNs3lgL/
8VaDVo9sJMowrdWhBawALuEHrIYkX6S1HsgvcRW9n23zb1AyjwbCZlKK8QH4Moh6uByO+pOSZdbV
Lz2dw6nIoKz702VMiElLXeE1pDJIeCr5W1FJAZpi9SEWIzdjtHGojSpUx7CNupCOOTIH8R1cHbO0
mBDnP20LUI+JjtN1Va0bAHc2W8UZSsW8g4QvTBJ7XvsOGV+7XhFmxZmEhGaFDtPyCMW6E34EuRJS
9l8al9sP+u2brS6fQ5qoc5xyZVVcffYPdBFT8gY=''',
                                       'rsa_private_key':
'''MIIEowIBAAKCAQEAwht2/O58vKh51ihZopbw4hItwLwZ++m+GdSasz2msQg0djBJ9e5WWpUYde1F
R88mPQi2z5NB/ubI4fmCwCmrBfYyo/lBwnI7Zo1ZE2ROYrdc8LuBYjQdx3mqDDPMrdKfpQpGOagR
yMva+b01o0iDxMySPTiUAbdF7n36/m4ur5yW1x53ZAoqnF1TwRa0YxCjMC4BhokzVq9xfnV5rXbb
Pj0us0In7Z0VymSVgXrULr8D2C4qRcaGTQoz6fwlTWnIqWm+Yj+nzBVuCzkaeYJpR2zJSduVcAo/
emKgLUskxHObXlg/oYsbKjyVFC+IhH158otehVLc+KwLt6gJzJQ5NwIDAQABAoIBAQCoDild4YmD
uYYK4dKBT5fs03pjZThF/+DD8muiBh2dJpJtRW+zio+fS3jrGOujuXjM3Q+R9lfsPpnr9B+9ChZ1
SewcRcEmfcpqBrT5ch3foAvKrTze7mpd+zs751ktoa7wsE2Ou7HyHHVRRfz7itvy9n8inCqgtbHJ
Q6+cu36WMUXhDlfa9hq73DN2nmKZjqaRg0rIIfyLa4fvMFWz5AtHR8FOwk79YvzOAE70MXuca0en
NmqXD/OaZ4MNXTMdPt0f2hlYOO+/rPv8DZpfi+joB9NQ+ZZqcb7nQ56yOcJx+yPjQ+8yRmheil4g
BVRr83Z41ZsCpuHpnP6FdwZNqobpAoGBAONEBtYpyktY6bbc1Z9pAy00kiGyoNFCzvtvhL3JvPZo
nC+N0aRMnlWXxxPtvyqyJIJvqK1KbWcd4yD200xhLpC5r/y3HpXJ19V+mAQ0cEsuZamw9K1fB+EY
aSlp9Foz/5cZCiX31F8yi0js+IuP1xzAv7oqup9CFry/6wp7jBm7AoGBANqmMH+0OpFVIiSjBjre
/E+sSxcqrajv4JvDns96fJjGOJ/LBY0eUUhwPY4wim0rfNcu3Hmotp/X8w0+OX0svXhu0MP0WCCJ
y/S8wNIQuXN25mqVRmU+hLFii4t7SgdxM8r1/oQKH0lAqE123zAYR417cdBurCBQA5aKKyEyTZi1
AoGAWesucUnzmkBBqHJTq1DXSumD8AVHD8TJND55XMYXF79oHICWM9WEyATXZZEpk/EL9PfM21OZ
WbU/imleTNgennB5qxmg5k8IMJZ3+yHsVDK1UqCLDpWM/oi0AwjC/3WXaOclVsRpqIjNBzuLU1zE
FcJFmZkSYbS6Xk/o5Srg0cUCgYBVRLZpNwoYH1E/ZGxLjSZsk8587GHpHhND65gFZcktczAl8PDr
RcWBMHRw/TEevfTjnhzRPSBrWbYplfipfkctrlmv8ZxkpBhsCyhPQ8Ju6xGUwz4+wZDR9JJjBOOr
31PJdQGa0K++y35XJ2KGyREuddO+60opF8subBfBzHJCeQKBgA0SkCgGHFILi80EG4FHZCdb3+CR
w/0z56l5aPSP52xpWjzPyywv+4ku+LXEyWF3qj4xJww8SVBP5nmTsYEJwu26g97ZWprehJzOOhWu
11HQQLNLNPYu68sggMAjjdguSl7W2cEJskqTWs8Gsjug0HQw/I3I9MTJKa71rsYBNdhL''',
                                       'name':'myooici',
                                       'institution':'OOICI',
                                       'email':'myooici@gmail.com',
                                       'authenticating_organization':'Google'}
                        },


root_name:{ID_CFG:'E15CADEA-4605-4AFD-AF80-8FC3BC54D2A3',
                          TYPE_CFG:identity_type,
                          NAME_CFG:root_name,
                          DESCRIPTION_CFG:'The root user is the super administrator.',
                          CONTENT_CFG:{'subject':'/DC=org/DC=cilogon/C=US/O=Google/CN=root',
                                       'certificate':'',
                                       'rsa_private_key':'',
                                       'name':'Root User',
                                       'institution':'OOICI',
                                       'email':'ooici-root@ucsd.edu',
                                       'authenticating_organization':''}
                        },


}

ROOT_USER_ID = ION_IDENTITIES[root_name][ID_CFG]
ANONYMOUS_USER_ID = ION_IDENTITIES[anonymous_name][ID_CFG]
MYOOICI_USER_ID = ION_IDENTITIES[myooici_name][ID_CFG]


###### Define Roles ##########

admin_name = 'Administrator'
data_provider_name = 'Data Provider'
marine_operator_name = 'Marine Operator'
early_adopter_name = 'Early Adopter'
authenticated_name = 'Authenticated'

# Anonymous name is already defined

role_type = create_type_identifier(object_id=1404, version=1)

ION_ROLES = {
admin_name:{ID_CFG:'A3D5E5B1-83786-4EF2-C1BE-3CE2DC7252D0',
                          TYPE_CFG:role_type,
                          NAME_CFG:'OOI User Role Resource: %s' % admin_name,
                          DESCRIPTION_CFG:'The Admin Role for administrator users.',
                          CONTENT_CFG:{'role':admin_name}
                        },

data_provider_name:{ID_CFG:'A3D5E5B1-83786-4EF2-C1BE-3CE2DC7252D1',
                          TYPE_CFG:role_type,
                          NAME_CFG:'OOI User Role Resource: %s' % data_provider_name,
                          DESCRIPTION_CFG:'The data provider role for a owner of data sources.',
                          CONTENT_CFG:{'role':data_provider_name}
                        },

marine_operator_name:{ID_CFG:'A3D5E5B1-83786-4EF2-C1BE-3CE2DC7252D2',
                          TYPE_CFG:role_type,
                          NAME_CFG:'OOI User Role Resource: %s' % marine_operator_name,
                          DESCRIPTION_CFG:'The marine operator role for marine IO facilities.',
                          CONTENT_CFG:{'role':marine_operator_name}
                        },

early_adopter_name:{ID_CFG:'A3D5E5B1-83786-4EF2-C1BE-3CE2DC7252D3',
                          TYPE_CFG:role_type,
                          NAME_CFG:'OOI User Role Resource: %s' % early_adopter_name,
                          DESCRIPTION_CFG:'The early adopter role for IOOS science beta users.',
                          CONTENT_CFG:{'role':early_adopter_name}
                        },

authenticated_name:{ID_CFG:'A3D5E5B1-83786-4EF2-C1BE-3CE2DC7252D4',
                          TYPE_CFG:role_type,
                          NAME_CFG:'OOI User Role Resource: %s' % authenticated_name,
                          DESCRIPTION_CFG:'The authenticated role for registered users.',
                          CONTENT_CFG:{'role':authenticated_name}
                        },


    }

ADMIN_ROLE_ID = ION_ROLES[admin_name][ID_CFG]
DATA_PROVIDER_ROLE_ID = ION_ROLES[data_provider_name][ID_CFG]
MARINE_OPERATOR_ROLE_ID = ION_ROLES[marine_operator_name][ID_CFG]
EARLY_ADOPTER_ROLE_ID = ION_ROLES[early_adopter_name][ID_CFG]
AUTHENTICATED_ROLE_ID = ION_ROLES[authenticated_name][ID_CFG]

ROLE_IDS_BY_NAME = {'ADMIN': ADMIN_ROLE_ID, 'DATA_PROVIDER': DATA_PROVIDER_ROLE_ID,
                    'MARINE_OPERATOR': MARINE_OPERATOR_ROLE_ID, 'EARLY_ADOPTER': EARLY_ADOPTER_ROLE_ID,
                    'AUTHENTICATED': AUTHENTICATED_ROLE_ID}

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
                      LCS_CFG : COMMISSIONED
                      },

station_dataset_name:{ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E047F4',
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:station_dataset_name,
                      DESCRIPTION_CFG:'An example of a station dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_byte_array_dataset,
                      CONTENT_ARGS_CFG:{'filename':stn_dataset_loc},
                      LCS_CFG : COMMISSIONED
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
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG],
                      LCS_CFG : COMMISSIONED
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
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG],
                      LCS_CFG : COMMISSIONED
                      },

moanalua_rain_dataset_name:{
                      ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E04806',
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:moanalua_rain_dataset_name,
                      DESCRIPTION_CFG:'An example of a rain gauge dataset from moanalua',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_byte_array_dataset,
                      CONTENT_ARGS_CFG:{'filename':moanalua_rain_dataset_loc},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG],
                      LCS_CFG : COMMISSIONED
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
                      CONTENT_ARGS_CFG:{'associated_dataset_id':ION_DATASETS[profile_dataset_name][ID_CFG]},
                      },

traj_data_source_name:{ID_CFG:TESTING_SIGNIFIER + '-91F3-424F-8E69-4F28C4E047F5',
                      TYPE_CFG:DATASOURCE_TYPE,
                      NAME_CFG:traj_data_source_name,
                      DESCRIPTION_CFG:'An example of a data source for the trajectory dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_traj_data_source,
                      CONTENT_ARGS_CFG:{'associated_dataset_id':ION_DATASETS[traj_dataset_name][ID_CFG]},
                      LCS_CFG : COMMISSIONED
                      },

station_data_source_name:{ID_CFG:TESTING_SIGNIFIER + '-91F3-424F-8E69-4F28C4E047F6',
                      TYPE_CFG:DATASOURCE_TYPE,
                      NAME_CFG:station_data_source_name,
                      DESCRIPTION_CFG:'An example of a data source for the station dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_station_data_source,
                      CONTENT_ARGS_CFG:{'associated_dataset_id':ION_DATASETS[station_dataset_name][ID_CFG]},
                      LCS_CFG : COMMISSIONED
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
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG],
                      LCS_CFG : COMMISSIONED
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
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG],
                      LCS_CFG : COMMISSIONED
                      },

moanalua_rain_data_source_name:{ID_CFG:TESTING_SIGNIFIER + '-91F3-424F-8E69-4F28C4E04806',
                      TYPE_CFG:DATASOURCE_TYPE,
                      NAME_CFG:moanalua_rain_data_source_name,
                      DESCRIPTION_CFG:'An example of a rain gauge data source from moanalua',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_moanalua_data_source,
                      CONTENT_ARGS_CFG:{'associated_dataset_id':ION_DATASETS[moanalua_rain_dataset_name][ID_CFG]},
                      OWNER_ID : ION_IDENTITIES[myooici_name][ID_CFG],
                      LCS_CFG : COMMISSIONED
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





