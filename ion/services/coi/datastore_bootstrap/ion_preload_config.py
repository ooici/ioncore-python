#!/usr/bin/env python

"""
@file ion/res/config.py
@author David Stuebe
@TODO
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


# Set some constants based on the config file:
ION_PREDICATES_CFG = 'ion_predicates'
ION_RESOURCE_TYPES_CFG = 'ion_resource_types'
ION_DATASETS_CFG = 'ion_datasets'
ION_IDENTITIES_CFG = 'ion_identities'


### Defined Resource Types
topic_res_type_name = 'topic_resource_type'
dataset_res_type_name = 'dataset_resource_type'
identity_res_type_name = 'identity_resource_type'
datasource_res_type_name = 'datasource_resource_type'
resource_type_type_name = 'resource_type_type'
default_resource_type_name = 'default_resource_type'
exchange_space_rtn = 'exchange_space_resource_type'
exchange_point_rtn = 'exchange_point_resource_type'
publisher_rtn = 'publisher_resource_type'
subscriber_rtn = 'subscriber_type'

resource_type_type = create_type_identifier(object_id=1103, version=1)
# Data structure used by datastore intialization
ION_RESOURCE_TYPES={
resource_type_type_name:{ID_CFG:'173a3188-e290-42be-8776-8717077dd207',
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
datasource_res_type_name:{ID_CFG:'b8b7bb73-f578-4604-b3b3-088d28f9a7dc',
                       TYPE_CFG:resource_type_type,
                       NAME_CFG:datasource_res_type_name,
                       DESCRIPTION_CFG:'A data source resource contains information about an source of data - metadata about the input to a dataset',
                       CONTENT_CFG:{'object_identifier':4503,
                                    'object_version':1,
                                    'meta_description':'protomessage?'}
                        },

default_resource_type_name:{ID_CFG:'422ade3c-d820-437f-8bd3-7d8793591eb0',
                     TYPE_CFG:resource_type_type,
                     NAME_CFG:default_resource_type_name,
                     DESCRIPTION_CFG:'A type to catch unregistered types!',
                     CONTENT_CFG:{'object_identifier':-1,
                                  'object_version':-1,
                                  'meta_description':'protomessage?'}
                     },

}

# Extract Resource ID_CFGs for use in services and tests
TOPIC_RESOURCE_TYPE_ID = ION_RESOURCE_TYPES[topic_res_type_name][ID_CFG]
EXCHANGE_SPACE_RES_TYPE_ID = ION_RESOURCE_TYPES[exchange_space_rtn][ID_CFG]
EXCHANGE_POINT_RES_TYPE_ID = ION_RESOURCE_TYPES[exchange_point_rtn][ID_CFG]
PUBLISHER_RES_TYPE_ID = ION_RESOURCE_TYPES[publisher_rtn][ID_CFG]
SUBSCRIBER_RES_TYPE_ID = ION_RESOURCE_TYPES[subscriber_rtn][ID_CFG]
DATASET_RESOURCE_TYPE_ID = ION_RESOURCE_TYPES[dataset_res_type_name][ID_CFG]
IDENTITY_RESOURCE_TYPE_ID = ION_RESOURCE_TYPES[identity_res_type_name][ID_CFG]
DATASOURCE_RESOURCE_TYPE_ID = ION_RESOURCE_TYPES[datasource_res_type_name][ID_CFG]
RESOURCE_TYPE_TYPE_ID = ION_RESOURCE_TYPES[resource_type_type_name][ID_CFG]


DEFAULT_RESOURCE_TYPE_ID = ION_RESOURCE_TYPES[default_resource_type_name][ID_CFG]


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

identity_type = create_type_identifier(object_id=1401, version=1)
ION_IDENTITIES = {
anonymous_name:{ID_CFG:'a3d5d4a0-7265-4ef2-b0ad-3ce2dc7252d8',
                          TYPE_CFG:identity_type,
                          NAME_CFG:anonymous_name,
                          DESCRIPTION_CFG:'The anonymous user is the identity used by any unregistered user.',
                          CONTENT_CFG:{'subject':'ss',
                                       'certificate':'',
                                       'rsa_private_key':'',
                                       'dispatcher_queue':'',
                                       'email':'',
                                       'life_cycle_state':''}
                        },

root_name:{ID_CFG:'e15cadea-4605-4afd-af80-8fc3bc54d2a3',
                          TYPE_CFG:identity_type,
                          NAME_CFG:root_name,
                          DESCRIPTION_CFG:'The root user is the super administrator.',
                          CONTENT_CFG:{'subject':'aa',
                                       'certificate':'',
                                       'rsa_private_key':'',
                                       'dispatcher_queue':'',
                                       'email':'',
                                       'life_cycle_state':''}
                        },


}

ROOT_USER_ID = ION_IDENTITIES[root_name][ID_CFG]
ANONYMOUS_USER_ID = ION_IDENTITIES[anonymous_name][ID_CFG]




##### Define Datasets and data sources #####:

# Dataset names
profile_dataset_name = 'sample_profile_dataset'
profile_data_source_name = 'sample_profile_datasource'
traj_dataset_name = 'sample_traj_dataset'
traj_data_source_name = 'sample_traj_datasource'
station_dataset_name = 'sample_station_dataset'
station_data_source_name = 'sample_station_datasource'
grid_dataset_name = 'grid_dataset'

# Resource Byte Array locations
trj_dataset_loc = CONF.getValue(traj_dataset_name, None)
stn_dataset_loc = CONF.getValue(station_dataset_name, None)


DATASET_TYPE = create_type_identifier(object_id=10001, version=1)
DATASOURCE_TYPE = create_type_identifier(object_id=4503, version=1)
# Data structure used by datastore intialization
TESTING_SIGNIFIER = '3319A67F'
ION_DATASETS={
profile_dataset_name:{ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E047F1',
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:profile_dataset_name,
                      DESCRIPTION_CFG:'An example of a profile dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_profile_dataset
                      },

profile_data_source_name:{ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E047F2',
                      TYPE_CFG:DATASOURCE_TYPE,
                      NAME_CFG:profile_data_source_name,
                      DESCRIPTION_CFG:'An example of a data source for the profile dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_data_source_resource
                      },

traj_dataset_name:{ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E047F3',
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:traj_dataset_name,
                      DESCRIPTION_CFG:'An example of a trajectory dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_byte_array_dataset,
                      CONTENT_ARGS_CFG:{'filename':trj_dataset_loc},
                      },
                      
traj_data_source_name:{ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E047F5',
                      TYPE_CFG:DATASOURCE_TYPE,
                      NAME_CFG:traj_data_source_name,
                      DESCRIPTION_CFG:'An example of a data source for the trajectory dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_traj_data_source
                      },

station_dataset_name:{ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E047F4',
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:station_dataset_name,
                      DESCRIPTION_CFG:'An example of a station dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_byte_array_dataset,
                      CONTENT_ARGS_CFG:{'filename':stn_dataset_loc},
                      },
                      
station_data_source_name:{ID_CFG:TESTING_SIGNIFIER + '-81F3-424F-8E69-4F28C4E047F6',
                      TYPE_CFG:DATASOURCE_TYPE,
                      NAME_CFG:station_data_source_name,
                      DESCRIPTION_CFG:'An example of a data source for the station dataset',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_station_data_source
                      },

#grid_dataset_name:{ID_CFG:''},
}


# Extract Resource ID_CFGs for use in services and tests
SAMPLE_PROFILE_DATASET_ID = ION_DATASETS[profile_dataset_name][ID_CFG]
SAMPLE_PROFILE_DATA_SOURCE_ID = ION_DATASETS[profile_data_source_name][ID_CFG]
SAMPLE_TRAJ_DATASET_ID = ION_DATASETS[traj_dataset_name][ID_CFG]
SAMPLE_TRAJ_DATA_SOURCE_ID = ION_DATASETS[traj_data_source_name][ID_CFG]
SAMPLE_STATION_DATASET_ID = ION_DATASETS[station_dataset_name][ID_CFG]
SAMPLE_STATION_DATA_SOURCE_ID = ION_DATASETS[station_data_source_name][ID_CFG]
#SAMPLE_GRID_DATASET_ID = ION_DATASETS[grid_dataset_name][ID_CFG]










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




