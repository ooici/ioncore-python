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
### Constants used in defining the basic configuration which is preloaded into the datastore:
ID_CFG = 'id'
TYPE_CFG = 'type'
NAME_CFG = 'name'
PREDICATE_CFG = 'predicate'
DESCRIPTION_CFG = 'description'
CONTENT_CFG = 'content'


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

resource_type_type = create_type_identifier(object_id=1103, version=1)
# Data structure used by datastore intialization
ION_RESOURCE_TYPES={
topic_res_type_name:{ID_CFG:'3BD84B48-073E-4833-A62B-0DE4EC106A34',
                     TYPE_CFG:resource_type_type,
                     NAME_CFG:topic_res_type_name,
                     DESCRIPTION_CFG:'A topic resource is used by the pubsub controller service to represent a topic on which messages can be sent',
                     CONTENT_CFG:{'object_identifier':2317,
                                  'object_version':1,
                                  'meta_description':'protomessage?'}
                     },

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
                        }
}

# Extract Resource ID_CFGs for use in services and tests
TOPIC_RESOURCE_TYPE_ID = ION_RESOURCE_TYPES[topic_res_type_name][ID_CFG]
DATASET_RESOURCE_TYPE_ID = ION_RESOURCE_TYPES[dataset_res_type_name][ID_CFG]
IDENTITY_RESOURCE_TYPE_ID = ION_RESOURCE_TYPES[identity_res_type_name][ID_CFG]


##### Define Predicates #####:

# Predicate names:
has_a_name = 'has_a'
is_a_name = 'is_a'
type_of_name = 'type_of'
owned_by_name = 'owned_by'
# specialized predicates
topic_for_name = 'topic_for'
has_source_name = 'has_source'

# Data structure used by datastore intialization
ION_PREDICATES={
has_a_name:{ID_CFG:'C22A454D-389E-4BA6-88BC-CEDD93B5C87E'},
is_a_name:{ID_CFG:'60029609-FD0C-4DE3-8E52-9F5DDAD9A9A8'},
type_of_name:{ID_CFG:'F30A45F8-331D-4D44-AECC-746DA81B012F'},
owned_by_name:{ID_CFG:'734CE3E6-90ED-4642-AD46-7C2E67BDA798'}
}


# Extract Resource ID_CFGs for use in services and tests
HAS_A_ID = ION_PREDICATES[has_a_name][ID_CFG]
IS_A_ID = ION_PREDICATES[is_a_name][ID_CFG]
TYPE_OF_ID = ION_PREDICATES[type_of_name][ID_CFG]
OWNED_BY_ID = ION_PREDICATES[owned_by_name][ID_CFG]


##### Define Datasets and data sources #####:

# Dataset names
profile_dataset_name = 'profile_dataset'
grid_dataset_name = 'grid_dataset'


DATASET_TYPE = create_type_identifier(object_id=10001, version=1)
# Data structure used by datastore intialization
ION_DATASETS={
profile_dataset_name:{ID_CFG:'3319A67F-81F3-424F-8E69-4F28C4E047F1',
                      TYPE_CFG:DATASET_TYPE,
                      NAME_CFG:profile_dataset_name,
                      DESCRIPTION_CFG:'Some junk',
                      CONTENT_CFG:dataset_bootstrap.bootstrap_profile_dataset
                      },
#grid_dataset_name:{ID_CFG:''},
}


# Extract Resource ID_CFGs for use in services and tests
SAMPLE_PROFILE_DATASET_ID = ION_DATASETS[profile_dataset_name][ID_CFG]
#SAMPLE_GRID_DATASET_ID = ION_DATASETS[grid_dataset_name][ID_CFG]










def generate_reference_instance(proc=None, resource_id=None):
    """
    Helper method to create a resource type identifier. Uses the process work bench
    to create an ID_CFGRef object which can be used in find, get and association operations

    @param proc is a process instance which is in an active state.
    @param resource_id is a resource id defined in this file.
    """

    #@ TODO Complete this method... Should be about ten lines.
    


