#!/usr/bin/env python

"""
@file ion/res/config.py
@author David Stuebe
@TODO
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.object.object_utils import create_type_identifier

### Constants used in defining the basic configuration which is preloaded into the datastore:
ID = 'ID'
TYPE = 'TYPE'
PREDICATE = 'PREDICATE'
DESCRIPTION = 'DESCRIPTION'

### Defined Resource Types
topic_res_type_name = 'TOPIC_RESOURCE_TYPE'
dataset_res_type_name = 'DATASET_RESOURCE_TYPE'
identity_res_type_name = 'IDENTITY_RESOURCE_TYPE'

# Data structure used by datastore intialization
ION_RESOURCE_TYPES={
topic_res_type_name:{ID:'3BD84B48-073E-4833-A62B-0DE4EC106A34',
                     TYPE:create_type_identifier(object_id=10001, version=1),
                     DESCRIPTION:'Some junk'},
dataset_res_type_name:{ID:'487594C6-3D10-4DAA-A8FF-83E1E0EFB964',
                       TYPE:create_type_identifier(object_id=10002, version=1),
                       DESCRIPTION:'More junk'},
identity_res_type_name:{ID:'9C457C32-5982-4044-A3ED-6DBDB5E3EB5C',
                       TYPE:create_type_identifier(object_id=10003, version=1),
                       DESCRIPTION:'More junk'}
}

# Extract Resource IDs for use in services and tests
TOPIC_RESOURCE_TYPE_ID = ION_RESOURCE_TYPES[topic_res_type_name][ID]
DATASET_RESOURCE_TYPE_ID = ION_RESOURCE_TYPES[dataset_res_type_name][ID]
IDENTITY_RESOURCE_TYPE_ID = ION_RESOURCE_TYPES[identity_res_type_name][ID]


##### Define Predicates #####:

# Predicate names:
has_a_name = 'HAS_A'
is_a_name = 'IS_A'
type_of_name = 'TYPE_OF'
owned_by_name = 'OWNED_BY'
# specialized predicates
topic_for_name = 'TOPIC_FOR'
has_source_name = 'HAS_SOURCE'

# Data structure used by datastore intialization
ION_PREDICATES={
has_a_name:{ID:'C22A454D-389E-4BA6-88BC-CEDD93B5C87E'},
is_a_name:{ID:'60029609-FD0C-4DE3-8E52-9F5DDAD9A9A8'},
type_of_name:{ID:'F30A45F8-331D-4D44-AECC-746DA81B012F'},
owned_by_name:{ID:'734CE3E6-90ED-4642-AD46-7C2E67BDA798'}
}


# Extract Resource IDs for use in services and tests
HAS_A_ID = ION_PREDICATES[has_a_name][ID]
IS_A_ID = ION_PREDICATES[is_a_name][ID]
TYPE_OF_ID = ION_PREDICATES[type_of_name][ID]
OWNED_BY_ID = ION_PREDICATES[owned_by_name][ID]


##### Define Datasets and data sources #####:

# Dataset names
profile_dataset_name = 'PROFILE_DATASET'


# Data structure used by datastore intialization
ION_DATASETS={
profile_dataset_name:{ID:'3319A67F-81F3-424F-8E69-4F28C4E047F1',
                      },
grid_dataset_name:{},
}


# Extract Resource IDs for use in services and tests
SAMPLE_PROFILE_DATASET_ID = ION_DATASETS[profile_dataset_name][ID]
SAMPLE_GRID_DATASET_ID = ION_DATASETS[grid_dataset_name][ID]










def generate_reference_instance(proc=None, resource_type=None):
    """
    Helper method to create a resource type identifier. Uses the process work bench
    to create an IDRef object which can be used in find, get and association operations

    @param proc is a process instance which is in an active state.
    @param resource_type is a Keyword Type name defined in this file.
    """

    #@ TODO Complete this method... Should be about ten lines.
    


