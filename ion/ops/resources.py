#!/usr/bin/env python
"""
@file ion/ops/resources.py
@author David Stuebe

"""


from ion.core import ioninit
from ion.services.coi.resource_registry.resource_client import ResourceClient as RC
from ion.core.process.process import Process

from ion.services.coi.datastore_bootstrap.ion_preload_config import ROOT_USER_ID, MYOOICI_USER_ID, ANONYMOUS_USER_ID
from ion.services.coi.datastore_bootstrap.ion_preload_config import TYPE_OF_ID, HAS_LIFE_CYCLE_STATE_ID, OWNED_BY_ID, HAS_ROLE_ID, HAS_A_ID, IS_A_ID
from ion.services.coi.datastore_bootstrap.ion_preload_config import SAMPLE_PROFILE_DATASET_ID, SAMPLE_PROFILE_DATA_SOURCE_ID, ADMIN_ROLE_ID, DATA_PROVIDER_ROLE_ID, MARINE_OPERATOR_ROLE_ID, EARLY_ADOPTER_ROLE_ID, AUTHENTICATED_ROLE_ID
from ion.services.coi.datastore_bootstrap.ion_preload_config import RESOURCE_TYPE_TYPE_ID, DATASET_RESOURCE_TYPE_ID, TOPIC_RESOURCE_TYPE_ID, EXCHANGE_POINT_RES_TYPE_ID,EXCHANGE_SPACE_RES_TYPE_ID, PUBLISHER_RES_TYPE_ID, SUBSCRIBER_RES_TYPE_ID, SUBSCRIPTION_RES_TYPE_ID, DATASOURCE_RESOURCE_TYPE_ID, DISPATCHER_RESOURCE_TYPE_ID, DATARESOURCE_SCHEDULE_TYPE_ID, IDENTITY_RESOURCE_TYPE_ID

ASSOCIATION_TYPE = object_utils.create_type_identifier(object_id=13, version=1)
PREDICATE_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=25, version=1)
LCS_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=26, version=1)

from ion.services.dm.inventory.association_service import AssociationServiceClient, ASSOCIATION_QUERY_MSG_TYPE, PREDICATE_OBJECT_QUERY_TYPE, IDREF_TYPE, SUBJECT_PREDICATE_QUERY_TYPE
from ion.services.coi.resource_registry.association_client import AssociationClient


# Create a process
resource_process = Process()
resource_process.spawn()

# Create a resource client
rc = RC(resource_process)

# create an association service client
asc = AssociationServiceClient(resource_process)

# create an association client
ac = AssociationClient(resource_process)

# Set ALL for import *
__all__= ['resource_process','rc','asc','ac','ROOT_USER_ID', 'MYOOICI_USER_ID', 'ANONYMOUS_USER_ID']
__all__.extend(['TYPE_OF_ID', 'HAS_LIFE_CYCLE_STATE_ID', 'OWNED_BY_ID', 'HAS_ROLE_ID', 'HAS_A_ID', 'IS_A_ID'])
__all__.extend(['SAMPLE_PROFILE_DATASET_ID', 'SAMPLE_PROFILE_DATA_SOURCE_ID', 'ADMIN_ROLE_ID', 'DATA_PROVIDER_ROLE_ID', 'MARINE_OPERATOR_ROLE_ID', 'EARLY_ADOPTER_ROLE_ID', 'AUTHENTICATED_ROLE_ID'])
__all__.extend(['RESOURCE_TYPE_TYPE_ID', 'DATASET_RESOURCE_TYPE_ID', 'TOPIC_RESOURCE_TYPE_ID', 'EXCHANGE_POINT_RES_TYPE_ID', 'EXCHANGE_SPACE_RES_TYPE_ID', 'PUBLISHER_RES_TYPE_ID', 'SUBSCRIBER_RES_TYPE_ID', 'SUBSCRIPTION_RES_TYPE_ID', 'DATASOURCE_RESOURCE_TYPE_ID', 'DISPATCHER_RESOURCE_TYPE_ID', 'DATARESOURCE_SCHEDULE_TYPE_ID', 'IDENTITY_RESOURCE_TYPE_ID'])
__all__.extend(['ASSOCIATION_TYPE','PREDICATE_REFERENCE_TYPE','LCS_REFERENCE_TYPE','ASSOCIATION_QUERY_MSG_TYPE', 'PREDICATE_OBJECT_QUERY_TYPE', 'IDREF_TYPE', 'SUBJECT_PREDICATE_QUERY_TYPE'])





