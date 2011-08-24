#!/usr/bin/env python
"""
@file ion/ops/resources.py
@author David Stuebe

"""

import time
from ion.core import ioninit
from ion.core.object.object_utils import sha1_to_hex
from ion.services.coi.datastore import CDM_BOUNDED_ARRAY_TYPE
from ion.services.coi.resource_registry.resource_client import ResourceClient as RC
from ion.core.object import object_utils
from ion.core.process.process import Process

from ion.services.coi.datastore_bootstrap.ion_preload_config import ROOT_USER_ID, MYOOICI_USER_ID, ANONYMOUS_USER_ID
from ion.services.coi.datastore_bootstrap.ion_preload_config import TYPE_OF_ID, HAS_LIFE_CYCLE_STATE_ID, OWNED_BY_ID, HAS_ROLE_ID, HAS_A_ID, IS_A_ID
from ion.services.coi.datastore_bootstrap.ion_preload_config import SAMPLE_PROFILE_DATASET_ID, SAMPLE_PROFILE_DATA_SOURCE_ID, ADMIN_ROLE_ID, DATA_PROVIDER_ROLE_ID, MARINE_OPERATOR_ROLE_ID, EARLY_ADOPTER_ROLE_ID, AUTHENTICATED_ROLE_ID
from ion.services.coi.datastore_bootstrap.ion_preload_config import RESOURCE_TYPE_TYPE_ID, DATASET_RESOURCE_TYPE_ID, TOPIC_RESOURCE_TYPE_ID, EXCHANGE_POINT_RES_TYPE_ID,EXCHANGE_SPACE_RES_TYPE_ID, PUBLISHER_RES_TYPE_ID, SUBSCRIBER_RES_TYPE_ID, SUBSCRIPTION_RES_TYPE_ID, DATASOURCE_RESOURCE_TYPE_ID, DISPATCHER_RESOURCE_TYPE_ID, DATARESOURCE_SCHEDULE_TYPE_ID, IDENTITY_RESOURCE_TYPE_ID

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

ASSOCIATION_TYPE = object_utils.create_type_identifier(object_id=13, version=1)
PREDICATE_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=25, version=1)
LCS_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=26, version=1)

from ion.services.dm.inventory.association_service import AssociationServiceClient, ASSOCIATION_QUERY_MSG_TYPE, PREDICATE_OBJECT_QUERY_TYPE, IDREF_TYPE, SUBJECT_PREDICATE_QUERY_TYPE
from ion.services.coi.resource_registry.association_client import AssociationClient


from ion.services.coi.identity_registry import IdentityRegistryClient, RESOURCE_CFG_REQUEST_TYPE

# Create a process
resource_process = Process()
resource_process.spawn()

# Create a resource client
rc = RC(resource_process)

# Create an association service client
asc = AssociationServiceClient(resource_process)

# Create an association client
ac = AssociationClient(resource_process)

# Capture the message client
mc = resource_process.message_client

irc = IdentityRegistryClient(resource_process)

# Set ALL for import *
__all__= ['resource_process','rc','asc','ac','mc','ROOT_USER_ID', 'MYOOICI_USER_ID', 'ANONYMOUS_USER_ID']
__all__.extend(['TYPE_OF_ID', 'HAS_LIFE_CYCLE_STATE_ID', 'OWNED_BY_ID', 'HAS_ROLE_ID', 'HAS_A_ID', 'IS_A_ID'])
__all__.extend(['SAMPLE_PROFILE_DATASET_ID', 'SAMPLE_PROFILE_DATA_SOURCE_ID', 'ADMIN_ROLE_ID', 'DATA_PROVIDER_ROLE_ID', 'MARINE_OPERATOR_ROLE_ID', 'EARLY_ADOPTER_ROLE_ID', 'AUTHENTICATED_ROLE_ID'])
__all__.extend(['RESOURCE_TYPE_TYPE_ID', 'DATASET_RESOURCE_TYPE_ID', 'TOPIC_RESOURCE_TYPE_ID', 'EXCHANGE_POINT_RES_TYPE_ID', 'EXCHANGE_SPACE_RES_TYPE_ID', 'PUBLISHER_RES_TYPE_ID', 'SUBSCRIBER_RES_TYPE_ID', 'SUBSCRIPTION_RES_TYPE_ID', 'DATASOURCE_RESOURCE_TYPE_ID', 'DISPATCHER_RESOURCE_TYPE_ID', 'DATARESOURCE_SCHEDULE_TYPE_ID', 'IDENTITY_RESOURCE_TYPE_ID'])
__all__.extend(['ASSOCIATION_TYPE','PREDICATE_REFERENCE_TYPE','LCS_REFERENCE_TYPE','ASSOCIATION_QUERY_MSG_TYPE', 'PREDICATE_OBJECT_QUERY_TYPE', 'IDREF_TYPE', 'SUBJECT_PREDICATE_QUERY_TYPE'])
__all__.extend(['find_resource_keys','find_dataset_keys','find_datasets','pprint_datasets','clear', 'print_dataset_history','update_identity_subject','get_identities_by_subject', '_checkout_all'])





@defer.inlineCallbacks
def find_resource_keys(resource_type, lifecycle_state=None):
    """
    @Brief: Uses the associations service to gather a list of IDs of all the resources with
            the given resource_type and lifecycle_state.
    @param resource_type: a string specifying the ResourceTypeID of the resource to find
    @param lifecycle_state: an int value of a lifecycle state as from the LifeCycleState enum
                            embedded in MessageInstance_Wrapper objects.  If lifecycle_state is
                            None it will not be used in the query.
    
    @return: A list containing the ID reference keys of the resources found.  If nothing is
             found, an empty list is returned
    """
    query = yield mc.create_instance(PREDICATE_OBJECT_QUERY_TYPE)

    pair = query.pairs.add()

    # Set the predicate search term
    pref = query.CreateObject(PREDICATE_REFERENCE_TYPE)
    pref.key = TYPE_OF_ID

    pair.predicate = pref

    # Set the Object search term

    type_ref = query.CreateObject(IDREF_TYPE)
    type_ref.key = resource_type

    pair.object = type_ref

    ### Check the type of the configuration request
    if lifecycle_state is not None:

        # Add a life cycle state request
        pair = query.pairs.add()

        # Set the predicate search term
        pref = query.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = HAS_LIFE_CYCLE_STATE_ID

        pair.predicate = pref


        # Set the Object search term
        state_ref = query.CreateObject(LCS_REFERENCE_TYPE)
        state_ref.lcs = lifecycle_state
        pair.object = state_ref


    result = yield asc.get_subjects(query)

    defer.returnValue(result.idrefs[:])


@defer.inlineCallbacks
def find_dataset_keys(lifecycle_state=None):
    """
    Uses the associations framework to grab the ID reference keys of all available datasets with
    the given lifecycle_state.
    @param lifecycle_state: an int value of a lifecycle state as from the LifeCycleState enum
                            embedded in MessageInstance_Wrapper objects.  If lifecycle_state is
                            None it will not be used in the query.
                            
    @return: A list containing the currently available dataset keys as string or unicode objects.
             If nothing is found, an empty list is returned
    """
    result = []
    idrefs = yield find_resource_keys(DATASET_RESOURCE_TYPE_ID, lifecycle_state)
    
    if len(idrefs) > 0:
        for idref in idrefs:
            result.append(idref.key)
            
        # Add a line return and print each key on its own line encoded in utf-8 format
        log.info('\n\n\t%s' % '\n\t'.join(result).encode('utf-8'))
        
        
    defer.returnValue(result)


@defer.inlineCallbacks
def find_datasets(lifecycle_state=None):
    """
    Uses the associations framework to grab the ID reference keys of all available datasets with
    the given lifecycle_state and then uses a resource_client to obtain the resource objects for
    those keys.
    @param lifecycle_state: the int value of a lifecycle state as from the LifeCycleState enum
                            embedded in MessageInstance_Wrapper objects.  If lifecycle_state is
                            None it will not be used in the query.
                            
    @return: A dictionary mapping dataset resource keys (ids) to their dataset resource objects.
             If nothing is found, an empty dictionary is returned
    """
    result = {}
    idrefs = yield find_resource_keys(DATASET_RESOURCE_TYPE_ID, lifecycle_state)
    
    if len(idrefs) > 0:
        for idref in idrefs:
            dataset = yield rc.get_instance(idref)
            result[idref.key] = dataset
                
                
    defer.returnValue(result)


@defer.inlineCallbacks
def pprint_datasets(dataset_dict=None):
    """
    @param dataset_dict: a dictionary mapping dataset resource keys (ids) to their dataset resource objects.
                         If the dictionary is None, find_datasets() will be called to populate it
    @return: a defered containing a pretty-formatted output string
    """
    
    if dataset_dict is None:
        dataset_dict = yield find_datasets()
    
    # Add a header
    output = [' ']
    for i in range(182):
        output.append('-')
    output.append('\n |%s|%s|%s|%s|\n ' % ('Resource Key (lifecycle state)'.center(59), 'Dataset Title'.center(60), 'Variable List'.center(30), 'Variable Dimensions'.center(28)))
    for i in range(182):
        output.append('-')
    output.append('\n')
    
    # Iterate over each dataset in the list..
    for key, dataset in dataset_dict.items():
        # Get some info
        title = dataset.root_group.FindAttributeByName('title').GetValue()
        state = dataset.ResourceLifeCycleState
        vrbls = [(var.name, [(dim.name, dim.length) for dim in var.shape]) for var in dataset.root_group.variables]
        
        # Truncate title if its too long
        if len(title) > 58:
            title = '%s...' % title[:55]
        
        # Add the dataset key and title to the output
        key     = '"%s" (%s)' % (key.encode('utf-8'), state.encode('utf-8'))
        title   = '"%s"' % title.encode('utf-8')
        output.append(' %-60s %-60s ' % (key, title))
        for var_name, shape in vrbls:
            
            # Truncate title if its too long
            if len(var_name) > 30:
                var_name = '%s...' % var_name[:27]
                
            # Add each variables name for this dataset to the output
            output.append('%-30s ' % var_name.encode('utf-8'))
            for dim_name, dim_length in shape:
                
                # Add information about the variables dimensions to the output
                output.append('%s(0:%i) ' % (dim_name.encode('utf-8'), dim_length - 1))
                
            # Add necessary whitespace to display the next variable
            output.append('\n%-122s ' % (''))
        
        # Adjust spacing for the next dataset
        output[-1] = '\n\n'
        
        
    soutput = ''.join(output)
    del output
    defer.returnValue(soutput)


def clear(lines=100):
    """
    Attempts to clear the interactive python console by printing line breaks.
    @param lines: The number of lines to print to the console (default=100)
    """
    print ''.join( ['\n' for i in range(lines)] )

@defer.inlineCallbacks
def print_dataset_history(dsid):
    dataset = yield rc.get_instance(dsid, excluded_types=[CDM_BOUNDED_ARRAY_TYPE])
    repo = dataset.Repository

    outlines = []

    # get all parent commits, similar to list_parent_commits but not just keys
    commits = []
    branch = repo._current_branch
    cref = branch.commitrefs[0]

    while cref:
        commits.append(cref)

        if cref.parentrefs:
            cref = cref.parentrefs[0].commitref
        else:
            cref = None

    # parent -> child ordering
    commits.reverse()

    outlines.append('========= Dataset History: ==========')
    outlines.append('= Dataset ID: %s' % repo.repository_key)
    outlines.append('= Dataset Branch: %s' % repo.current_branch_key())

    for i, c in enumerate(commits):
        outlines.append("%d\t%s\t%s\t%s" % (i+1, time.strftime("%d %b, %H:%M:%S", time.gmtime(c.date)), sha1_to_hex(c.MyId), c.comment))
        links = []
        try:
            for var in c.objectroot.resource_object.root_group.variables:
                links.extend(var.content.bounded_arrays.GetLinks())

            # get em
            yield repo.fetch_links(links)

            for var in c.objectroot.resource_object.root_group.variables:
                outsublines = []

                for ba in var.content.bounded_arrays:
                    outsublines.append("%s%s\t%s" % (" "*40, sha1_to_hex(ba.MyId)[0:6] + "...", " ".join(["[%s+%s]" % (x.origin, x.size) for x in ba.bounds])))

                varname = " "*4 + str(var.name)
                if len(outsublines) > 1:
                    varname += " (%d)" % len(outsublines)

                outsublines[0] = varname + outsublines[0][len(varname):]

                outlines.append("\n".join(outsublines))

        except:# Exception, ex:
            pass
            #print ex

    outlines.append('=====================================')
    defer.returnValue("\n".join(outlines))



@defer.inlineCallbacks
def update_identity_subject(old_subject, new_subject):

    if old_subject == new_subject:
        raise RuntimeError('The old CI Login subject must be different than the new one')

    # get all the identity resources out of the Association Service
    request = yield mc.create_instance(PREDICATE_OBJECT_QUERY_TYPE)
    pair = request.pairs.add()

    # Set the predicate search term
    pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
    pref.key = TYPE_OF_ID
    pair.predicate = pref

    # Set the Object search term
    type_ref = request.CreateObject(IDREF_TYPE)
    type_ref.key = IDENTITY_RESOURCE_TYPE_ID
    pair.object = type_ref

    ooi_id_list = yield asc.get_subjects(request)

    # Now we have a list of ooi_ids. Gotta pull and search them individually.
    old_id = None
    new_id = None
    for ooi_id in ooi_id_list.idrefs:
        id_res = yield rc.get_instance(ooi_id)
        if old_subject == id_res.subject:
            old_id = id_res

        if new_subject == id_res.subject:
             new_id = id_res

        if old_id is not None and new_id is not None:
            break

    else:
        if old_id is None:
            raise RuntimeError('No identity resource found with the specified original subject "%s"' % old_subject)


    old_id.subject = new_subject

    resources=[old_id]
    if new_id is not None:
        new_id.ResourceLifeCycleState = new_id.RETIRED
        new_id.subject = 'Junk - identity provider changed the subject. This is a bogus ID!'
        resources.append(new_id)

    yield rc.put_resource_transaction(resources)

    # Do we need to set roles? It is not a new UUID - it is a new subject!
    #old_roles = yield irc.get_roles(old_uuid)
    #op_unset_role(old_uuid)
    #op_set_role(new_uuid, old_roles)

    defer.returnValue('Success!')





@defer.inlineCallbacks
def get_identities_by_subject(subject):


    # get all the identity resources out of the Association Service
    request = yield mc.create_instance(PREDICATE_OBJECT_QUERY_TYPE)
    pair = request.pairs.add()

    # Set the predicate search term
    pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
    pref.key = TYPE_OF_ID
    pair.predicate = pref

    # Set the Object search term
    type_ref = request.CreateObject(IDREF_TYPE)
    type_ref.key = IDENTITY_RESOURCE_TYPE_ID
    pair.object = type_ref

    ooi_id_list = yield asc.get_subjects(request)

    res = []
    for ooi_id in ooi_id_list.idrefs:
        id_res = yield rc.get_instance(ooi_id)

        if id_res.subject == subject:
            res.append(id_res)

    defer.returnValue(id_res)

@defer.inlineCallbacks
def _checkout_all(arr):
    goodlist = []
    badlist = []
    for id in arr:
        id = str(id)
        log.warn("Getting id %s" % id)

        try:
            yield rc.get_instance(id)
            log.warn("... ok")
            goodlist.append(id)
        except:
            log.warn("... bad")
            badlist.append(id)
    defer.returnValue((goodlist, badlist))