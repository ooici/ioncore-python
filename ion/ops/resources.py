#!/usr/bin/env python
"""
@file ion/ops/resources.py
@author David Stuebe

"""
import tempfile

import time
from ion.core import ioninit
from ion.core.data.storage_configuration_utility import RESOURCE_OBJECT_TYPE
from ion.core.object.gpb_wrapper import WrappedMessageProperty, WrappedRepeatedCompositeProperty, WrappedRepeatedScalarProperty
from ion.core.object.object_utils import sha1_to_hex
from ion.services.coi.datastore import CDM_BOUNDED_ARRAY_TYPE
from ion.services.coi.resource_registry.resource_client import ResourceClient as RC
from ion.core.object import object_utils, workbench, repository, gpb_wrapper
from ion.core.process.process import Process
import os, os.path

from ion.services.coi.datastore_bootstrap.ion_preload_config import ROOT_USER_ID, MYOOICI_USER_ID, ANONYMOUS_USER_ID, TypeIDMap, PredicateMap
from ion.services.coi.datastore_bootstrap.ion_preload_config import TYPE_OF_ID, HAS_LIFE_CYCLE_STATE_ID, OWNED_BY_ID, HAS_ROLE_ID, HAS_A_ID, IS_A_ID
from ion.services.coi.datastore_bootstrap.ion_preload_config import SAMPLE_PROFILE_DATASET_ID, SAMPLE_PROFILE_DATA_SOURCE_ID, ADMIN_ROLE_ID, DATA_PROVIDER_ROLE_ID, MARINE_OPERATOR_ROLE_ID, EARLY_ADOPTER_ROLE_ID, AUTHENTICATED_ROLE_ID
from ion.services.coi.datastore_bootstrap.ion_preload_config import RESOURCE_TYPE_TYPE_ID, DATASET_RESOURCE_TYPE_ID, TOPIC_RESOURCE_TYPE_ID, EXCHANGE_POINT_RES_TYPE_ID,EXCHANGE_SPACE_RES_TYPE_ID, PUBLISHER_RES_TYPE_ID, SUBSCRIBER_RES_TYPE_ID, SUBSCRIPTION_RES_TYPE_ID, DATASOURCE_RESOURCE_TYPE_ID, DISPATCHER_RESOURCE_TYPE_ID, DATARESOURCE_SCHEDULE_TYPE_ID, IDENTITY_RESOURCE_TYPE_ID

from ion.services.coi.datastore import DataStoreService, DataStoreWorkbench, DataStoreWorkBenchError, BLOB_CACHE, COMMIT_CACHE, Query, REPOSITORY_KEY, MUTABLE_TYPE, BRANCH_NAME, VALUE
from ion.core.data.cassandra_bootstrap import CassandraIndexedStoreBootstrap, CassandraStoreBootstrap


import ion.util.ionlog
from ion.util.os_process import OSProcess

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

type_id_map = TypeIDMap()
predicate_map = PredicateMap()

# Set ALL for import *
__all__= ['resource_process','rc','asc','ac','mc','ROOT_USER_ID', 'MYOOICI_USER_ID', 'ANONYMOUS_USER_ID','predicate_map', 'type_id_map']
__all__.extend(['TYPE_OF_ID', 'HAS_LIFE_CYCLE_STATE_ID', 'OWNED_BY_ID', 'HAS_ROLE_ID', 'HAS_A_ID', 'IS_A_ID'])
__all__.extend(['SAMPLE_PROFILE_DATASET_ID', 'SAMPLE_PROFILE_DATA_SOURCE_ID', 'ADMIN_ROLE_ID', 'DATA_PROVIDER_ROLE_ID', 'MARINE_OPERATOR_ROLE_ID', 'EARLY_ADOPTER_ROLE_ID', 'AUTHENTICATED_ROLE_ID'])
__all__.extend(['RESOURCE_TYPE_TYPE_ID', 'DATASET_RESOURCE_TYPE_ID', 'TOPIC_RESOURCE_TYPE_ID', 'EXCHANGE_POINT_RES_TYPE_ID', 'EXCHANGE_SPACE_RES_TYPE_ID', 'PUBLISHER_RES_TYPE_ID', 'SUBSCRIBER_RES_TYPE_ID', 'SUBSCRIPTION_RES_TYPE_ID', 'DATASOURCE_RESOURCE_TYPE_ID', 'DISPATCHER_RESOURCE_TYPE_ID', 'DATARESOURCE_SCHEDULE_TYPE_ID', 'IDENTITY_RESOURCE_TYPE_ID'])
__all__.extend(['ASSOCIATION_TYPE','PREDICATE_REFERENCE_TYPE','LCS_REFERENCE_TYPE','ASSOCIATION_QUERY_MSG_TYPE', 'PREDICATE_OBJECT_QUERY_TYPE', 'IDREF_TYPE', 'SUBJECT_PREDICATE_QUERY_TYPE'])
__all__.extend(['find_resource_keys','find_dataset_keys','find_datasets','find_broken_datasets','pprint_datasets','clear', 'print_dataset_history','update_identity_subject','get_identities_by_subject', '_checkout_all','print_dataset_time'])

# graphviz related
__all__.extend(['_gv_resource_commits', '_graph', 'graph_resource_commits', '_gv_resource_associations', 'graph_resource_associations', '_gv_resource', 'graph_resource'])

# these are handy...
__all__.extend(['time','sha1_to_hex'])

# Data Store Repair
__all__.extend(['cassandra_repair_shop',])


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
            try:
                dataset = yield rc.get_instance(idref)
                result[idref.key] = dataset
            except Exception, ex:
                log.exception('The dataset %s could not be retrieved!' % str(idref))
                #result[idref.key] = ex

    defer.returnValue(result)

@defer.inlineCallbacks
def find_broken_datasets(lifecycle_state=None):
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
            try:
                dataset = yield rc.get_instance(idref)
                #result[idref.key] = dataset
            except Exception, ex:
                result[idref.key] = ex

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
        try:
            title = dataset.root_group.FindAttributeByName('title').GetValue()
        except:
            title = "(no title found)"
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
def print_dataset_time(dsid):
    dataset = yield rc.get_instance(dsid, excluded_types=[])
    rg = dataset.root_group
    t = rg.FindVariableByName('time')

    defer.returnValue(t.PPrint())



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
            try:
                cref = cref.parentrefs[0].commitref
            except KeyError:
                log.info('Commit history was truncated!')
                cref = None
        else:
            cref = None

    try:
        title = commits[0].objectroot.resource_object.root_group.FindAttributeByName('title').GetValue()
    except:
        title = "(no title found)"

    # parent -> child ordering
    commits.reverse()

    outlines.append('========= Dataset History: ==========')
    outlines.append('= Dataset ID: %s' % repo.repository_key)
    outlines.append('= Dataset Title: %s' % title)
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


class GraphvizEntry(object):
    """
    A graphviz graph entry.

    Can represent edges and nodes. Will truncate values if max_value_len is specified.
    Attributes on the nodes can be set via accessors, aka:

        ge = GraphvisEntry("source_name")
        ge['label'] = "This is the source"
        ge['color'] = "blue"

    Use str() to automatically transform this entry into a graphviz language line (without any indent).
    Note that it will quote everything, so you cannot use it to set defaults on things like node - use
    the GraphvizGraph's preamble parameter for that.
    """
    def __init__(self, src=None, dest=None, attrs=None, max_value_len=0):
        self._attrs = attrs or {}
        self.src = src
        self.dest = dest
        self._max_value_len = max_value_len

    def __getitem__(self, item):
        return self._attrs[item]

    def __setitem__(self, key, value):
        self._attrs[key] = value

    def __str__(self):
        out = "\"%s\"" % self.src
        if self.dest:
            out += " -> \"%s\"" % self.dest

        attrs = map(lambda x: "%s=\"%s\"" % (x[0], self._sanitize(x[1])), self._attrs.iteritems())
        if len(attrs):
            out += " [%s]" % ",".join(attrs)
            
        out += ";"
        return out

    def _sanitize(self, mystring):
        if self._max_value_len > 0 and len(mystring) > self._max_value_len:
            mystring = "%s... (trunc)" % mystring[0:self._max_value_len]
        return mystring

class GraphvizGraph(list):
    """
    Represents a Graphviz graph.

    This is a simple inheritance from list, and expects to contain GraphvizEntry objects. When turning
    this object into a string, it builds a full graphviz input of itself and any contained objects.
    It also sets some defaults for nodes via the DEFAULT_PREAMBLE - you can override this in the
    initializer.
    """
    DEFAULT_PREAMBLE = '\tnode [shape="box", style="filled, rounded", fillcolor="#E7E9E8"];'

    def __init__(self, name, entries=None, preamble=None):
        self._name = name
        # default - shape like boxes please
        self._preamble = preamble or self.DEFAULT_PREAMBLE
        entries = entries or ()
        list.__init__(self, entries)

    def __str__(self):
        outlines = []
        outlines.append("digraph \"%s\" {" % self._name)
        outlines.append(self._preamble)
        outlines.extend(map(lambda x: "\t%s" % str(x), self))
        outlines.append("}")
        return "\n".join(outlines)

@defer.inlineCallbacks
def _graph(callable, ident, kwargs):

    call_args = kwargs.copy()
    mode = call_args.pop('mode','svg')

    ident = str(ident)
    gvinp = yield defer.maybeDeferred(callable, ident, **call_args)

    (inp, inpfile) = tempfile.mkstemp(suffix='.txt', prefix="%s-" % ident)
    os.write(inp, gvinp)
    os.close(inp)
    gt = time.time()

    outimg = os.path.join(tempfile.gettempdir(), "%s-%s.%s" % (ident, str(gt), mode))

    dot = OSProcess(binary="dot", spawnargs=["-T%s" % mode, "-o%s" % outimg, inpfile])
    yield dot.spawn()

    viewit = OSProcess(binary="open", spawnargs=[outimg])
    yield viewit.spawn()

    log.warn("Graph Input: %s" % inpfile)
    log.warn("graph Output: %s" % outimg)

@defer.inlineCallbacks
def _gv_resource_associations(res_id, show_assoc_ids=False):
    res = yield rc.get_instance(res_id)
    sbj_mngr = yield ac.find_associations(subject=res)
    obj_mngr = yield ac.find_associations(obj=res)

    obj_list = []
    for assoc in sbj_mngr:
        r = yield rc.get_instance(assoc.ObjectReference.key)
        obj_list.append(r)

    sub_list=[]
    for assoc in obj_mngr:
        r = yield rc.get_instance(assoc.SubjectReference.key)
        sub_list.append(r)

    rlist = [res,]
    rlist.extend(sub_list)
    rlist.extend(obj_list)

    g=GraphvizGraph("associations-%s" % res_id)

    def sanatize_string(mystring, maxlen=38):
        if len(mystring) > maxlen and "\n" in mystring:
            mystring = "%s; (Truncated!!)" % (mystring[0:maxlen].split('\n')[0],)

        elif len(mystring) > maxlen:
            mystring = "%s; (Truncated length)" % (mystring[0:maxlen],)

        elif "\n" in mystring:
            mystring = "%s; (Truncated newline)" % (mystring.split('\n')[0],)

        return mystring

    for ind, r in enumerate(rlist):
        resource_lines = []

        ro = r.ResourceObject

        for pname, pvalue in ro._Properties.iteritems():

            value = getattr(ro, pname)
            if pvalue.field_type == 'TYPE_MESSAGE' and value is not None:

                r1 = value
                if hasattr(r1,'__iter__'):
                    resource_lines.append("%s (repeated field length): %d" % (pname, len(r1)))
                else:
                    resource_lines.append("%s::" % pname)
                    for pname1, pvalue1 in r1._Properties.iteritems():
                        if pvalue1.field_type != 'TYPE_MESSAGE':
                            svalue1 = str(getattr(r1, pname1))

                            resource_lines.append("::%s: '%s'" % (pname1, sanatize_string(svalue1)))
                        else:
                            resource_lines.append("::%s - nested field skipped" % (pname1,))

            else:
                svalue = str(value)
                resource_lines.append("%s: '%s'" % (pname, sanatize_string(svalue)))

        rid = r.ResourceIdentity
        lbl = "KEY: %s\\nName: %s\\nType: %s\\nLCState: %s\\nResource Properties:\\n%s" % (rid, r.ResourceName, type_id_map[r.ResourceTypeID.key], r.ResourceLifeCycleState, "\\n".join(resource_lines))
        attrs = {'label':lbl}
        if ind == 0:
            attrs['fillcolor'] = '#ffaaaa'

        g.append(GraphvizEntry(rid, None, attrs))

    edgestyle = {'style':'bold', 'color':'#a40000', 'fontcolor':'#a40000'}

    for pred, assoc_set in sbj_mngr.iteritems():

        predicate = predicate_map.get(pred)

        for assoc in assoc_set:
            lbl = str(predicate)
            if show_assoc_ids:
                lbl += "\\n%s" % assoc.AssociationIdentity
            attrs = {'label':lbl }
            attrs.update(edgestyle)
            g.append(GraphvizEntry(res_id, assoc.ObjectReference.key, attrs))

    for pred, assoc_set in obj_mngr.iteritems():
        predicate = predicate_map.get(pred)

        for assoc in assoc_set:
            lbl = str(predicate)
            if show_assoc_ids:
                lbl += "\\n%s" % assoc.AssociationIdentity
            attrs = {'label':lbl }
            attrs.update(edgestyle)
            g.append(GraphvizEntry(assoc.SubjectReference.key, res_id, attrs))

    defer.returnValue(str(g))

@defer.inlineCallbacks
def graph_resource_associations(res_id, mode="svg", show_assoc_ids=False):
    yield _graph(_gv_resource_associations, res_id, {'mode':mode,'show_assoc_ids':show_assoc_ids} )

@defer.inlineCallbacks
def _gv_resource_commits(rid, get_instance_callable=None):

    get_instance_callable = get_instance_callable or rc.get_instance
    res = yield defer.maybeDeferred(get_instance_callable, rid)

    g = GraphvizGraph("commits-%s" % rid)


    def get_commit_chain(repo):

        for cref in repo._commit_index.values():
            crefkey = sha1_to_hex(cref.MyId)
            tm = time.gmtime(cref.date)
            dat = time.strftime("%m/%d %H:%M:%S",tm)
            lbl = "KEY: %s\\nCOMMENT: %s\\nDATE: %s" % (crefkey, str(cref.comment), dat)


            fc = '#FFFFAA' # Default is No Parent - that is bad
            for link in cref.ParentLinks:
                fc = '#dddddd' # If there are any parent links it is not a head... unless

                if link.Root.ObjectType != cref.ObjectType:
                    fc = '#ffaaaa' # it has a ref from the .git object
                    break

            g.append(GraphvizEntry(crefkey, None, {"label":lbl, 'fillcolor': fc}))

            for idx, x in enumerate(cref.parentrefs):
                try:
                    pcref = x.commitref
                    pcrefkey = sha1_to_hex(pcref.MyId)
                    ge = GraphvizEntry(crefkey, pcrefkey, {'taillabel':str(idx)})
                except KeyError, ke:
                    log.warn('Commit not found or truncated')
                    link = x.GetLink('commitref')
                    key = sha1_to_hex(link.key)
                    ge = GraphvizEntry(crefkey, key, {'taillabel':str(idx),'fillcolor':'#ffaaaa'})

                g.append(ge)


                

    repo = res.Repository # this works for a repository or a resource
    get_commit_chain(repo)

    defer.returnValue(str(g))

@defer.inlineCallbacks
def graph_resource_commits(rid, mode="svg", get_instance_callable=None):
    yield _graph(_gv_resource_commits, rid, {'mode':mode,'get_instance_callable':get_instance_callable})

@defer.inlineCallbacks
def _gv_resource(rid, get_instance_callable=None):

    get_instance_callable = get_instance_callable or rc.get_instance
    res = yield defer.maybeDeferred(get_instance_callable, rid)
    #res = yield rc.get_instance(rid)
    g = GraphvizGraph("resource-%s" % rid)

    rwo = res.ResourceObject
    g.append(GraphvizEntry(rid, None, {'label':'ID: %s\\nType: %s' % (rid, rwo.ObjectClass)}))

    def pprint(wo, parentnode=None):
        try:
            for name, field in wo._Properties.iteritems():
                try:
                    field_val = field.__get__(wo)
                except KeyError, ke:
                    g.append(GraphvizEntry(name, None, {'label':'Error during get field (%s)' % name}))
                    continue

                # fullname is the new parent node we pass into things - fully qualified
                if parentnode:
                    fullname = "%s/%s" % (parentnode, name)
                else:
                    fullname = "%s/%s" % (rid, name)

                if isinstance(field, WrappedMessageProperty):
                    try:
                        pprint(field_val, fullname)
                        lbl = "WrappedMessageProperty: %s" % name

                    except AttributeError, ae:
                        log.debug("Unset CasRef Field Name: %s: catching attribute error: %s" % (name, ae))
                    except Exception, ex:
                        log.exception("unexpected state in a wrapped message property")
                        continue

                elif isinstance(field, WrappedRepeatedCompositeProperty):
                    try:
                        length = len(field_val)
                        lbl = "%s (length: %d)" % (name, length)
                        for i in range(length):
                            try:
                                # make a new node for this item
                                nodename = "%s/comp-%d" % (fullname, i)
                                nodelbl = "[%d] Name: %s" % (i, field_val[i].name)
                                g.append(GraphvizEntry(nodename, None, {'label':nodelbl}))
                                g.append(GraphvizEntry(fullname, nodename))

                                pprint(field_val[i], nodename)

                            except AttributeError, ex:
                                log.exception("Attribute Error in RepeatedComposite")
                            except KeyError, ex:
                                log.exception("Key Error in RepeatedComposite")
                            except Exception, ex:
                                log.exception("Unknown excpetion in RepeatedComposite")

                    except Exception, ex:
                        log.exception('Unexpected state in a WrappedRepeatedCompositeProperty.')
                        continue

                elif isinstance(field, WrappedRepeatedScalarProperty):
                    scalars = field_val
                    lbl = "Scalars (length: %d)" % len(scalars)
                    for i, scalval in enumerate(scalars[0:20]):
                        nodename = "%s/scalar-%d" % (fullname, i)
                        scallbl = str(scalval).replace('\n', '\\n')
                        g.append(GraphvizEntry(nodename, None, {'label':scallbl}))
                        g.append(GraphvizEntry(fullname, nodename))
                    if len(scalars) > 20:
                        lbl += " (truncated)"

                else:
                    item = field_val
                    if field.field_type == 'TYPE_ENUM':
                        item = field.field_enum.lookup.get(item, 'Invalid Enum Value!')

                    lbl = "Name: %s\\nValue: '%s'" % (name, str(item))

                # make node/connection
                g.append(GraphvizEntry(fullname, None, {'label':lbl}))

                if parentnode:
                    g.append(GraphvizEntry(parentnode, fullname))
                else:
                    g.append(GraphvizEntry(rid, fullname))

        except Exception, ex:
            log.exception("well, something went wrong here (wo type %s)" % str(wo.__class__))
            pass

    pprint(rwo)

    defer.returnValue(str(g))

@defer.inlineCallbacks
def graph_resource(rid, mode="svg", get_instance_callable=None):
    yield _graph(_gv_resource, rid, {'mode':mode,'get_instance_callable':get_instance_callable})


class RepairBench(DataStoreWorkbench):



    def __init__(self, blob_store, commit_store, cache_size=10**8):

        DataStoreWorkbench.__init__(self, None, blob_store, commit_store, cache_size)

    @defer.inlineCallbacks
    def find_resource_keys(self):

        resource_types=[RESOURCE_TYPE_TYPE_ID, DATASET_RESOURCE_TYPE_ID, TOPIC_RESOURCE_TYPE_ID, EXCHANGE_POINT_RES_TYPE_ID,EXCHANGE_SPACE_RES_TYPE_ID, PUBLISHER_RES_TYPE_ID, SUBSCRIBER_RES_TYPE_ID, SUBSCRIPTION_RES_TYPE_ID, DATASOURCE_RESOURCE_TYPE_ID, DISPATCHER_RESOURCE_TYPE_ID, DATARESOURCE_SCHEDULE_TYPE_ID, IDENTITY_RESOURCE_TYPE_ID]
        resource_type_names=['RESOURCE_TYPE', 'DATASET_RESOURCE_TYPE', 'TOPIC_RESOURCE_TYPE', 'EXCHANGE_POINT_RES_TYPE', 'EXCHANGE_SPACE_RES_TYPE', 'PUBLISHER_RES_TYPE', 'SUBSCRIBER_RES_TYPE', 'SUBSCRIPTION_RES_TYPE', 'DATASOURCE_RESOURCE_TYPE', 'DISPATCHER_RESOURCE_TYPE', 'DATARESOURCE_SCHEDULE_TYPE', 'IDENTITY_RESOURCE_TYPE']

        keys_by_type={}

        for type,type_name in zip(resource_types, resource_type_names):

            q = Query()
            q.add_predicate_eq(RESOURCE_OBJECT_TYPE,type)
            q.add_predicate_gt(BRANCH_NAME, '')

            rows = yield self._commit_store.query(q)

            keys=set()
            for row in rows.itervalues():
                keys.add(row[REPOSITORY_KEY])

            keys_by_type[type_name] = keys

        defer.returnValue(keys_by_type)



    @defer.inlineCallbacks
    def graph_commits(self,repo):

        yield _graph(_gv_resource_commits, repo.repository_key, {'mode':'svg','get_instance_callable':self.get_repository})



    @defer.inlineCallbacks
    def find_broken_repos(self, keys_by_type):

        broken_by_type={}
        for type_name, keyset in keys_by_type.iteritems():

            repos = set()
            for key in keyset:
                repo = yield self.read_repo_state(key)


                if len(repo.orphaned_crefs) is 0:
                    log.warn('Repository %s appears to be okay!' % repo.repository_key)
                    self.clear_repository(repo)

                else:

                    log.warn('Repository %s appears to be broken!' % repo.repository_key)
                    repos.add(repo)

            if repos:
                broken_by_type[type_name] = repos

        defer.returnValue(broken_by_type)




    @defer.inlineCallbacks
    def read_repo_state(self, repository_key):
        """
        @returns Repo.
        """

        log.info('read_repo_state: start')

        repo = self.get_repository(repository_key)
        if repo is None:
            #if it does not exist make a new one
            log.debug('Repository is not loaded - get it from the persistent store')

            repo = repository.Repository(repository_key=repository_key)
            self.put_repository(repo)
        else:
            raise RuntimeError('read_repo_state should only be run once on a repo that is not yet loaded. Clear it from the repair bench first...')

        q = Query()
        q.add_predicate_eq(REPOSITORY_KEY, repository_key)

        rows = yield self._commit_store.query(q)
        log.warn('Found %d commits in the store' % len(rows))

        # This method does not sync with existing it loads it!
        new_head = repo._dotgit

        # Keep track of the current heads...
        commits_front = set()


        for key, columns in rows.iteritems():


            blob = columns[VALUE]
            wse = gpb_wrapper.StructureElement.parse_structure_element(blob)
            repo.index_hash[key] = wse

            cref = repo._load_element(wse)

            repo._commit_index[key] = cref
            cref.ReadOnly = True


            if columns[BRANCH_NAME]:
                # If this appears to be a head commit

                # Deal with the possibility that more than one branch points to the same commit
                branch_names = columns[BRANCH_NAME].split(',')

                for name in branch_names:

                    for branch in new_head.branches:
                        # if the branch already exists in the new_head just add a commitref
                        if branch.branchkey == name:
                            link = branch.commitrefs.add()
                            break
                    else:
                        # If not add a new branch
                        branch = new_head.branches.add()
                        branch.branchkey = name
                        link = branch.commitrefs.add()

                    # Add all the commitrefs to the list to load from - makes the edge cases simpler...
                    commits_front.add(cref)


                    link.SetLink(cref)
                    link.isleaf=False

        if len(commits_front) is 0:
            log.warn('No heads found in this repository!')

        repo.broken_children = {}

        repo.broken_parents = {}

        for key, cref in repo._commit_index.iteritems():

            for parent in cref.parentrefs:
                link = parent.GetLink('commitref')

                # load the linked object no matter what to realize parent child relationships
                try:
                    obj = repo.get_linked_object(link)
                except KeyError:
                    repo.broken_children[key] = cref


        for key, cref in repo._commit_index.iteritems():

            if len(cref.ParentLinks) == 0:

                repo.broken_parents[key] = cref

            if len(cref.parentrefs) == 0:

                repo.root_commit = cref


        repo.oldest_valid = None
        repo.orphaned_crefs = {}
        if repo.broken_children:

            min_date = 99999999999999999
            my_ref = None
            for cref in repo.broken_parents.values():
              if cref.date < min_date:
                my_ref = cref
                min_date = cref.date

            repo.oldest_valid = my_ref

            repo.orphaned_crefs = repo.broken_children.copy()

            for cref in repo.broken_children.itervalues():

                prefs = [ref.Root for ref in cref.ParentLinks]

                new_refs = []
                while len(prefs) > 0:

                    for ref in prefs:

                        if ref.MyId not in repo.orphaned_crefs:
                            repo.orphaned_crefs[ref.MyId] = ref

                            new_refs.extend([_ref.Root for _ref in ref.ParentLinks])

                    prefs = new_refs
                    new_refs = []

            broken_parents = repo.broken_parents.copy()
            if repo.oldest_valid is not None:
                del broken_parents[repo.oldest_valid.MyId]

            repo.orphaned_crefs.update(broken_parents)

        elif repo.broken_parents:
            # if there are only broken parents...

            repo.orphaned_crefs.update(repo.broken_parents)

            broken_refs = repo.broken_parents.values()


            reachable_heads = repo.current_heads()

            reachable_keys = set([cref.MyId for cref in reachable_heads])
            while reachable_heads:

                new_reachables = []
                for cref in reachable_heads:

                    prefs = [pref.commitref for pref in cref.parentrefs]

                    for new_ref in prefs:

                        if new_ref.MyId not in reachable_keys:
                            reachable_keys.add(new_ref.MyId)

                            new_reachables.append(new_ref)

                reachable_heads = new_reachables


            while broken_refs:
                new_broken = []
                for cref in broken_refs:

                    crefs = [pref.commitref for pref in cref.parentrefs]

                    for new_ref in crefs:

                        if new_ref.MyId not in reachable_keys and new_ref.MyId not in repo.orphaned_crefs:
                            repo.orphaned_crefs[new_ref.MyId] = new_ref
                            new_broken.append(new_ref)

                broken_refs = new_broken
                

        log.info('read_repo_state: complete')

        # return repository
        defer.returnValue(repo)

    @defer.inlineCallbacks
    def checkout(self, repo, cref):

        def filtermethod(x):
                """
                Returns true if the passed in link's type is not in the excluded_types list of the passed in message.
                """
                return (x.type.GPBMessage not in repo.excluded_types)


        yield self._get_blobs(repo, [cref.GetLink('objectroot').key, ], filtermethod)

        defer.returnValue(cref.objectroot)

    @defer.inlineCallbacks
    def resolve_broken_repository(self,repo, cref):


        # Must reconstitute the head
        mutable_cls = object_utils.get_gpb_class_from_type_id(MUTABLE_TYPE)
        new_head = repo._wrap_message_object(mutable_cls(), addtoworkspace=False)
        new_head.repositorykey = repo.repository_key

        # keep the first branch name...
        bname = repo.branches[0].branchkey

        branch = new_head.branches.add()
        branch.branchkey = bname
        link = branch.commitrefs.add()

        link.SetLink(cref)
        link.isleaf=False

        repo._dotgit.Invalidate()
        repo._dotgit = new_head


        for key in repo.orphaned_crefs.iterkeys():

            res = yield self._commit_store.remove(key)

            if res is not None:
                log.warn('Could not delete commit - that is bad!')

            #del repo._commit_index[key]


        yield self._commit_store.update_index(key=cref.MyId, index_attributes={BRANCH_NAME:bname})


        log.critical('I think it worked - try checking out your resource!')

#del RepairBench.op_checkout
#del RepairBench.op_fetch_blobs
#del RepairBench.op_pull
#del RepairBench.op_pull
#del RepairBench.op_push


@defer.inlineCallbacks
def cassandra_repair_shop(host='', keyspace='', uname=None, pword=None ):

    storage_provider = {'host':host,'port':9160}
    if host is '':
        raise RuntimeError('No host name provided!')

    if keyspace is '':
        raise RuntimeError('No keyspace name provided!')

    c_store = CassandraIndexedStoreBootstrap(uname,pword,storage_provider, keyspace, COMMIT_CACHE)
    yield c_store.initialize()
    yield c_store.activate()

    b_store = CassandraStoreBootstrap(uname,pword,storage_provider, keyspace, BLOB_CACHE)
    yield b_store.initialize()
    yield b_store.activate()

    rb = RepairBench(blob_store=b_store, commit_store=c_store, cache_size=10**9)
    # Make the cache big - so you don't have to worry about it...

    defer.returnValue(rb)
