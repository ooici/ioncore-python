#!/usr/bin/env python

"""
@file ion/integration/ais/findDataResources/resourceStubs.py
@author David Everett
@brief Stubs to simulate the resource registry client.

@ TODO
Add methods to access the state of updates which are merging...
"""

from twisted.internet import defer, reactor
from twisted.python import failure
from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.exception import ReceivedError
import ion.util.procutils as pu
from ion.util.state_object import BasicLifecycleObject
from ion.core.messaging.ion_reply_codes import ResponseCodes
from ion.core.process import process
from ion.core.object import workbench
from ion.core.object import repository
from ion.core.object.repository import RepositoryError

from ion.services.coi.resource_registry_beta.resource_registry import ResourceRegistryClient
from ion.core.exception import ReceivedError


from google.protobuf import message
from google.protobuf.internal import containers
from ion.core.object import gpb_wrapper
from ion.core.object import object_utils

# Create CDM Type Objects
dataset_type = object_utils.create_type_identifier(object_id=10001, version=1)
group_type = object_utils.create_type_identifier(object_id=10020, version=1)
dimension_type = object_utils.create_type_identifier(object_id=10018, version=1)
variable_type = object_utils.create_type_identifier(object_id=10024, version=1)
attribute_type = object_utils.create_type_identifier(object_id=10017, version=1)
stringArray_type = object_utils.create_type_identifier(object_id=10015, version=1)
boundedArray_type = object_utils.create_type_identifier(object_id=10021, version=1)
float32Array_type = object_utils.create_type_identifier(object_id=10013, version=1)
int32Array_type = object_utils.create_type_identifier(object_id=10009, version=1)

resource_description_type = object_utils.create_type_identifier(object_id=1101, version=1)
resource_type = object_utils.create_type_identifier(object_id=1102, version=1)
idref_Type = object_utils.create_type_identifier(object_id=4, version=1)

CONF = ioninit.config(__name__)


class ResourceClientError(Exception):
    """
    A class for resource client exceptions
    """

class ResourceClient(object):
    """
    @brief This is the base class for a resource client. It is a factory for resource
    instances. The resource instance provides the interface for working with resources.
    The client helps create and manage resource instances.
    """
    
    def __init__(self, proc=None, datastore_service='datastore'):
        """
        Initializes a process client
        @param proc a IProcess instance as originator of messages
        @param datastore the name of the datastore service with which you wish to
        interact with the OOICI.
        """
        log.debug('DHE: STUB RESOURCE CLIENT!!!!!!')
        if not proc:
            proc = process.Process()
        
        if not hasattr(proc, 'op_fetch_linked_objects'):
            setattr(proc, 'op_fetch_linked_objects', proc.workbench.op_fetch_linked_objects)
                        
        self.proc = proc
        
        self.datastore_service = datastore_service
                
        # The resource client is backed by a process workbench.
        self.workbench = self.proc.workbench        
        
        # What about the name of the index services to use?
        
        self.registry_client = ResourceRegistryClient(proc=self.proc)

    @defer.inlineCallbacks
    def _check_init(self):
        """
        Called in client methods to ensure that there exists a spawned process
        to send and receive messages
        """
        if not self.proc.is_spawned():
            yield self.proc.spawn()
        
        assert isinstance(self.workbench, workbench.WorkBench), \
        'Process workbench is not initialized'

    @defer.inlineCallbacks
    def build_objects(self):
        """
        DHE: TESTTESTTEST
        """
        # DHE TEST!!!
        #data_resources = yield self._bootstrap_objects(self.proc)
        data_resources = yield self._bootstrap_objects()
        
    @defer.inlineCallbacks
    def create_instance(self, type_id, ResourceName, ResourceDescription=''):
        """
        @brief Ask the resource registry to create the instance!
        @param type_id is a type identifier object
        @param name is a string, a name for the new resource
        @param description is a string describing the resource
        @retval resource is a ResourceInstance object
        """
        yield self._check_init()
        
        # Create a sendable resource object
        description_repository = self.workbench.create_repository(resource_description_type)
        
        resource_description = description_repository.root_object
        
        # Set the description
        resource_description.name = ResourceName
        resource_description.description = ResourceDescription
            
        # This is breaking some abstractions - using the GPB directly...
        resource_description.type.GPBMessage.CopyFrom(type_id)
            
        # Use the registry client to make a new resource
        result = yield self.registry_client.register_resource_instance(resource_description)
        
        if result.MessageResponseCode == result.ResponseCodes.NOT_FOUND:
            raise ResourceClientError('Pull from datastore failed in resource client! Requested Resource Type Not Found!')
        #elif :
        else:
            res_id = str(result.MessageResponseBody)
                    
        result = yield self.workbench.pull(self.datastore_service, res_id)
        if result.MessageResponseCode != result.ResponseCodes.OK:
            raise ResourceClientError('Pull from datastore failed in resource client! Resource Not Found!')
        
        
        repo = self.workbench.get_repository(res_id)
        
        self.workbench.set_repository_nickname(res_id, ResourceName)
            
        yield repo.checkout('master')
        resource = ResourceInstance(repo)
        
        defer.returnValue(resource)
        
        
    @defer.inlineCallbacks
    def get_instance(self, resource_id):
        """
        @brief Get the latest version of the identified resource from the data store
        @param resource_id can be either a string resource identity or an IDRef
        object which specifies the resource identity as well as optional parameters
        version and version state.
        @retval the specified ResourceInstance 
        """
        yield self._check_init()
        
        reference = None
        branch = 'master'
        commit = None
        
        # Get the type of the argument and act accordingly
        if hasattr(resource_id, 'ObjectType') and resource_id.ObjectType == idref_Type:
            # If it is a resource reference, unpack it.
            if resource_id.branch:
                branch = resource_id.branch
                
            reference = resource_id.key
            commit = resource_id.commit
            
        elif isinstance(resource_id, (str, unicode)):
            # if it is a string, us it as an identity
            reference = resource_id
            # @TODO Some reasonable test to make sure it is valid?
            
        else:
            raise ResourceClientError('''Illegal argument type in retrieve_resource_instance:
                                      \n type: %s \nvalue: %s''' % (type(resource_id), str(resource_id)))    
            
        # Pull the repository
        result= yield self.workbench.pull(self.datastore_service, reference)
        
        if result.MessageResponseCode == result.ResponseCodes.NOT_FOUND:
            raise ResourceClientError('Pull from datastore failed in resource client! Resource Not Found!')
        #elif :
        
        
        # Get the repository
        repo = self.workbench.get_repository(reference)
        yield repo.checkout(branch)
        
        # Create a resource instance to return
        resource = ResourceInstance(repo)
            
        self.workbench.set_repository_nickname(reference, resource.ResourceName)
        # Is this a good use of the resource name? Is it safe?
            
        defer.returnValue(resource)
        
    @defer.inlineCallbacks
    def put_instance(self, instance, comment=None):
        """
        @breif Write the current state of the resource to the data store
        @param instance is a ResourceInstance object to be written
        @param comment is a comment to add about the current state of the resource
        """
        
        if not comment:
            comment = 'Resource client default commit message'
            
        # Get the repository
        repository = instance.Repository
            
        repository.commit(comment=comment)            
            
        result = yield self.workbench.push(self.datastore_service, repository.repository_key)

        if not result.MessageResponseCode == result.ResponseCodes.OK :
            raise ResourceClientError('Push to datastore failed during put_instance')
        


    @defer.inlineCallbacks
    def find_instance(self, **kwargs):
        """
        Use the index to find resource instances that match a set of constraints
        For R1 the constraints that may be used are very limited
        """
        yield self._check_init()
            
        raise NotImplementedError, "Interface Method Not Implemented"
        
    def reference_instance(self, instance, current_state=False):
        """
        @brief Reference Resource creates a data object which can be used as a
        message or part of a message or added to another data object or resource.
        @param instance is a ResourceInstance object
        @param current_state is a boolen argument which determines whether you
        intend to reference exactly the current state of the resource.
        @retval an Identity Reference object to the resource
        """
        
        return self.workbench.reference_repository(instance.ResourceIdentity, current_state)
        
    # DHE NEW CODE FOR TESTING!!! These go to bottom of the class

    @defer.inlineCallbacks
    def _bootstrap_objects(self):
            
        # We need to cheat - get the supervisor process from the container
        # This is only for bootstrap in a demo application - do no do this in operational code!
        #sup = ioninit.container_instance.proc_manager.process_registry.kvs.get(supid, None)    
        
        # Instantiate the Resource Client using the supervisor
        #rc = ResourceClient(proc=sup)
        
        # Create the dataset resource
        dataset = yield self.create_instance(dataset_type, ResourceName='Test CDM Resource dataset',
                                           ResourceDescription='A test resource')
        
        # Attach the root group
        group = dataset.CreateObject(group_type)
        group.name = 'junk data'
        dataset.root_group = group
        
        # Create all dimension and variable objects
        # Note: CDM variables such as scalars, coordinate variables and data are all represented by
        #       the variable object type.  Signifying the difference between these types is done
        #       simply by the conventions used in implementing variable objects.  Some noteable
        #       fields of the variable object are the 'shape' field and the 'content' field.  The
        #       'shape field is used for defining the dimensionality of the variable and is defined
        #       as a repeated field so that it can support multi-dimensional variables.  The 'content'
        #       field can be filled with a Bounded Array, a Structure or a Sequence with the same rank
        #       and length as the dimension objects stored in the variables shape field.
        #
        #       See: http://oceanobservatories.org/spaces/display/CIDev/DM+CDM
        #       
        #       Scalars:
        #       Scalar variables such as 'station ID' in the example below, are not associated with a
        #       dimension and therefore do NOT contain an entry for their shape field.  Also, the
        #       BoundedArray which contains the station ID's content contains only a single value.
        #       
        #       Coordinate Variables:
        #       Coordinate variables are those which contain an array of values upon which other variables
        #       are dependent on.  An example of this is the 'time' variable.  Data variables such as
        #       salinity are dependent on the dimension of time.  Coordinate variables are represented
        #       by constructing a dimension object for that coordinate and also creating a variable object
        #       to store the values of that dimension.  Once this is done, dependet data variables can
        #       define their shape with the aforementioned dimension object as well.
        #       
        #       Data Variables:
        #       Data variables are the most straight-forward types to implement.  The following example
        #       should explain all that is needed to use these types.
        dimension_t = dataset.CreateObject(dimension_type)       # dimension object for time
        dimension_z = dataset.CreateObject(dimension_type)       # dimension object for depth
        variable_t = dataset.CreateObject(variable_type)         # coordinate variable for time
        variable_z = dataset.CreateObject(variable_type)         # coordinate variable for depth
        scalar_lat = dataset.CreateObject(variable_type)         # scalar variable for latitude
        scalar_lon = dataset.CreateObject(variable_type)         # scalar variable for longitude
        scalar_sid = dataset.CreateObject(variable_type)         # scalar variable for station ID
        variable_salinity = dataset.CreateObject(variable_type)  # Data variable for salinity
        
    
        # Assign required field values (name, length, datatype, etc)
        #-----------------------------------------------------------
        dimension_t.name = 'time'
        dimension_z.name = 'z'
        dimension_t.length = 2
        dimension_z.length = 3
        
        variable_t.name = 'time'
        variable_z.name = 'depth'
        scalar_lat.name = 'lat'
        scalar_lon.name = 'lon'
        scalar_sid.name = 'stnId'
        
        variable_salinity.name = 'salinity'
        variable_t.data_type = variable_t.DataType.INT
        variable_z.data_type = variable_z.DataType.FLOAT
        scalar_lat.data_type = scalar_lat.DataType.FLOAT
        scalar_lon.data_type = scalar_lon.DataType.FLOAT
        scalar_sid.data_type = scalar_sid.DataType.INT
        variable_salinity.data_type = variable_salinity.DataType.FLOAT
        
        
        # Construct the Coordinate Variables: time and depth
        #------------------------------------------------------
        # Add dimensionality (shape)
        variable_t.shape.add()
        variable_t.shape[0] = dimension_t
        variable_z.shape.add()
        variable_z.shape[0] = dimension_z
        # Add attributes (CDM conventions require certain attributes!)
        _add_string_attribute(dataset, variable_t, 'units', ['seconds since 1970-01-01 00:00::00'])
        _add_string_attribute(dataset, variable_t, 'long_name', ['time'])
        _add_string_attribute(dataset, variable_t, 'standard_name', ['time'])
        _add_string_attribute(dataset, variable_t, '_CoordinateAxisType', ['Time'])
        _add_string_attribute(dataset, variable_z, 'units', ['m'])
        _add_string_attribute(dataset, variable_z, 'positive', ['down'])
        _add_string_attribute(dataset, variable_z, 'long_name', ['depth below mean sea level'])
        _add_string_attribute(dataset, variable_z, 'standard_name', ['depth'])
        _add_string_attribute(dataset, variable_z, '_CoordinateAxisType', ['Height'])
        _add_string_attribute(dataset, variable_z, '_CoordinateZisPositive', ['down'])
        # Add data values
        variable_t.content.add()
        variable_t.content[0] = dataset.CreateObject(boundedArray_type)
        variable_t.content[0].bounds.add()
        variable_t.content[0].bounds[0].origin = 0
        variable_t.content[0].bounds[0].size = 2
        variable_t.content[0].ndarray = dataset.CreateObject(int32Array_type) 
        variable_t.content[0].ndarray.value.extend([1280102520, 1280106120])
        variable_z.content.add()
        variable_z.content[0] = dataset.CreateObject(boundedArray_type)
        variable_z.content[0].bounds.add()
        variable_z.content[0].bounds[0].origin = 0
        variable_z.content[0].bounds[0].size = 3
        variable_z.content[0].ndarray = dataset.CreateObject(float32Array_type) 
        variable_z.content[0].ndarray.value.extend([0.0, 0.1, 0.2])
        
        
        # Construct the Scalar Variables: lat, lon and station id
        #------------------------------------------------------------
        # Add dimensionality (shape)
        # !! scalars DO NOT specify dimensions !!
        # Add attributes (CDM conventions require certain attributes!)
        _add_string_attribute(dataset, scalar_lat, 'units', ['degree_north'])
        _add_string_attribute(dataset, scalar_lat, 'long_name', ['northward positive degrees latitude'])
        _add_string_attribute(dataset, scalar_lat, 'standard_name', ['latitude'])
        _add_string_attribute(dataset, scalar_lon, 'units', ['degree_east'])
        _add_string_attribute(dataset, scalar_lon, 'long_name', ['eastward positive degrees longitude'])
        _add_string_attribute(dataset, scalar_lon, 'standard_name', ['longitude'])
        _add_string_attribute(dataset, scalar_sid, 'long_name', ['integer station identifier'])
        _add_string_attribute(dataset, scalar_sid, 'standard_name', ['station_id'])
        # Add data values
        scalar_lat.content.add()
        scalar_lat.content[0] = dataset.CreateObject(boundedArray_type)
        scalar_lat.content[0].bounds.add()
        scalar_lat.content[0].bounds[0].origin = 0
        scalar_lat.content[0].bounds[0].size = 1
        scalar_lat.content[0].ndarray = dataset.CreateObject(float32Array_type) 
        scalar_lat.content[0].ndarray.value.extend([-45.431])
        scalar_lon.content.add()
        scalar_lon.content[0] = dataset.CreateObject(boundedArray_type)
        scalar_lon.content[0].bounds.add()
        scalar_lon.content[0].bounds[0].origin = 0
        scalar_lon.content[0].bounds[0].size = 1
        scalar_lon.content[0].ndarray = dataset.CreateObject(float32Array_type) 
        scalar_lon.content[0].ndarray.value.extend([25.909])
        scalar_sid.content.add()
        scalar_sid.content[0] = dataset.CreateObject(boundedArray_type)
        scalar_sid.content[0].bounds.add()
        scalar_sid.content[0].bounds[0].origin = 0
        scalar_sid.content[0].bounds[0].size = 1
        scalar_sid.content[0].ndarray = dataset.CreateObject(int32Array_type) 
        scalar_sid.content[0].ndarray.value.extend([10059])
        
        
        # Construct the Data Variable: salinity
        #-----------------------------------------------------------
        # Add dimensionality (shape)
        variable_salinity.shape.add()
        variable_salinity.shape.add()
        variable_salinity.shape[0] = dimension_t
        variable_salinity.shape[1] = dimension_z
        # Add attributes (CDM conventions require certain attributes!)
        _add_string_attribute(dataset, variable_salinity, 'units', ['psu'])
        _add_string_attribute(dataset, variable_salinity, 'long_name', ['water salinity at location'])
        _add_string_attribute(dataset, variable_salinity, 'coordinates', ['time lon lat z'])
        _add_string_attribute(dataset, variable_salinity, 'standard_name', ['sea_water_salinity'])
        # Add data values
        variable_salinity.content.add()
        variable_salinity.content[0] = dataset.CreateObject(boundedArray_type)
        variable_salinity.content[0].bounds.add()
        variable_salinity.content[0].bounds[0].origin = 0
        variable_salinity.content[0].bounds[0].size = 2 # time dimension
        variable_salinity.content[0].bounds.add()
        variable_salinity.content[0].bounds[1].origin = 0
        variable_salinity.content[0].bounds[1].size = 3 # depth dimension
        variable_salinity.content[0].ndarray = dataset.CreateObject(float32Array_type) 
        variable_salinity.content[0].ndarray.value.extend([29.82, 29.74, 29.85, 30.14, 30.53, 30.85])
        
        
        # Attach variable and dimension objects to the root group
        #--------------------------------------------------------
        group.dimensions.add()
        group.dimensions.add()
        group.dimensions[0] = dimension_z
        group.dimensions[1] = dimension_t
        
        group.variables.add()
        group.variables.add()
        group.variables.add()
        group.variables.add()
        group.variables.add()
        group.variables.add()
        group.variables[0] = scalar_lat
        group.variables[1] = scalar_lon
        group.variables[2] = scalar_sid
        group.variables[3] = variable_salinity
        group.variables[4] = variable_t
        group.variables[5] = variable_z
        
        
        # Create and Attach global attributes to the root group
        #--------------------------------------------------------
        attrib_feature_type =   _create_string_attribute(dataset, 'CF:featureType', ['stationProfile'])
        attrib_conventions =    _create_string_attribute(dataset, 'Conventions',    ['CF-1.5'])
        attrib_history =        _create_string_attribute(dataset, 'history',        ['Converted from CSV to OOI CDM compliant NC by net.ooici.agent.abstraction.impl.SosAgent', 'Reconstructed manually as a GPB composite for the resource registry tutorial'])
        attrib_references =     _create_string_attribute(dataset, 'references',     ['http://sdf.ndbc.noaa.gov/sos/', 'http://www.ndbc.noaa.gov/', 'http://www.noaa.gov/'])
        attrib_title =          _create_string_attribute(dataset, 'title',          ['NDBC Sensor Observation Service data from "http://sdf.ndbc.noaa.gov/sos/"'])
        attrib_utc_begin_time = _create_string_attribute(dataset, 'utc_begin_time', ['2008-08-01T00:50:00Z'])
        attrib_source =         _create_string_attribute(dataset, 'source',         ['NDBC SOS'])
        attrib_utc_end_time =   _create_string_attribute(dataset, 'utc_end_time',   ['2008-08-01T23:50:00Z'])
        attrib_institution =    _create_string_attribute(dataset, 'institution',    ["NOAA's National Data Buoy Center (http://www.ndbc.noaa.gov/)"])
        
        group.attributes.add()
        group.attributes.add()
        group.attributes.add()
        group.attributes.add()
        group.attributes.add()
        group.attributes.add()
        group.attributes.add()
        group.attributes.add()
        group.attributes.add()
        
        group.attributes[0] = attrib_feature_type
        group.attributes[1] = attrib_conventions
        group.attributes[2] = attrib_history
        group.attributes[3] = attrib_references
        group.attributes[4] = attrib_title
        group.attributes[5] = attrib_utc_begin_time
        group.attributes[6] = attrib_source
        group.attributes[7] = attrib_utc_end_time
        group.attributes[8] = attrib_institution
        
        
        # 'put' the resource into the Resource Registry
        rc.put_instance(dataset, 'Testing put...')
        
        
        data_resources ={'dataset1':dataset.ResourceIdentity}
        defer.returnValue(data_resources)
        
    def _create_string_attribute(dataset, name, values):
        '''
        Helper method to create string attributes for variables and dataset groups
        '''
        atrib = dataset.CreateObject(attribute_type)
        atrib.name = name
        atrib.data_type= atrib.DataType.STRING
        atrib.array = dataset.CreateObject(stringArray_type)
        atrib.array.value.extend(values)
        return atrib
    
    def _add_string_attribute(dataset, variable, name, values):
        '''
        Helper method to add string attributes to variable instances
        '''
        atrib = _create_string_attribute(dataset, name, values)
        
        atrib_ref = variable.attributes.add()
        atrib_ref.SetLink(atrib)

    
class ResourceInstanceError(Exception):
    """
    Exception class for Resource Instance Object
    """
    
class ResourceFieldProperty(object):
    
    def __init__(self, name, doc=None):
        self.name = name
        if doc: self.__doc__ = doc
        
    def __get__(self, resource_instance, objtype=None):
        return getattr(resource_instance._repository.root_object.resource_object, self.name)
        
    def __set__(self, resource_instance, value):
        return setattr(resource_instance._repository.root_object.resource_object, self.name, value)
        
    def __delete__(self, wrapper):
        raise AttributeError('Can not delete a Resource Instance property')
        
        
class ResourceEnumProperty(object):
    
    def __init__(self, name, doc=None):
        self.name = name
        if doc: self.__doc__ = doc
        
    def __get__(self, resource_instance, objtype=None):
        return getattr(resource_instance._repository.root_object.resource_object, self.name)
        
    def __set__(self, wrapper, value):
        raise AttributeError('Can not set a Resource Instance enum object')
        
    def __delete__(self, wrapper):
        raise AttributeError('Can not delete a Resource Instance property')
    
class ResourceInstanceType(type):
    """
    Metaclass that automatically generates subclasses of Wrapper with corresponding enums and
    pass-through properties for each field in the protobuf descriptor.
    
    This approach is generally applicable to wrap data structures. It is extremely powerful!
    """

    _type_cache = {}

    def __call__(cls, resource_repository, *args, **kwargs):
        # Cache the custom-built classes
        
        # Check that the object we are wrapping is a Google Message object
        if not isinstance(resource_repository, repository.Repository):
            raise ResourceInstanceError('ResourceInstance init argument must be an instance of a Repository')
        
        if resource_repository.status == repository.Repository.NOTINITIALIZED:
            raise ResourceInstanceError('ResourceInstance init Repository argument is in an invalid state - checkout first!')
        
        if resource_repository.root_object.ObjectType != resource_type:
            raise ResourceInstanceError('ResourceInstance init Repository is not a resource object!')
        
        resource_obj = resource_repository.root_object.resource_object
        
        msgType, clsType = type(resource_obj), None

        if msgType in ResourceInstanceType._type_cache:
            clsType = ResourceInstanceType._type_cache[msgType]
        else:
            
            
            # Get the class name
            clsName = '%s_%s' % (cls.__name__, msgType.__name__)
            clsDict = {}
                
            # Now setup the properties to map through to the GPB object
            resDict = msgType.__dict__
            
            for fieldName, resource_field in resDict.items():
                #print 'Key: %s; Type: %s' % (fieldName, type(resource_field))
                if isinstance(resource_field, gpb_wrapper.WrappedProperty):
                    prop = ResourceFieldProperty(fieldName )
                    
                    clsDict[fieldName] = prop
                    
                elif isinstance(resource_field, gpb_wrapper.EnumObject):
                    prop = ResourceEnumProperty(fieldName )
                    
                    clsDict[fieldName] = prop
            

            clsType = ResourceInstanceType.__new__(ResourceInstanceType, clsName, (cls,), clsDict)

            ResourceInstanceType._type_cache[msgType] = clsType

        # Finally allow the instantiation to occur, but slip in our new class type
        obj = super(ResourceInstanceType, clsType).__call__(resource_repository, *args, **kwargs)
        return obj
    
    
    
class ResourceInstance(object):
    """
    @brief The resoure instance is the vehicle through which a process
    interacts with a resource instance. It hides the git semantics of the data
    store and deals with resource specific properties.
    """
    __metaclass__ = ResourceInstanceType
    
    # Life Cycle States
    NEW='New'
    ACTIVE='Active'
    INACTIVE='Inactive'
    COMMISSIONED='Commissioned'
    DECOMMISSIONED='Decommissioned'
    RETIRED='Retired'
    DEVELOPED='Developed'
    UPDATE = 'Update'
    
    # Resource update mode
    APPEND = 'Appending new update'
    CLOBBER = 'Clobber current state with this update'
    MERGE = 'Merge modifications in this update'
    
    # Resource update Resolutions
    RESOLVED = 'Update resolved' # When a merger occurs with the previous state
    REJECTED = 'Update rejected' # When an update is rejected
    
    def __init__(self, resource_repository):
        """
        Resource Instance objects are created by the resource client
        """
        object.__setattr__(self,'_repository',None)
        
        self._repository = resource_repository
        
        
    @property
    def Repository(self):
        return self._repository
        
    @property
    def Resource(self):
        repo = self._repository
        return repo._workspace_root
        
    
    def _get_resource_object(self):
        repo = self._repository
        return repo._workspace_root.resource_object
        
    def _set_resource_object(self, value):
        repo = self._repository
        if value.ObjectType != self.ResourceType:
            raise ResourceInstanceError('Can not change the type of a resource object!')
        repo._workspace_root.resource_object = value
        
    ResourceObject = property(_get_resource_object, _set_resource_object)
        
        
    @property
    def CompareToUpdates(self):
        """
        @ Brief This methods provides access to the committed resource states that
        are bing merged into the current version of the Resource.
        @param ind is the index into list of merged states.
        @result The result is a list of update objects which are in a read only state.
        """
        
        updates = self.Repository.merge_objects
        if len(updates)==0:
            log.warn('Invalid index into MergingResource. Current number of merged states is: %d' % (len(self.Repository.merge_objects)))
            raise ResourceInstanceError('Invalid index access to Merging Resources. Either there is no merge or you picked an invalid index.')
        
        objects=[]
        for resource in updates:
            objects.append(resource.resource_object)
        
        return objects
        
    def __str__(self):
        output  = '============== Resource ==============\n'
        output += str(self.Resource) + '\n'
        output += '============== Object ==============\n'
        output += str(self.ResourceObject) + '\n'
        output += '============ End Resource ============\n'
        return output
        
    def VersionResource(self):
        """
        @brief Create a new version of this resource - creates a new branch in
        the objects repository. This is purely local until the next push!
        @retval the key for the new version
        """
        
        branch_key = self.Repository.branch()            
        return branch_key

    @defer.inlineCallbacks
    def MergeResourceUpdate(self, mode, *args):
        """
        Use this method when updating an existing resource.
        This is the recommended pattern for updating a resource. The Resource history will include a special
        Branch pattern showing the previous state, the update and the updated state...
        Once an update is commited, the update must be resolved before the instance
        can be put (pushed) to the public datastore.
        
        <Updated State>  
        |    \          \
        |    <Update1>  <Update2> ...
        |    /          /
        <Previous State>
        
        """
        if not self.Repository.status == self.Repository.UPTODATE:
            raise ResourceInstanceError('Can not merge while the resource is in a modified state')
        
        merge_branches = []
        for update in args:
        
            if update.ObjectType != self.ResourceType:
                log.debug ('Resource Type does not match update Type')
                log.debug ('Update type %s; Resource type %s' % (str(update.ObjectType), str(self.ResourceType)))
                raise ResourceInstanceError('update_instance argument "update" must be of the same type as the resource')
            
            current_branchname = self.Repository._current_branch.branchkey
            
            # Create and switch to a new branch
            merge_branches.append(self.Repository.branch())
        
            # Set the LCS in the resource branch to UPDATE and the object to the update
            self.ResourceLifeCycleState = self.UPDATE
        
            # Copy the update object into resource as the current state object.
            self.Resource.resource_object = update
        
            self.Repository.commit(comment=str(mode))
            
            yield self.Repository.checkout(branchname=current_branchname)
        
        # Set up the merge in the repository
        for b_name in merge_branches:
            yield self.Repository.merge(branchname=b_name)
        
            # Remove the merge branch - it is only a local concern
            self.Repository.remove_branch(b_name)
        # on the next commit - when put_instance is called - the merge will be complete!
        
        
        
    
    
    def CreateObject(self, type_id):
        """
        @brief CreateObject is used to make new locally create objects which can
        be added to the resource's data structure.
        @param type_id is the type_id of the object to be created
        @retval the new object which can now be attached to the resource
        """
        return self.Repository.create_object(type_id)
        
        
    
    def ListSetFields(self):
        """
        Return a list of the names of the fields which have been set.
        """
        return self.ResourceObject.ListSetFields()

    def HasField(self, field):
        log.warn('HasField is depricated because the name is confusing. Use IsFieldSet')
        return self.IsFieldSet(field)
        
    def IsFieldSet(self, field):
        return self.ResourceObject.IsFieldSet(field)
        
    def ClearField(self, field):
        return self.ResourceObject.ClearField(field)
        
    @property
    def ResourceIdentity(self):
        """
        @brief Return the resource identity as a string
        """
        return str(self.Resource.identity)
    
    @property
    def ResourceType(self):
        """
        @brief Returns the resource type - A type identifier object - not the wrapped object.
        """
        return self.Resource.type.GPBMessage
    
    def _set_life_cycle_state(self, state):
        """
        @brief Set the Life Cycel State of the resource
        @param state is a resource life cycle state class variable defined in
        the ResourceInstance class.
        """
        # Using IS for comparison - I think this is better than the usual ==
        # Want to force the use of the self.XXXX as the argument!
                
        if state == self.NEW:        
            self.Resource.lcs = self.Resource.LifeCycleState.NEW
        elif state == self.ACTIVE:
            self.Resource.lcs = self.Resource.LifeCycleState.ACTIVE
        elif state == self.INACTIVE:
            self.Resource.lcs = self.Resource.LifeCycleState.INACTIVE
        elif state == self.COMMISSIONED:
            self.Resource.lcs = self.Resource.LifeCycleState.COMMISSIONED
        elif state == self.DECOMMISSIONED:
            self.Resource.lcs = self.Resource.LifeCycleState.DECOMMISSIONED
        elif state == self.RETIRED:
            self.Resource.lcs = self.Resource.LifeCycleState.RETIRED
        elif state == self.DEVELOPED:
            self.Resource.lcs = self.Resource.LifeCycleState.DEVELOPED
        elif state == self.UPDATE:
            self.Resource.lcs = self.Resource.LifeCycleState.UPDATE
        else:
            raise Exception('''Invalid argument value state: %s. State must be 
                one of the class variables defined in Resource Instance''' % str(state))
        
    def _get_life_cycle_state(self):
        """
        @brief Get the life cycle state of the resource
        """
        state = None
        if self.Resource.lcs == self.Resource.LifeCycleState.NEW:
            state = self.NEW    
        
        elif self.Resource.lcs == self.Resource.LifeCycleState.ACTIVE:
            state = self.ACTIVE
            
        elif self.Resource.lcs == self.Resource.LifeCycleState.INACTIVE:
            state = self.INACTIVE
            
        elif self.Resource.lcs == self.Resource.LifeCycleState.COMMISSIONED:
            state = self.COMMISSIONED
            
        elif self.Resource.lcs == self.Resource.LifeCycleState.DECOMMISSIONED:
            state = self.DECOMMISSIONED
            
        elif self.Resource.lcs == self.Resource.LifeCycleState.RETIRED:
            state = self.RETIRED
            
        elif self.Resource.lcs == self.Resource.LifeCycleState.DEVELOPED:
            state = self.DEVELOPED
        
        elif self.Resource.lcs == self.Resource.LifeCycleState.UPDATE:
            state = self.UPDATE
        
        return state
        
    ResourceLifeCycleState = property(_get_life_cycle_state, _set_life_cycle_state)
    """
    @var ResourceLifeCycleState is a getter setter property for the life cycle state of the resource
    """
        
    def _set_resource_name(self, name):
        """
        Set the name of the resource object
        """
        self.Resource.name = name
        
    def _get_resource_name(self):
        """
        """
        return str(self.Resource.name)
    
    ResourceName = property(_get_resource_name, _set_resource_name)
    """
    @var ResourceName is a getter setter property for the name of the resource
    """
    
    def _set_resource_description(self, description):
        """
        """
        self.Resource.description = description
        
    def _get_resource_description(self):
        """
        """
        return str(self.Resource.description)
        
    
    ResourceDescription = property(_get_resource_description, _set_resource_description)
    """
    @var ResourceDescription is a getter setter property for the description of the resource
    """

