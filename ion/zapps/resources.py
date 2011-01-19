

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core.process.process import ProcessDesc

from ion.core.pack import app_supervisor

#from ion.core.ioninit import ion_config
from ion.core import ioninit
from ion.core.cc.shell import control

from ion.core.object import object_utils
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient, ResourceInstance

import net.ooici.cdm.syntactic.cdmdatatype_pb2 as DataType

# --- CC Application interface

# Functions required
@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):
    resource_proc = [
        {'name':'ds1',
         'module':'ion.services.coi.datastore',
         'class':'DataStoreService',
         'spawnargs':{'servicename':'datastore'}
            },
        {'name':'resource_registry1',
         'module':'ion.services.coi.resource_registry_beta.resource_registry',
         'class':'ResourceRegistryService',
         'spawnargs':{'datastore_service':'datastore'}}
        ]

    appsup_desc = ProcessDesc(name='app-supervisor-'+app_definition.name,
                              module=app_supervisor.__name__,
                              spawnargs={'spawn-procs':resource_proc})
    supid = yield appsup_desc.spawn()

    res = (supid.full, [appsup_desc])
    
    
    # Check for command line argument to add some example data resources
    if ioninit.cont_args.get('register',None) == 'demodata':

        # Run script to create data objects
        data_resources = yield _bootstrap_objects(supid)
        
        ### Rather than print the data_resources object - how do we add it to locals?
        ### I can't find the control object for the shell from here?
        print '================================================================='
        print 'Added Data Resources:'
        print data_resources
        print 'The dataset IDs will be available in your localsOkay after the shell starts!'
        print '================================================================='
        for k,v in data_resources.items():
            control.add_term_name(k,v)
    
    defer.returnValue(res)

@defer.inlineCallbacks
def stop(container, state):
    log.info("state:" +str(state) )
    supdesc = state[0]
    log.info("Terminating CC agent")
    yield supdesc.terminate()
    
    
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


@defer.inlineCallbacks
def _bootstrap_objects(supid):
    
    print 'Sup:',supid
    
    # This is only for bootstrap - do no do this in operational code!
    sup = ioninit.container_instance.proc_manager.process_registry.kvs.get(supid, None)    
    rc = ResourceClient(proc=sup)
    
    # Create the dataset resource
    dataset = yield rc.create_instance(dataset_type, name='Test CDM Resource dataset', description='A test resource')
    
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
    variable_t.data_type = DataType.INT
    variable_z.data_type = DataType.FLOAT
    scalar_lat.data_type = DataType.FLOAT
    scalar_lon.data_type = DataType.FLOAT
    scalar_sid.data_type = DataType.INT
    variable_salinity.data_type = DataType.FLOAT
    
    
    # Construct the Coordinate Variables for time and depth
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
    
    
    # Construct the Scalar Variables for lat, lon and station id
    #-----------------------------------------------------------
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
    
    
    # 'put' the resource into the Resource Registry
    rc.put_instance(dataset, 'Testing put...')
    
    
    data_resources ={'dataset1':dataset.ResourceIdentity}
    defer.returnValue(data_resources)
    
    
def _add_string_attribute(dataset, variable, name, values):
    '''
    Helper method to add string attributes to variable instances
    '''
    atrib = dataset.CreateObject(attribute_type)
    atrib.name = name
    atrib.data_type= DataType.STRING
    atrib.array = dataset.CreateObject(stringArray_type)
    atrib.array.value.extend(values)
    
    atrib_ref = variable.attributes.add()
    atrib_ref.SetLink(atrib)
