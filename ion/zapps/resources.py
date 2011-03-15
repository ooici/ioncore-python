

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

    appsup_desc = ProcessDesc(name='app-supervisor-' + app_definition.name,
                              module=app_supervisor.__name__,
                              spawnargs={'spawn-procs':resource_proc})
    supid = yield appsup_desc.spawn()

    res = (supid.full, [appsup_desc])
    
    
    # Check for command line argument to add some example data resources
    if ioninit.cont_args.get('register', None) == 'demodata':

        # Run script to create data objects
        data_resources = yield _bootstrap_objects(supid)
        
        ### Rather than print the data_resources object - how do we add it to locals?
        ### I can't find the control object for the shell from here?
        print '================================================================='
        print 'Added Data Resources:'
        print data_resources
        print 'The dataset IDs will be available in your localsOkay after the shell starts!'
        print '================================================================='
        for k, v in data_resources.items():
            control.add_term_name(k, v)
    
    defer.returnValue(res)

@defer.inlineCallbacks
def stop(container, state):
    log.info("state:" + str(state))
    supdesc = state[0]
    log.info("Terminating CC agent")
    yield supdesc.terminate()

    
# Create CDM Type Objects
datasource_type = object_utils.create_type_identifier(object_id=4502, version=1)
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
        
    # We need to cheat - get the supervisor process from the container
    # This is only for bootstrap in a demo application - do no do this in operational code!
    sup = ioninit.container_instance.proc_manager.process_registry.kvs.get(supid, None)    
    
    # Instantiate the Resource Client using the supervisor
    rc = ResourceClient(proc=sup)
    
    # Create the dataset resource
    dataset = yield rc.create_instance(dataset_type, ResourceName='Test CDM Resource dataset',
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
    attrib_feature_type = _create_string_attribute(dataset, 'CF:featureType', ['stationProfile'])
    attrib_title = _create_string_attribute(dataset, 'title', ['NDBC Sensor Observation Service data from "http://sdf.ndbc.noaa.gov/sos/"'])
    attrib_institution = _create_string_attribute(dataset, 'institution', ["NOAA's National Data Buoy Center (http://www.ndbc.noaa.gov/)"])
    attrib_source = _create_string_attribute(dataset, 'source', ['NDBC SOS'])
    attrib_history = _create_string_attribute(dataset, 'history', ['Converted from CSV to OOI CDM compliant NC by net.ooici.agent.abstraction.impl.SosAgent', 'Reconstructed manually as a GPB composite for the resource registry tutorial'])
    attrib_references = _create_string_attribute(dataset, 'references', ['http://sdf.ndbc.noaa.gov/sos/', 'http://www.ndbc.noaa.gov/', 'http://www.noaa.gov/'])
    attrib_conventions = _create_string_attribute(dataset, 'Conventions', ['CF-1.5'])
    attrib_time_start = _create_string_attribute(dataset, 'ion_time_coverage_start', ['2008-08-01T00:50:00Z'])
    attrib_time_end = _create_string_attribute(dataset, 'ion_time_coverage_end', ['2008-08-01T23:50:00Z'])
    attrib_lat_max = _create_string_attribute(dataset, 'ion_geospatial_lat_max', ['-45.431'])
    attrib_lat_min = _create_string_attribute(dataset, 'ion_geospatial_lat_min', ['-45.431'])
    attrib_lon_max = _create_string_attribute(dataset, 'ion_geospatial_lon_max', ['25.909'])
    attrib_lon_min = _create_string_attribute(dataset, 'ion_geospatial_lon_min', ['25.909'])
    attrib_vert_max = _create_string_attribute(dataset, 'ion_geospatial_vertical_max', ['0.0'])
    attrib_vert_min = _create_string_attribute(dataset, 'ion_geospatial_vertical_min', ['0.2'])
    attrib_vert_pos = _create_string_attribute(dataset, 'ion_geospatial_vertical_positive', ['down'])
    
    group.attributes.add()
    group.attributes.add()
    group.attributes.add()
    group.attributes.add()
    group.attributes.add()
    group.attributes.add()
    group.attributes.add()
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
    group.attributes[1] = attrib_title
    group.attributes[2] = attrib_institution
    group.attributes[3] = attrib_source
    group.attributes[4] = attrib_history
    group.attributes[5] = attrib_references
    group.attributes[6] = attrib_conventions
    group.attributes[7] = attrib_time_start
    group.attributes[8] = attrib_time_end
    group.attributes[9] = attrib_lat_max
    group.attributes[10] = attrib_lat_min
    group.attributes[11] = attrib_lon_max
    group.attributes[12] = attrib_lon_min
    group.attributes[13] = attrib_vert_max
    group.attributes[14] = attrib_vert_min
    group.attributes[15] = attrib_vert_pos
    
    
    # 'put' the resource into the Resource Registry
    rc.put_instance(dataset, 'Testing put...')
    
    
    #-------------------------------------------#
    # Create the coresponding datasource object #
    #-------------------------------------------#
    datasource = yield rc.create_instance(datasource_type, ResourceName='Test CDM Resource datasource',
                                       ResourceDescription='A test resource for retrieving dataset context (datasource)')
    datasource.source_type = datasource.SourceType.SOS
    datasource.property.append('sea_water_temperature')
    datasource.station_id.append('41012')
    datasource.request_type = datasource.RequestType.NONE
    # datasource.top = *not used*
    # datasource.bottom = *not used*
    # datasource.left = *not used*
    # datasource.right = *not used*
#    datasource.base_url = 'http://sdf.ndbc.noaa.gov/sos/server.php?request=GetObservation&service=SOS&responseformat=text/csv&'
    datasource.base_url = "http://sdf.ndbc.noaa.gov/sos/server.php?"
    # datasource.dataset_url = *not used*
    # datasource.ncml_mask = *not used*
    
    rc.put_instance(datasource, 'Commiting initial datasource...')
    
    data_resources = {'dataset1':dataset.ResourceIdentity,
                     'datasource1':datasource.ResourceIdentity}
    defer.returnValue(data_resources)
    
def _create_string_attribute(dataset, name, values):
    '''
    Helper method to create string attributes for variables and dataset groups
    '''
    atrib = dataset.CreateObject(attribute_type)
    atrib.name = name
    atrib.data_type = atrib.DataType.STRING
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
