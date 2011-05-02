#!/usr/bin/env python

"""
@file ion/res/config.py
@author David Stuebe
@author Tim LaRocque

Sample Dataset are configure and loaded like so:
'ion.services.coi.datastore_bootstrap.ion_preload_config':{
    # Path to files relative to ioncore-python directory!
    # Get files from:  http://ooici.net/ion_data/
	'sample_traj_dataset' : '../../ion_data/SOS_Test.tar.gz',
	'sample_station_dataset' : '../../ion_data/USGS_Test.tar.gz'
},

"""
import tarfile
import random
from tarfile import ExtractError
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)


from twisted.internet import defer

from ion.util import procutils as pu

from ion.core.object import object_utils, codec, gpb_wrapper


# Create CDM Type Objects
datasource_type = object_utils.create_type_identifier(object_id=4502, version=1)
dataset_type = object_utils.create_type_identifier(object_id=10001, version=1)
group_type = object_utils.create_type_identifier(object_id=10020, version=1)
dimension_type = object_utils.create_type_identifier(object_id=10018, version=1)
variable_type = object_utils.create_type_identifier(object_id=10024, version=1)
bounded_array_type = object_utils.create_type_identifier(object_id=10021, version=1)
array_structure_type = object_utils.create_type_identifier(object_id=10025, version=1)

attribute_type = object_utils.create_type_identifier(object_id=10017, version=1)
stringArray_type = object_utils.create_type_identifier(object_id=10015, version=1)
float32Array_type = object_utils.create_type_identifier(object_id=10013, version=1)
int32Array_type = object_utils.create_type_identifier(object_id=10009, version=1)

from ion.core import ioninit
CONF = ioninit.config(__name__)

def bootstrap_byte_array_dataset(resource_instance, *args, **kwargs):
    """
    Example file: ion/services/coi/SOS_Test.arr
    This method loads data from byte array files on disk - structure container GPB's or tgz of the same...
    """
    ds_svc = args[0]
    filename = kwargs['filename']
    log.debug('Bootstraping dataset from local byte array: "%s"' % filename)

    assert ds_svc is not None, 'Invalid invocation of the bootstrap_byte_array_dataset function. Must pass the datastore svc instance!'


    if not filename or filename == 'None':
        log.info('Could not bootstrap dataset with using datastore service "%s" and filename "%s"' % (str(ds_svc), str(filename)))
        return False

    if filename.endswith('.tar.gz') or filename.endswith('.tgz'):

        result = read_ooicdm_tar_file(resource_instance, filename)

    else:
        result = read_ooicdm_file(resource_instance, filename)




    log.debug('Bootstraping dataset from local byte array complete: "%s"' % filename)



    return result

def read_ooicdm_file(resource_instance, filename):
    f = None
    try:

       # Get an absolute path to the file
       filename = pu.get_ion_path(filename)

       f = open(filename, 'r')
       result = True

    except IOError, e:
       log.error('dataset_bootstrap.bootstrap_byte_array_dataset(): Could not open the given filepath "%s" for read access: %s' % (filename, str(e)))

    if f is not None:
       head_elm, obj_dict = codec._unpack_container(f.read())
       resource_instance.Repository.index_hash.update(obj_dict)

       root_obj = resource_instance.Repository._load_element(head_elm)
       resource_instance.ResourceObject = root_obj

       resource_instance.Repository.load_links(root_obj)

       f.close()

    return result

def read_ooicdm_tar_file(resource_instance, filename):
    f = None
    tar = None
    result = False
    try:

        # Get an absolute path to the file
        filename = pu.get_ion_path(filename)


        log.debug('Untaring file...')
        tar = tarfile.open(filename, 'r')

        #f = tar.extractfile(tar.next())

    except IOError, e:
        log.error('dataset_bootstrap.bootstrap_byte_array_dataset(): Could not open the given filepath "%s" for read access: %s' % (filename, str(e)))

    except ExtractError, e:
        log.error('dataset_bootstrap.bootstrap_byte_array_dataset(): Could not read from zipped tar filepath "%s", Extract error: %s' % (filename, str(e)))

    if tar is None:
        return False


    vars=[]
    root_obj = None
    for name in tar.getnames():

        try:
            f = tar.extractfile(tar.getmember(name))
        except ExtractError, e:
            log.error('dataset_bootstrap.bootstrap_byte_array_dataset(): Could not extract from zipped tar filepath "%s", Extract error: %s' % (filename, str(e)))
            return False

        head_elm, obj_dict = codec._unpack_container(f.read())
        resource_instance.Repository.index_hash.update(obj_dict)

        f.close()

        head_obj = resource_instance.Repository._load_element(head_elm)

        if head_obj.ObjectType == dataset_type:
            root_obj = head_obj
        else:
            vars.append(head_obj)

    resource_instance.ResourceObject = root_obj

    resource_instance.Repository.load_links(root_obj)


    group = root_obj.root_group

    # Clear any bounded arrays which are empty. Create content field if it is not present
    for var in group.variables:

        if var.IsFieldSet('content'):

            content = var.content

            if len(content.bounded_arrays) > 0:

                i =0
                while i < len(content.bounded_arrays):

                    ba = content.bounded_arrays[i]

                    if not ba.IsFieldSet('ndarray'):
                        del content.bounded_arrays[i]

                        continue
                    else:
                        i += 1

        else:
            var.content = resource_instance.CreateObject(array_structure_type)

    # Now add any bounded arrays that we need....
    for var_container in vars:
        ba = var_container.bounded_array

        log.debug('Adding content to variable name: %s' % var_container.variable_name)
        try:
            var = group.FindVariableByName(var_container.variable_name)
        except gpb_wrapper.OOIObjectError, oe:
            log.error(str(oe))
            raise IOError('Expected variable name %s not found in tar file dataset %s' % (var_container.variable_name, filename))

        ba_link = var.content.bounded_arrays.add()
        ba_link.SetLink(ba)

    result = True

    tar.close()

    return result


    
def bootstrap_profile_dataset(dataset, *args, **kwargs):
    """
    Pass in a link from the resource object which is created in the initialization of the datastore
    This method constructs a dataset manually!
    """
    # Attach the root group
    group = dataset.CreateObject(group_type)
    group.name = 'junk data'
    dataset.root_group = group
    
    random_initialization = CONF.getValue('Initialize_random_data', False)
    log.info("Random initialization of datasets is set to %s" % (random_initialization,))
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
    variable_t.content = dataset.CreateObject(array_structure_type)
    variable_t.content.bounded_arrays.add()
    variable_t.content.bounded_arrays[0] = dataset.CreateObject(bounded_array_type)

    variable_t.content.bounded_arrays[0].bounds.add()
    variable_t.content.bounded_arrays[0].bounds[0].origin = 0
    variable_t.content.bounded_arrays[0].bounds[0].size = 2
    variable_t.content.bounded_arrays[0].ndarray = dataset.CreateObject(int32Array_type)
    
    if random_initialization:
        start_time = 1280102000 + int(round(random.random()* 360000))
        end_time = start_time + 3600
        variable_t.content.bounded_arrays[0].ndarray.value.extend([start_time, end_time])
        log.info("start_time %s end_time %s " % (start_time, end_time))
    else:
        variable_t.content.bounded_arrays[0].ndarray.value.extend([1280102520, 1280106120])

    variable_z.content = dataset.CreateObject(array_structure_type)
    variable_z.content.bounded_arrays.add()
    variable_z.content.bounded_arrays[0] = dataset.CreateObject(bounded_array_type)

    variable_z.content.bounded_arrays[0].bounds.add()
    variable_z.content.bounded_arrays[0].bounds[0].origin = 0
    variable_z.content.bounded_arrays[0].bounds[0].size = 3
    variable_z.content.bounded_arrays[0].ndarray = dataset.CreateObject(float32Array_type)
    variable_z.content.bounded_arrays[0].ndarray.value.extend([0.0, 0.1, 0.2])


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
    scalar_lat.content= dataset.CreateObject(array_structure_type)
    scalar_lat.content.bounded_arrays.add()
    scalar_lat.content.bounded_arrays[0] = dataset.CreateObject(bounded_array_type)

    if random_initialization:
        sign = 1
        if int(random.random() * 10) % 2 == 1:
            sign = -1 
        lat = round(sign * random.random() * 90,3)
        long = round(random.random() * 180,3)
        log.info("Using lat %s long %s" % (lat,long))
    else:
        lat = -41.431
        long = 25.909    

    scalar_lat.content.bounded_arrays[0].bounds.add()
    scalar_lat.content.bounded_arrays[0].bounds[0].origin = 0
    scalar_lat.content.bounded_arrays[0].bounds[0].size = 1
    scalar_lat.content.bounded_arrays[0].ndarray = dataset.CreateObject(float32Array_type)
    scalar_lat.content.bounded_arrays[0].ndarray.value.extend([lat])


    scalar_lon.content= dataset.CreateObject(array_structure_type)
    scalar_lon.content.bounded_arrays.add()
    scalar_lon.content.bounded_arrays[0] = dataset.CreateObject(bounded_array_type)

    scalar_lon.content.bounded_arrays[0].bounds.add()
    scalar_lon.content.bounded_arrays[0].bounds[0].origin = 0
    scalar_lon.content.bounded_arrays[0].bounds[0].size = 1
    scalar_lon.content.bounded_arrays[0].ndarray = dataset.CreateObject(float32Array_type)
    scalar_lon.content.bounded_arrays[0].ndarray.value.extend([long])


    scalar_sid.content= dataset.CreateObject(array_structure_type)
    scalar_sid.content.bounded_arrays.add()
    scalar_sid.content.bounded_arrays[0] = dataset.CreateObject(bounded_array_type)

    scalar_sid.content.bounded_arrays[0].bounds.add()
    scalar_sid.content.bounded_arrays[0].bounds[0].origin = 0
    scalar_sid.content.bounded_arrays[0].bounds[0].size = 1
    scalar_sid.content.bounded_arrays[0].ndarray = dataset.CreateObject(int32Array_type)
    scalar_sid.content.bounded_arrays[0].ndarray.value.extend([10059])


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
    variable_salinity.content= dataset.CreateObject(array_structure_type)
    variable_salinity.content.bounded_arrays.add()
    variable_salinity.content.bounded_arrays[0] = dataset.CreateObject(bounded_array_type)

    variable_salinity.content.bounded_arrays[0].bounds.add()
    variable_salinity.content.bounded_arrays[0].bounds[0].origin = 0
    variable_salinity.content.bounded_arrays[0].bounds[0].size = 2 # time dimension
    variable_salinity.content.bounded_arrays[0].bounds.add()
    variable_salinity.content.bounded_arrays[0].bounds[1].origin = 0
    variable_salinity.content.bounded_arrays[0].bounds[1].size = 3 # depth dimension
    variable_salinity.content.bounded_arrays[0].ndarray = dataset.CreateObject(float32Array_type)
    
    if random_initialization:
        l = [round(random.random()*2 +29,2) for i in range(6)]
        log.info("Adding random data %s" % (l,))
        variable_salinity.content.bounded_arrays[0].ndarray.value.extend(l)
    else:
        variable_salinity.content.bounded_arrays[0].ndarray.value.extend([29.82, 29.74, 29.85, 30.14, 30.53, 30.85])


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
    attrib_lat_max = _create_string_attribute(dataset, 'ion_geospatial_lat_max', [str(lat)])
    attrib_lat_min = _create_string_attribute(dataset, 'ion_geospatial_lat_min', [str(lat)])
    attrib_lon_max = _create_string_attribute(dataset, 'ion_geospatial_lon_max', [str(long)])
    attrib_lon_min = _create_string_attribute(dataset, 'ion_geospatial_lon_min', [str(long)])
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
    
    return True


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


#---------------------------------------------#
# Create the corresponding datasource objects #
#---------------------------------------------#

def bootstrap_profile_data_source_resource(datasource, *args, **kwargs):


    #--------------------------------------------#
    # Create the corresponding datasource object #
    #--------------------------------------------#
    ds_svc = args[0]

    dataset_id = kwargs.get('associated_dataset_id')
    dataset = ds_svc.workbench.get_repository(dataset_id)

    has_a_id = kwargs.get('has_a_id')
    has_a = ds_svc.workbench.get_repository(has_a_id)

    datasource.Repository.commit('Commit source before creating association')

     # Just create it - the workbench/datastore will take care of the rest!
    asssociation = ds_svc.workbench.create_association(datasource, has_a,  dataset)

    datasource.source_type = datasource.SourceType.SOS
    datasource.property.append('sea_water_temperature')
    datasource.station_id.append('41012')
    datasource.request_type = datasource.RequestType.NONE

    datasource.base_url = "http://sdf.ndbc.noaa.gov/sos/server.php?"

    datasource.max_ingest_millis = 6000

    return True

def bootstrap_traj_data_source(datasource, *args, **kwargs):

    #-------------------------------------------#
    # Create the corresponding datasource object #
    #-------------------------------------------#
    # Datasource: NDBC SOS Glider data

    ds_svc = args[0]

    dataset_id = kwargs.get('associated_dataset_id')
    dataset = ds_svc.workbench.get_repository(dataset_id)
    if not dataset:
        # Abort if the dataset does not exist
        return False

    has_a_id = kwargs.get('has_a_id')
    has_a = ds_svc.workbench.get_repository(has_a_id)

    datasource.Repository.commit('Commit source before creating association')

    # Just create it - the workbench/datastore will take care of the rest!
    asssociation = ds_svc.workbench.create_association(datasource, has_a,  dataset)

    datasource.source_type = datasource.SourceType.SOS
    datasource.property.append('salinity')
    datasource.station_id.append('48900')
    datasource.request_type = datasource.RequestType.NONE

    datasource.base_url = "http://sdf.ndbc.noaa.gov/sos/server.php?"

    datasource.max_ingest_millis = 10000

    return True


def bootstrap_station_data_source(datasource, *args, **kwargs):

    #-------------------------------------------#
    # Create the corresponding datasource object #
    #-------------------------------------------#
    # Datasource: USGS waterservices


    ds_svc = args[0]

    dataset_id = kwargs.get('associated_dataset_id')
    dataset = ds_svc.workbench.get_repository(dataset_id)

    if not dataset:
        # Abort if the dataset does not exist
        return False

    has_a_id = kwargs.get('has_a_id')
    has_a = ds_svc.workbench.get_repository(has_a_id)

    datasource.Repository.commit('Commit source before creating association')

    # Just create it - the workbench/datastore will take care of the rest!
    asssociation = ds_svc.workbench.create_association(datasource, has_a,  dataset)


    datasource.source_type = datasource.SourceType.USGS
    datasource.property.append('00010')
    datasource.property.append('00060')
    datasource.station_id.append('01463500')
    datasource.request_type = datasource.RequestType.NONE # *not used*

    datasource.base_url = "http://waterservices.usgs.gov/nwis/iv?"

    datasource.max_ingest_millis = 6000

    return True


def bootstrap_hycom_data_source(datasource, *args, **kwargs):

    #-------------------------------------------#
    # Create the corresponding datasource object #
    #-------------------------------------------#
    # Datasource: USGS waterservices


    ds_svc = args[0]

    dataset_id = kwargs.get('associated_dataset_id')
    dataset = ds_svc.workbench.get_repository(dataset_id)

    if not dataset:
        # Abort if the dataset does not exist
        return False

    has_a_id = kwargs.get('has_a_id')
    has_a = ds_svc.workbench.get_repository(has_a_id)

    datasource.Repository.commit('Commit source before creating association')

    # Just create it - the workbench/datastore will take care of the rest!
    asssociation = ds_svc.workbench.create_association(datasource, has_a,  dataset)


    datasource.source_type = datasource.SourceType.NETCDF_S
    datasource.request_type = datasource.RequestType.FTP
    datasource.base_url = "ftp://ftp7300.nrlssc.navy.mil/pub/smedstad/ROMS/"
    datasource.max_ingest_millis = 6000

    return True


def bootstrap_ntas1_data_source(datasource, *args, **kwargs):

    #-------------------------------------------#
    # Create the corresponding datasource object #
    #-------------------------------------------#
    # Datasource: USGS waterservices


    ds_svc = args[0]

    dataset_id = kwargs.get('associated_dataset_id')
    dataset = ds_svc.workbench.get_repository(dataset_id)

    if not dataset:
        # Abort if the dataset does not exist
        return False

    has_a_id = kwargs.get('has_a_id')
    has_a = ds_svc.workbench.get_repository(has_a_id)

    datasource.Repository.commit('Commit source before creating association')

    # Just create it - the workbench/datastore will take care of the rest!
    asssociation = ds_svc.workbench.create_association(datasource, has_a,  dataset)


    datasource.source_type = datasource.SourceType.NETCDF_S
    datasource.request_type = datasource.RequestType.DAP

    datasource.base_url = "http://geoport.whoi.edu/thredds/dodsC/usgs/data0/rsignell/data/oceansites/OS_NTAS_2010_R_M-1.nc"

    datasource.max_ingest_millis = 6000

    return True


def bootstrap_ntas2_data_source(datasource, *args, **kwargs):

    #-------------------------------------------#
    # Create the corresponding datasource object #
    #-------------------------------------------#
    # Datasource: USGS waterservices


    ds_svc = args[0]

    dataset_id = kwargs.get('associated_dataset_id')
    dataset = ds_svc.workbench.get_repository(dataset_id)

    if not dataset:
        # Abort if the dataset does not exist
        return False

    has_a_id = kwargs.get('has_a_id')
    has_a = ds_svc.workbench.get_repository(has_a_id)

    datasource.Repository.commit('Commit source before creating association')

    # Just create it - the workbench/datastore will take care of the rest!
    asssociation = ds_svc.workbench.create_association(datasource, has_a,  dataset)


    datasource.source_type = datasource.SourceType.NETCDF_S
    datasource.request_type = datasource.RequestType.DAP


    datasource.base_url = "http://geoport.whoi.edu/thredds/dodsC/usgs/data0/rsignell/data/oceansites/OS_NTAS_2010_R_M-2.nc"

    datasource.max_ingest_millis = 6000

    return True


def bootstrap_whots1_data_source(datasource, *args, **kwargs):

    #-------------------------------------------#
    # Create the corresponding datasource object #
    #-------------------------------------------#
    # Datasource: USGS waterservices


    ds_svc = args[0]

    dataset_id = kwargs.get('associated_dataset_id')
    dataset = ds_svc.workbench.get_repository(dataset_id)

    if not dataset:
        # Abort if the dataset does not exist
        return False

    has_a_id = kwargs.get('has_a_id')
    has_a = ds_svc.workbench.get_repository(has_a_id)

    datasource.Repository.commit('Commit source before creating association')

    # Just create it - the workbench/datastore will take care of the rest!
    asssociation = ds_svc.workbench.create_association(datasource, has_a,  dataset)


    datasource.source_type = datasource.SourceType.NETCDF_S
    datasource.request_type = datasource.RequestType.DAP

    datasource.base_url = "http://geoport.whoi.edu/thredds/dodsC/usgs/data0/rsignell/data/oceansites/OS_WHOTS_2010_R_M-1.nc"

    datasource.max_ingest_millis = 6000

    return True


def bootstrap_whots2_data_source(datasource, *args, **kwargs):

    #-------------------------------------------#
    # Create the corresponding datasource object #
    #-------------------------------------------#
    # Datasource: USGS waterservices


    ds_svc = args[0]

    dataset_id = kwargs.get('associated_dataset_id')
    dataset = ds_svc.workbench.get_repository(dataset_id)

    if not dataset:
        # Abort if the dataset does not exist
        return False

    has_a_id = kwargs.get('has_a_id')
    has_a = ds_svc.workbench.get_repository(has_a_id)

    datasource.Repository.commit('Commit source before creating association')

    # Just create it - the workbench/datastore will take care of the rest!
    asssociation = ds_svc.workbench.create_association(datasource, has_a,  dataset)

    datasource.source_type = datasource.SourceType.NETCDF_S
    datasource.request_type = datasource.RequestType.DAP

    datasource.base_url = "http://geoport.whoi.edu/thredds/dodsC/usgs/data0/rsignell/data/oceansites/OS_WHOTS_2010_R_M-1.nc"

    datasource.max_ingest_millis = 6000

    return True

def bootstrap_moanalua_data_source(datasource, *args, **kwargs):

    #-------------------------------------------#
    # Create the corresponding datasource object #
    #-------------------------------------------#
    # Datasource: USGS waterservices


    ds_svc = args[0]

    dataset_id = kwargs.get('associated_dataset_id')
    dataset = ds_svc.workbench.get_repository(dataset_id)

    if not dataset:
        # Abort if the dataset does not exist
        return False

    has_a_id = kwargs.get('has_a_id')
    has_a = ds_svc.workbench.get_repository(has_a_id)

    datasource.Repository.commit('Commit source before creating association')

    # Just create it - the workbench/datastore will take care of the rest!
    asssociation = ds_svc.workbench.create_association(datasource, has_a,  dataset)


    datasource.source_type = datasource.SourceType.USGS
    datasource.property.extend(['00010', '00060', '00065', '00045', '00095'])
    datasource.station_id.append('212359157502601')
    datasource.request_type = datasource.RequestType.NONE # *not used*

    datasource.base_url = "http://waterservices.usgs.gov/nwis/iv?"

    datasource.max_ingest_millis = 6000

    return True


def bootstrap_choptank_river_data_source(datasource, *args, **kwargs):

    #-------------------------------------------#
    # Create the corresponding datasource object #
    #-------------------------------------------#
    # Datasource: USGS waterservices


    ds_svc = args[0]

    dataset_id = kwargs.get('associated_dataset_id')
    dataset = ds_svc.workbench.get_repository(dataset_id)

    if not dataset:
        # Abort if the dataset does not exist
        return False

    has_a_id = kwargs.get('has_a_id')
    has_a = ds_svc.workbench.get_repository(has_a_id)

    datasource.Repository.commit('Commit source before creating association')

    # Just create it - the workbench/datastore will take care of the rest!
    asssociation = ds_svc.workbench.create_association(datasource, has_a,  dataset)


    datasource.source_type = datasource.SourceType.USGS
    datasource.property.extend(['00010', '00060', '00065', '00045', '00095'])
    datasource.station_id.append('01491000')
    datasource.request_type = datasource.RequestType.NONE # *not used*

    datasource.base_url = "http://waterservices.usgs.gov/nwis/iv?"

    datasource.max_ingest_millis = 6000

    return True


def bootstrap_connecticut_river_data_source(datasource, *args, **kwargs):

    #-------------------------------------------#
    # Create the corresponding datasource object #
    #-------------------------------------------#
    # Datasource: USGS waterservices


    ds_svc = args[0]

    dataset_id = kwargs.get('associated_dataset_id')
    dataset = ds_svc.workbench.get_repository(dataset_id)

    if not dataset:
        # Abort if the dataset does not exist
        return False

    has_a_id = kwargs.get('has_a_id')
    has_a = ds_svc.workbench.get_repository(has_a_id)

    datasource.Repository.commit('Commit source before creating association')

    # Just create it - the workbench/datastore will take care of the rest!
    asssociation = ds_svc.workbench.create_association(datasource, has_a,  dataset)


    datasource.source_type = datasource.SourceType.USGS
    datasource.property.extend(['00010', '00060', '00065', '00045', '00095'])
    datasource.station_id.append('01184000')
    datasource.request_type = datasource.RequestType.NONE # *not used*

    datasource.base_url = "http://waterservices.usgs.gov/nwis/iv?"

    datasource.max_ingest_millis = 6000

    return True



