#!/usr/bin/env python

"""
@file ion/integration/test_app_integration.py
@test ion.integration.app_integration_service
@author David Everett
"""



import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

#from ion.integration.r1integration_service import R1IntegrationServiceClient
from ion.integration.ais.app_integration_service import AppIntegrationServiceClient
from ion.core.messaging.message_client import MessageClient
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient
from ion.services.coi.resource_registry_beta.resource_client import ResourceInstance
from ion.test.iontest import IonTestCase

from ion.core.object import object_utils

# import GPB type identifiers for AIS
from ion.integration.ais.ais_object_identifiers import AIS_REQUEST_MSG_TYPE, AIS_RESPONSE_MSG_TYPE
from ion.integration.ais.ais_object_identifiers import REGISTER_USER_TYPE, \
                                                       UPDATE_USER_EMAIL_TYPE,   \
                                                       UPDATE_USER_DISPATCH_QUEUE_TYPE, \
                                                       OOI_ID_TYPE, \
                                                       FIND_DATA_RESOURCES_REQ_MSG_TYPE


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

class AppIntegrationTest(IonTestCase):
    """
    Testing Application Integration Service.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        services = [
            {'name':'app_integration','module':'ion.integration.ais.app_integration_service','class':'AppIntegrationService'},
            {'name':'ds1','module':'ion.services.coi.datastore','class':'DataStoreService',
             'spawnargs':{'servicename':'datastore'}},
            {'name':'resource_registry1','module':'ion.services.coi.resource_registry_beta.resource_registry','class':'ResourceRegistryService',
             'spawnargs':{'datastore_service':'datastore'}},
            {'name':'identity_registry','module':'ion.services.coi.identity_registry','class':'IdentityRegistryService'}
        ]
        sup = yield self._spawn_processes(services)

        self.sup = sup

        self.aisc = AppIntegrationServiceClient(proc=sup)
        #self.aisc = R1IntegrationServiceClient(proc=sup)
        #self.rc = ResourceClient(proc=sup)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_findDataResources(self):

        # Create a message client
        mc = MessageClient(proc=self.test_sup)
        rc = ResourceClient(proc=self.test_sup)
        
        log.debug('DHE: calling createDataset')
        dsID = yield self.createDataset(rc)

        print '================================================================='
        print 'Added Dataset:'
        print dsID
        print '================================================================='
        #log.debug('DHE: createDataset returned: ' + dsID))
        
        # Use the message client to create a message object
        log.debug('DHE: AppIntegrationService! instantiating FindResourcesMsg.\n')
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_RESOURCES_REQ_MSG_TYPE)
        reqMsg.message_parameters_reference.minLatitude = 32.87521
        reqMsg.message_parameters_reference.maxLatitude = 32.97521
        reqMsg.message_parameters_reference.minLongitude = -117.274609
        reqMsg.message_parameters_reference.maxLongitude = -117.174609
        
        """
        DHE: temporarily passing the identity of the dummied dataset just
        created into the client so that it can access because currently there
        is now way to search.
        """
        reqMsg.message_parameters_reference.identity = dsID

        log.debug('DHE: Calling findDataResources!!...')
        outcome1 = yield self.aisc.findDataResources(reqMsg)
        #log.debug('DHE: findDataResources returned:\n'+str(outcome1))
        log.debug('DHE: findDataResources returned:\n'+str(outcome1.message_parameters_reference[0].data_resource_id[0]))

    @defer.inlineCallbacks
    def test_getDataResourceDetail(self):

        # Create a message client
        mc = MessageClient(proc=self.test_sup)
        rc = ResourceClient(proc=self.test_sup)
        
        log.debug('DHE: testing getDataResourceDetail')

        # Use the message client to create a message object
        log.debug('DHE: AppIntegrationService! instantiating GetDataResourceDetailMsg.\n')
        
        # CHANGE THIS TO GET_DATA_RESOURCE_DETAIL_REQ_MSG_TYPE
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_RESOURCES_REQ_MSG_TYPE)
        #reqMsg.message_parameters_reference.minLatitude = 32.87521

        log.debug('DHE: Calling getDataResourceDetail!!...')
        outcome1 = yield self.aisc.getDataResourceDetail(reqMsg)
        log.debug('DHE: getDataResourceDetail returned:\n'+str(outcome1))

    @defer.inlineCallbacks
    def test_createDownloadURL(self):

        # Create a message client
        mc = MessageClient(proc=self.test_sup)
        rc = ResourceClient(proc=self.test_sup)
        
        log.debug('DHE: testing createDownloadURL')

        # Use the message client to create a message object
        log.debug('DHE: AppIntegrationService! instantiating CreateDownloadURLMSG.\n')
        
        # CHANGE THIS TO CREATE_DOWNLOAD_URL_REQ_MSG_TYPE
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(FIND_DATA_RESOURCES_REQ_MSG_TYPE)
        #reqMsg.message_parameters_reference.minLatitude = 32.87521

        log.debug('DHE: Calling createDownloadURL!!...')
        outcome1 = yield self.aisc.createDownloadURL(reqMsg)
        log.debug('DHE: createDownloadURL returned:\n'+str(outcome1))

    @defer.inlineCallbacks
    def test_registerUser(self):

        # Create a message client
        mc = MessageClient(proc=self.test_sup)

        # create the register_user request GPBs
        msg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE, MessageName='AIS RegisterUser request')
        msg.message_parameters_reference = msg.CreateObject(REGISTER_USER_TYPE)
        
        # fill in the certificate and key
        msg.message_parameters_reference.certificate = """-----BEGIN CERTIFICATE-----
MIIEMzCCAxugAwIBAgICBQAwDQYJKoZIhvcNAQEFBQAwajETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRsw
GQYDVQQDExJDSUxvZ29uIEJhc2ljIENBIDEwHhcNMTAxMTE4MjIyNTA2WhcNMTAxMTE5MTAzMDA2
WjBvMRMwEQYKCZImiZPyLGQBGRMDb3JnMRcwFQYKCZImiZPyLGQBGRMHY2lsb2dvbjELMAkGA1UE
BhMCVVMxFzAVBgNVBAoTDlByb3RlY3ROZXR3b3JrMRkwFwYDVQQDExBSb2dlciBVbndpbiBBMjU0
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtb
g0kKLmivgoVsA4U7swNDRH6svW242THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq
7LWt2T6GVVA10ex5WAeB/o7br/Z4U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b
2lUtQc6cjuHRDU4NknXaVMXTBHKPM40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4
dszsqn2SC8YDw1xrujvW2Bd7Q7BwMQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+
6M6SMQIDAQABo4HdMIHaMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgSwMBMGA1UdJQQMMAoG
CCsGAQUFBwMCMBgGA1UdIAQRMA8wDQYLKwYBBAGCkTYBAgEwagYDVR0fBGMwYTAuoCygKoYoaHR0
cDovL2NybC5jaWxvZ29uLm9yZy9jaWxvZ29uLWJhc2ljLmNybDAvoC2gK4YpaHR0cDovL2NybC5k
b2Vncmlkcy5vcmcvY2lsb2dvbi1iYXNpYy5jcmwwHwYDVR0RBBgwFoEUaXRzYWdyZWVuMUB5YWhv
by5jb20wDQYJKoZIhvcNAQEFBQADggEBAEYHQPMY9Grs19MHxUzMwXp1GzCKhGpgyVKJKW86PJlr
HGruoWvx+DLNX75Oj5FC4t8bOUQVQusZGeGSEGegzzfIeOI/jWP1UtIjzvTFDq3tQMNvsgROSCx5
CkpK4nS0kbwLux+zI7BWON97UpMIzEeE05pd7SmNAETuWRsHMP+x6i7hoUp/uad4DwbzNUGIotdK
f8b270icOVgkOKRdLP/Q4r/x8skKSCRz1ZsRdR+7+B/EgksAJj7Ut3yiWoUekEMxCaTdAHPTMD/g
Mh9xL90hfMJyoGemjJswG5g3fAdTP/Lv0I6/nWeH/cLjwwpQgIEjEAVXl7KHuzX5vPD/wqQ=
-----END CERTIFICATE-----"""
        msg.message_parameters_reference.rsa_private_key = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtbg0kKLmivgoVsA4U7swNDRH6svW24
2THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq7LWt2T6GVVA10ex5WAeB/o7br/Z4
U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b2lUtQc6cjuHRDU4NknXaVMXTBHKP
M40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4dszsqn2SC8YDw1xrujvW2Bd7Q7Bw
MQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+6M6SMQIDAQABAoIBAAc/Ic97ZDQ9
tFh76wzVWj4SVRuxj7HWSNQ+Uzi6PKr8Zy182Sxp74+TuN9zKAppCQ8LEKwpkKtEjXsl8QcXn38m
sXOo8+F1He6FaoRQ1vXi3M1boPpefWLtyZ6rkeJw6VP3MVG5gmho0VaOqLieWKLP6fXgZGUhBvFm
yxUPoNgXJPLjJ9pNGy4IBuQDudqfJeqnbIe0GOXdB1oLCjAgZlTR4lFA92OrkMEldyVp72iYbffN
4GqoCEiHi8lX9m2kvwiQKRnfH1dLnnPBrrwatu7TxOs02HpJ99wfzKRy4B1SKcB0Gs22761r+N/M
oO966VxlkKYTN+soN5ID9mQmXJkCgYEA/h2bqH9mNzHhzS21x8mC6n+MTyYYKVlEW4VSJ3TyMKlR
gAjhxY/LUNeVpfxm2fY8tvQecWaW3mYQLfnvM7f1FeNJwEwIkS/yaeNmcRC6HK/hHeE87+fNVW/U
ftU4FW5Krg3QIYxcTL2vL3JU4Auu3E/XVcx0iqYMGZMEEDOcQPcCgYEA6sLLIeOdngUvxdA4KKEe
qInDpa/coWbtAlGJv8NueYTuD3BYJG5KoWFY4TVfjQsBgdxNxHzxb5l9PrFLm9mRn3iiR/2EpQke
qJzs87K0A/sxTVES29w1PKinkBkdu8pNk10TxtRUl/Ox3fuuZPvyt9hi5c5O/MCKJbjmyJHuJBcC
gYBiAJM2oaOPJ9q4oadYnLuzqms3Xy60S6wUS8+KTgzVfYdkBIjmA3XbALnDIRudddymhnFzNKh8
rwoQYTLCVHDd9yFLW0d2jvJDqiKo+lV8mMwOFP7GWzSSfaWLILoXcci1ZbheJ9607faxKrvXCEpw
xw36FfbgPfeuqUdI5E6fswKBgFIxCu99gnSNulEWemL3LgWx3fbHYIZ9w6MZKxIheS9AdByhp6px
lt1zeKu4hRCbdtaha/TMDbeV1Hy7lA4nmU1s7dwojWU+kSZVcrxLp6zxKCy6otCpA1aOccQIlxll
Vc2vO7pUIp3kqzRd5ovijfMB5nYwygTB4FwepWY5eVfXAoGBAIqrLKhRzdpGL0Vp2jwtJJiMShKm
WJ1c7fBskgAVk8jJzbEgMxuVeurioYqj0Cn7hFQoLc+npdU5byRti+4xjZBXSmmjo4Y7ttXGvBrf
c2bPOQRAYZyD2o+/MHBDsz7RWZJoZiI+SJJuE4wphGUsEbI2Ger1QW9135jKp6BsY2qZ
-----END RSA PRIVATE KEY-----"""

        # try to register this user for the first time
        reply = yield self.aisc.registerUser(msg)
        log.debug('registerUser returned:\n'+str(reply))
        log.debug('registerUser returned:\n'+str(reply.message_parameters_reference[0]))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        if reply.message_parameters_reference[0].ObjectType != OOI_ID_TYPE:
            self.fail('response does not contain an OOI_ID GPB')
        FirstOoiId = reply.message_parameters_reference[0].ooi_id
        log.info("test_registerUser: first time registration received ooi_id = "+str(reply.message_parameters_reference[0].ooi_id))
            
        # try to re-register this user for a second time
        reply = yield self.aisc.registerUser(msg)
        log.debug('registerUser returned:\n'+str(reply))
        log.debug('registerUser returned:\n'+str(reply.message_parameters_reference[0]))
        if reply.MessageType != AIS_RESPONSE_MSG_TYPE:
            self.fail('response is not an AIS_RESPONSE_MSG_TYPE GPB')
        if reply.message_parameters_reference[0].ObjectType != OOI_ID_TYPE:
            self.fail('response does not contain an OOI_ID GPB')
        if FirstOoiId != reply.message_parameters_reference[0].ooi_id:
            self.fail("re-registration did not return the same OoiId as registration")
        log.info("test_registerUser: re-registration received ooi_id = "+str(reply.message_parameters_reference[0].ooi_id))



    @defer.inlineCallbacks
    def createDataset(self, rc):

        log.debug('DHE: creating test dataset!')

        dataset = yield rc.create_instance(dataset_type, ResourceName='AIS Test CDM Dataset Resource', ResourceDescription='A test resource')
        log.debug('DHE: created test dataset!')
        
        # Attach the root group
        group = dataset.CreateObject(group_type)
        group.name = 'ais test data'
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
        variable_t.content = dataset.CreateObject(array_structure_type)
        variable_t.content.bounded_arrays.add()
        variable_t.content.bounded_arrays[0] = dataset.CreateObject(bounded_array_type)
    
        variable_t.content.bounded_arrays[0].bounds.add()
        variable_t.content.bounded_arrays[0].bounds[0].origin = 0
        variable_t.content.bounded_arrays[0].bounds[0].size = 2
        variable_t.content.bounded_arrays[0].ndarray = dataset.CreateObject(int32Array_type)
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
    
        scalar_lat.content.bounded_arrays[0].bounds.add()
        scalar_lat.content.bounded_arrays[0].bounds[0].origin = 0
        scalar_lat.content.bounded_arrays[0].bounds[0].size = 1
        scalar_lat.content.bounded_arrays[0].ndarray = dataset.CreateObject(float32Array_type)
        scalar_lat.content.bounded_arrays[0].ndarray.value.extend([-45.431])
    
    
        scalar_lon.content= dataset.CreateObject(array_structure_type)
        scalar_lon.content.bounded_arrays.add()
        scalar_lon.content.bounded_arrays[0] = dataset.CreateObject(bounded_array_type)
    
        scalar_lon.content.bounded_arrays[0].bounds.add()
        scalar_lon.content.bounded_arrays[0].bounds[0].origin = 0
        scalar_lon.content.bounded_arrays[0].bounds[0].size = 1
        scalar_lon.content.bounded_arrays[0].ndarray = dataset.CreateObject(float32Array_type)
        scalar_lon.content.bounded_arrays[0].ndarray.value.extend([25.909])
    
    
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
        
        # 'put' the resource into the Resource Registry
        yield rc.put_instance(dataset, 'Testing put...')
        log.debug('DHE: createDataset supposedly put dataset with identity: ' + str(dataset.ResourceIdentity))
        
        defer.returnValue(dataset.ResourceIdentity)


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
