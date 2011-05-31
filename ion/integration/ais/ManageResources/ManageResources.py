#!/usr/bin/env python

"""
@file ion/integration/ais/ManageResource/ManageResources.py
@author Bill Bollenbcher
@brief The worker class that implements the ManageResources function for the AIS  (workflow #109)
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.messaging.message_client import MessageClient
from ion.core.exception import ApplicationError
from ion.services.dm.inventory.association_service import AssociationServiceClient
from ion.services.coi.resource_registry.resource_client import ResourceClient
from ion.integration.ais.ManageResources.epu_controller_client_stub import EPUControllerClient
from ion.services.cei.epu_controller_list_client import EPUControllerListClient

from ion.services.coi.datastore_bootstrap.ion_preload_config import dataset_res_type_name, \
                                                                    identity_res_type_name, \
                                                                    datasource_res_type_name

from ion.integration.ais.ais_object_identifiers import AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE, \
                                                       GET_RESOURCE_TYPES_RESPONSE_TYPE,\
                                                       GET_RESOURCES_OF_TYPE_RESPONSE_TYPE, \
                                                       GET_RESOURCE_RESPONSE_TYPE
from ion.core.object import object_utils
from ion.services.dm.inventory.association_service import PREDICATE_OBJECT_QUERY_TYPE, IDREF_TYPE
from ion.services.coi.datastore_bootstrap.ion_preload_config import ROOT_USER_ID, \
                                                                    HAS_A_ID, \
                                                                    IDENTITY_RESOURCE_TYPE_ID, \
                                                                    TYPE_OF_ID, \
                                                                    ANONYMOUS_USER_ID, \
                                                                    HAS_LIFE_CYCLE_STATE_ID, \
                                                                    OWNED_BY_ID, \
                                                                    SAMPLE_PROFILE_DATASET_ID, \
                                                                    DATASET_RESOURCE_TYPE_ID, \
                                                                    IDENTITY_RESOURCE_TYPE_ID, \
                                                                    DATASOURCE_RESOURCE_TYPE_ID
                                                                    

PREDICATE_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=25, version=1)

EPU_CONTROLLER_TYPE_ID = 'type_id_for_epu_controllers'

class DictObj(object):
    def __getattr__(self, attr):
        return self.__dict__.get(attr)


class ManageResources(object):
    
   def __init__(self, ais):
      log.debug('ManageResources.__init__()')
      DatasetValues = DATASET_RESOURCE_TYPE_ID, \
                      self.__LoadDatasetColumnData, \
                      self.__PrintDatasetAttributes, \
                      self.__LoadDatasetColumnHeadrers, \
                      self.__LoadDatasetAttributes
      IdentityValues = IDENTITY_RESOURCE_TYPE_ID, \
                       self.__LoadIdentityColumnData, \
                       self.__PrintIdentityAttributes, \
                       self.__LoadIdentityColumnHeadrers, \
                       self.__LoadIdentityAttributes
      DatasourceValues = DATASOURCE_RESOURCE_TYPE_ID, \
                         self.__LoadDatasourceColumnData, \
                         self.__PrintDatasourceAttributes, \
                         self.__LoadDatasourceColumnHeadrers, \
                         self.__LoadDatasourceAttributes
      EpucontrollerValues = EPU_CONTROLLER_TYPE_ID, \
                         self.__LoadEpucontrollerColumnData, \
                         self.__PrintEpucontrollerAttributes, \
                         self.__LoadEpucontrollerColumnHeadrers, \
                         self.__LoadEpucontrollerAttributes
      self.ResourceTypes = {'datasets' : DatasetValues,
                            'identities' : IdentityValues,
                            'datasources' : DatasourceValues,
                            'epucontrollers' : EpucontrollerValues
                           }
      self.MapGpbTypeToResourceType = {10001 : 'datasets',
                                       1401 : 'identities',
                                       4503 : 'datasources'                                     
                                       }
      self.SourceTypes = ['', 'SOS', 'USGS', 'AOML', 'NETCDF_S', 'NETCDF_C']
      self.RequestTypes = ['', 'NONE', 'XBT', 'CTD', 'DAP', 'FTP']
      self.mc = ais.mc
      self.asc = AssociationServiceClient(proc=ais)
      self.rc = ResourceClient(proc=ais)
      self.eclc = EPUControllerListClient(proc=ais)
        

   @defer.inlineCallbacks
   def getResourceTypes (self, msg):
      log.debug('ManageResources.getResourceTypes()\n'+str(msg))
      
      # no input for this message, just build AIS response with list of resource types
      Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='AIS getResourceTypes response')
      Response.message_parameters_reference.add()
      Response.message_parameters_reference[0] = Response.CreateObject(GET_RESOURCE_TYPES_RESPONSE_TYPE)
      for Type in self.ResourceTypes:
         log.info("Appending type=%s, name=%s"%(Type, self.ResourceTypes[Type]))
         Response.message_parameters_reference[0].resource_types_list.append(Type)
      Response.result = Response.ResponseCodes.OK
      log.debug('ManageResources.getResourceTypes(): returning\n'+str(Response))
      defer.returnValue(Response)


   @defer.inlineCallbacks
   def __findEpuControllers(self):
      log.debug('__findEpuControllers')
      d = DictObj
      d.idrefs = yield self.eclc.list()
      log.debug('__findEpuControllers: returning '+str(d))
      defer.returnValue(d)


   @defer.inlineCallbacks
   def __findResourcesOfType(self, resourceType):

      if resourceType == EPU_CONTROLLER_TYPE_ID:
         # get the resources from the EPU management
         result = yield self.__findEpuControllers()
         defer.returnValue(result)
      
      # get the resources out of the Association Service
      request = yield self.mc.create_instance(PREDICATE_OBJECT_QUERY_TYPE)
      pair = request.pairs.add()

      # Set the predicate search term
      pref = request.CreateObject(PREDICATE_REFERENCE_TYPE)
      pref.key = TYPE_OF_ID
      pair.predicate = pref

      # Set the Object search term
      type_ref = request.CreateObject(IDREF_TYPE)
      type_ref.key = resourceType    
      pair.object = type_ref

      result = yield self.asc.get_subjects(request)     
      defer.returnValue(result)

        
   def __PrintDatasetAttributes(self, ds):
      return   # TODO: turned off until problem with dataset fixed
      log.debug("Dataset = \n"+str(ds))
      for var in ds.root_group.variables:
         log.debug('Root Variable: %s' % str(var.name))
         for atrib in var.attributes:
            log.debug("Attribute: %s = %s" % (str(atrib.name), str(atrib.GetValue())))
         print "....Dimensions:"
         for dim in var.shape:
            log.debug("    ....%s (%s)" % (str(dim.name), str(dim.length)))
     

   def __PrintIdentityAttributes(self, ds):
      log.debug("Identity = \n"+str(ds))
    

   def __PrintDatasourceAttributes(self, ds):
      log.debug("Datasource = \n"+str(ds))
      log.debug('source_type: ' + self.SourceTypes[ds.source_type])
      for property in ds.property:
          log.debug('Property: ' + property)
      for sid in ds.station_id:
          log.debug('Station ID: ' + sid)
      log.debug('request_type: ' + self.RequestTypes[ds.request_type])
      log.debug('base_url: ' + ds.base_url)
      log.debug('max_ingest_millis: ' + str(ds.max_ingest_millis))


   def __PrintEpucontrollerAttributes(self, ds):
      log.debug('de_state = '+str(ds['de_state']))
      log.debug('de_conf_report = '+str(ds['de_conf_report']))
      log.debug('last_queuelen_size = '+str(ds['last_queuelen_size']))
      log.debug('last_queuelen_time = '+str(ds['last_queuelen_time']))
      for instance in ds['instances']:
         log.debug('Instance Name = '+instance)
         log.debug('iaas_state = '+ds['instances'][instance]['iaas_state'])
         log.debug('iaas_state = '+str(ds['instances'][instance]['iaas_state_time']))
         log.debug('iaas_state = '+str(ds['instances'][instance]['heartbeat_time']))
         log.debug('iaas_state = '+ds['instances'][instance]['heartbeat_state'])
    

   def __LoadDatasetColumnHeadrers(self, To):
      To.column_names.append('OoiId')
      To.column_names.append('Title')


   def __LoadIdentityColumnHeadrers(self, To):
      To.column_names.append('OoiId')
      To.column_names.append('Name')
      To.column_names.append('Email')
      To.column_names.append('Institution')
      To.column_names.append('Subject')


   def __LoadDatasourceColumnHeadrers(self, To):
      To.column_names.append('OoiId')
      To.column_names.append('Station ID')


   def __LoadEpucontrollerColumnHeadrers(self, To):
      To.column_names.append('EPU controller Id')


   def __LoadDatasetColumnData(self, To, From, Id):
      try:
         To.attribute.append(Id)
         To.attribute.append(From.root_group.FindAttributeByName('title').GetValue())
      
      except:
         estr = 'Object ERROR!'
         log.exception(estr)


   def __LoadIdentityColumnData(self, To, From, Id):
      try:
         To.attribute.append(Id)
         To.attribute.append(From.name)
         To.attribute.append(From.email)
         To.attribute.append(From.institution)
         To.attribute.append(From.subject)
      
      except:
         estr = 'Object ERROR!'
         log.exception(estr)


   def __LoadDatasourceColumnData(self, To, From, Id):
      #log.debug("To is:\n"+To.MessageType)
      try:
         To.attribute.append(Id)
         To.attribute.append(From.station_id[0])
      
      except:
         estr = 'Object ERROR!'
         log.exception(estr)


   def __LoadEpucontrollerColumnData(self, To, From):
      
      try:
         To.attribute.append(From)
      
      except:
         estr = 'Object ERROR!'
         log.exception(estr)


   @defer.inlineCallbacks
   def getResourcesOfType (self, msg):
      log.debug('ManageResources.getResourcesOfType()\n'+str(msg))
      
      # check that the GPB is correct type & has a payload
      result = yield self._CheckRequest(msg)
      if result != None:
         defer.returnValue(result)
         
      # check that resourceType is present in GPB
      if not msg.message_parameters_reference.IsFieldSet('resource_type'):
         # build AIS error response
         Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
         Response.error_num = Response.ResponseCodes.BAD_REQUEST
         Response.error_str = "Required field [resource_type] not found in message (AIS)"
         defer.returnValue(Response)

      # check for known resource type
      if msg.message_parameters_reference.resource_type in self.ResourceTypes:
         ResourceType = self.ResourceTypes[msg.message_parameters_reference.resource_type][0]
         LoaderFunc = self.ResourceTypes[msg.message_parameters_reference.resource_type][1]
         PrintFunc = self.ResourceTypes[msg.message_parameters_reference.resource_type][2]
         HeaderFunc = self.ResourceTypes[msg.message_parameters_reference.resource_type][3]
         log.debug('resource type [%s] is %s'%(msg.message_parameters_reference.resource_type, ResourceType))
      else:
         # build AIS error response
         log.debug('resource type ' + msg.message_parameters_reference.resource_type + ' is unknown')
         Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
         Response.error_num = Response.ResponseCodes.BAD_REQUEST
         Response.error_str = "Unknown resource type [%s] in message (AIS)"%msg.message_parameters_reference.resource_type
         defer.returnValue(Response)

      # Get the list of resource IDs for this type of resource
      Result = yield self.__findResourcesOfType(ResourceType)
      log.debug('Found ' + str(len(Result.idrefs)) + ' resources.')

      # build AIS response 
      Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='AIS getResourcesOfType response')
      Response.message_parameters_reference.add()
      Response.message_parameters_reference[0] = Response.CreateObject(GET_RESOURCES_OF_TYPE_RESPONSE_TYPE)
      Response.result = Response.ResponseCodes.OK
     
      # load the column headers for this resource type into response
      HeaderFunc(Response.message_parameters_reference[0])

      # load the attributes for each resource that was found into response
      i = 0
      while i < len(Result.idrefs):
         # load the attributes of the resource into response
         Response.message_parameters_reference[0].resources.add()
         if ResourceType == EPU_CONTROLLER_TYPE_ID:
            LoaderFunc(Response.message_parameters_reference[0].resources[i], Result.idrefs[i])
         else:
            # need to get the actual resource from it's ooi_id
            ResID = Result.idrefs[i].key
            log.debug('Working on ResID: ' + ResID)        
            Resource = yield self.rc.get_instance(ResID)
            # debug print for dumping the attributes of the resource
            PrintFunc(Resource)           
            LoaderFunc(Response.message_parameters_reference[0].resources[i], Resource, ResID)
         i = i + 1

      log.debug('ManageResources.getResourcesOfType(): returning\n'+str(Response))        
      defer.returnValue(Response)


   def __LoadDatasetAttributes(self, To, From):
      try:
         i = 0
         for atrib in From.root_group.attributes:
            #log.debug('Root Attribute: %s = %s'  % (str(atrib.name), str(atrib.GetValue())))
            To.resource.add()
            To.resource[i].name = str(atrib.name)
            To.resource[i].value = str(atrib.GetValue())
            i = i + 1
      
      except:
         estr = 'Object ERROR!'
         log.exception(estr)


   def __LoadIdentityAttributes(self, To, From):
      try:
         To.resource.add()
         To.resource[0].name = 'subject'
         To.resource[0].value = From.subject
         To.resource.add()
         To.resource[1].name = 'name'
         To.resource[1].value = From.name
         To.resource.add()
         To.resource[2].name = 'institution'
         To.resource[2].value = From.institution
         To.resource.add()
         To.resource[3].name = 'authenticating organization'
         To.resource[3].value = From.authenticating_organization
         To.resource.add()
         To.resource[4].name = 'email'
         To.resource[4].value = From.email
         i = 5
         for item in From.profile:
            To.resource.add()
            To.resource[i].name = item.name
            To.resource[i].value = item.value
            i = i + 1
      
      except:
         estr = 'Object ERROR!'
         log.exception(estr)


   def __LoadDatasourceAttributes(self, To, From):
      try:
         To.resource.add()
         To.resource[0].name = 'source_type'
         To.resource[0].value = self.SourceTypes[From.source_type]
         To.resource.add()
         To.resource[1].name = 'property'
         To.resource[1].value = From.property[0]
         To.resource.add()
         To.resource[2].name = 'station_id'
         To.resource[2].value = From.station_id[0]
         To.resource.add()
         To.resource[3].name = 'request_type'
         To.resource[3].value = self.RequestTypes[From.request_type]
         To.resource.add()
         To.resource[4].name = 'request_bounds_north'
         To.resource[4].value = str(From.request_bounds_north)
         To.resource.add()
         To.resource[5].name = 'request_bounds_south'
         To.resource[5].value = str(From.request_bounds_south)
         To.resource.add()
         To.resource[6].name = 'request_bounds_west'
         To.resource[6].value = str(From.request_bounds_west)
         To.resource.add()
         To.resource[7].name = 'request_bounds_east'
         To.resource[7].value = str(From.request_bounds_east)
         To.resource.add()
         To.resource[8].name = 'base_url'
         To.resource[8].value = From.base_url
         To.resource.add()
         To.resource[9].name = 'dataset_url'
         To.resource[9].value = From.dataset_url
         To.resource.add()
         To.resource[10].name = 'ncml_mask'
         To.resource[10].value = From.ncml_mask
         To.resource.add()
         To.resource[11].name = 'max_ingest_millis'
         To.resource[11].value = str(From.max_ingest_millis)
         To.resource.add()
         To.resource[12].name = 'ion_title'
         To.resource[12].value = From.ion_title
         To.resource.add()
         To.resource[13].name = 'ion_institution_id'
         To.resource[13].value = From.ion_institution_id
         To.resource.add()
         To.resource[14].name = 'update_interval_seconds'
         To.resource[14].value = str(From.update_interval_seconds)
      
      except:
         estr = 'Object ERROR!'
         log.exception(estr)


   def __LoadEpucontrollerAttributes(self, To, From):
      try:
         To.resource.add()
         To.resource[0].name = 'Decision Engine State'
         To.resource[0].value = From['de_state']
         To.resource.add()
         To.resource[1].name = 'Decision Engine Configuration Report'
         To.resource[1].value = From['de_conf_report']
         To.resource.add()
         To.resource[2].name = 'Last Queue Length Size'
         To.resource[2].value = str(From['last_queuelen_size'])
         To.resource.add()
         To.resource[3].name = 'Last Queue Length Time'
         To.resource[3].value = str(From['last_queuelen_time'])
         i = 4
         for instance in From['instances']:
            To.resource.add()
            To.resource[i].name = 'Instance Name'
            To.resource[i].value = instance
            i = i + 1
            To.resource.add()
            To.resource[i].name = 'Instance State'
            To.resource[i].value = From['instances'][instance]['iaas_state']
            i = i + 1
            To.resource.add()
            To.resource[i].name = 'Instance State Time'
            To.resource[i].value = str(From['instances'][instance]['iaas_state_time'])
            i = i + 1
            To.resource.add()
            To.resource[i].name = 'Heartbeat Time'
            To.resource[i].value = str(From['instances'][instance]['heartbeat_time'])
            i = i + 1
            To.resource.add()
            To.resource[i].name = 'Heartbeat State'
            To.resource[i].value = From['instances'][instance]['heartbeat_state']
    
      except:
         estr = 'Object ERROR!'
         log.exception(estr)


   @defer.inlineCallbacks
   def __GetEpuControllerInfo(self, Id):
      ecc = EPUControllerClient(targetname=Id)
      Result = yield ecc.whole_state()
      defer.returnValue(Result)


   @defer.inlineCallbacks
   def getResource (self, msg):
      log.debug('ManageResources.getResource()\n'+str(msg))
      
      # check that the GPB is correct type & has a payload
      result = yield self._CheckRequest(msg)
      if result != None:
         defer.returnValue(result)
         
      # check that ooi_id is present in GPB
      if not msg.message_parameters_reference.IsFieldSet('ooi_id'):
         # build AIS error response
         Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
         Response.error_num = Response.ResponseCodes.BAD_REQUEST
         Response.error_str = "Required field [ooi_id] not found in message (AIS)"
         defer.returnValue(Response)
         
      if 'epu_controller' in msg.message_parameters_reference.ooi_id:
         Result = yield self.__GetEpuControllerInfo(msg.message_parameters_reference.ooi_id)
         # debug print for dumping the attributes of the resource
         log.debug("got back resource \n"+str(Result))
         ResourceType = 'epucontrollers'
      else:
         # get resource from resource registry
         log.debug("attempting to get resource with id = "+msg.message_parameters_reference.ooi_id)
         try:
            Result = yield self.rc.get_instance(msg.message_parameters_reference.ooi_id)
         except ApplicationError, ex:
            # build AIS error response
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS getResource error response')
            Response.error_num = Response.ResponseCodes.NOT_FOUND
            Response.error_str = 'Error calling get_instance (AIS): '+str(ex)
            defer.returnValue(Response)
   
         # debug print for dumping the attributes of the resource
         log.debug("got back resource \n"+str(Result))
         log.debug("object GPB id = "+str(Result.ResourceObjectType.object_id))
         ResourceType = self.MapGpbTypeToResourceType[Result.ResourceObjectType.object_id]

      PrintFunc = self.ResourceTypes[ResourceType][2]
      PrintFunc(Result)
      
      # build AIS response
      Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='AIS getResource response')
      Response.message_parameters_reference.add()
      Response.message_parameters_reference[0] = Response.CreateObject(GET_RESOURCE_RESPONSE_TYPE)
      LoaderFunc = self.ResourceTypes[ResourceType][4]
      LoaderFunc(Response.message_parameters_reference[0], Result)
      Response.result = Response.ResponseCodes.OK
      log.debug('ManageResources.getResource(): returning\n'+str(Response))
      defer.returnValue(Response)


   @defer.inlineCallbacks
   def _CheckRequest(self, request):
      # Check for correct request protocol buffer type
      if request.MessageType != AIS_REQUEST_MSG_TYPE:
         # build AIS error response
         Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
         Response.error_num = Response.ResponseCodes.BAD_REQUEST
         Response.error_str = 'Bad message type receieved, ignoring (AIS)'
         defer.returnValue(Response)

      # Check payload in message
      if not request.IsFieldSet('message_parameters_reference'):
         # build AIS error response
         Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
         Response.error_num = Response.ResponseCodes.BAD_REQUEST
         Response.error_str = "Required field [message_parameters_reference] not found in message (AIS)"
         defer.returnValue(Response)
  
      defer.returnValue(None)
