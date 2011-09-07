#!/usr/bin/env python

"""
@file ion/integration/ais/ManageResource/ManageResources.py
@author Bill Bollenbcher
@brief The worker class that implements the ManageResources function for the AIS  (workflow #109)
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
import logging
from twisted.internet import defer
import time

from ion.core.exception import ApplicationError
from ion.services.dm.inventory.association_service import AssociationServiceClient
from ion.services.coi.resource_registry.resource_client import ResourceClient
from ion.services.cei.epu_controller_client import EPUControllerClient
from ion.services.cei.epu_controller_list_client import EPUControllerListClient

from ion.integration.ais.ais_object_identifiers import AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE, \
                                                       GET_RESOURCE_TYPES_RESPONSE_TYPE,\
                                                       GET_RESOURCES_OF_TYPE_RESPONSE_TYPE, \
                                                       GET_RESOURCE_RESPONSE_TYPE
from ion.core.object import object_utils
from ion.services.dm.inventory.association_service import PREDICATE_OBJECT_QUERY_TYPE, IDREF_TYPE
from ion.services.coi.datastore_bootstrap.ion_preload_config import IDENTITY_RESOURCE_TYPE_ID, \
                                                                    TYPE_OF_ID, \
                                                                    DATASET_RESOURCE_TYPE_ID, \
                                                                    DATASOURCE_RESOURCE_TYPE_ID
from ion.core.intercept.policy import get_current_roles, all_roles

PREDICATE_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=25, version=1)

EPU_CONTROLLER_TYPE_ID = 'dummy_type_id_for_epu_controllers'
DATASET_KEY = 'datasets'
DATASOURCE_KEY = 'datasources'
IDENTITY_KEY = 'identities'
EPUCONTROLLER_KEY = 'epucontrollers'


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
      self.ResourceTypes = {DATASET_KEY : DatasetValues,
                            IDENTITY_KEY : IdentityValues,
                            DATASOURCE_KEY : DatasourceValues,
                            EPUCONTROLLER_KEY : EpucontrollerValues
                           }
      self.MapGpbTypeToResourceType = {10001 : DATASET_KEY,
                                       1401 : IDENTITY_KEY,
                                       4503 : DATASOURCE_KEY                                     
                                       }
      self.SourceTypes = ['', 'SOS', 'USGS', 'AOML', 'NETCDF_S', 'NETCDF_C']
      self.RequestTypes = ['', 'NONE', 'XBT', 'CTD', 'DAP', 'FTP']

      self.mc = ais.mc
      self.asc = AssociationServiceClient(proc=ais)
      self.rc = ResourceClient(proc=ais)
      self.eclc = EPUControllerListClient(proc=ais)
      self.metadataCache = ais.getMetadataCache()


   @defer.inlineCallbacks
   def getResourceTypes (self, msg):
      if log.getEffectiveLevel() <= logging.DEBUG:
         log.debug('ManageResources.getResourceTypes()\n'+str(msg))
      
      # no input for this message, just build AIS response with list of resource types
      Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='AIS getResourceTypes response')
      Response.message_parameters_reference.add()
      Response.message_parameters_reference[0] = Response.CreateObject(GET_RESOURCE_TYPES_RESPONSE_TYPE)
      for Type in self.ResourceTypes:
         log.info("Appending type=%s, name=%s"%(Type, self.ResourceTypes[Type]))
         Response.message_parameters_reference[0].resource_types_list.append(Type)
      Response.result = Response.ResponseCodes.OK
      if log.getEffectiveLevel() <= logging.DEBUG:
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
      for instance in ds['instances']:
         log.debug('Instance Name = '+instance)
         log.debug('Instance IAAS ID', ds['instances'][instance]['iaas_id'])
         log.debug('Instance Public IP', ds['instances'][instance]['public_ip'])
         log.debug('Instance Private IP', ds['instances'][instance]['private_ip'])
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
      To.column_names.append('Role')


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
         if not From.IsFieldSet('name'):
            To.attribute.append("")
         else:
            To.attribute.append(From.name)
         if not From.IsFieldSet('email'):
            To.attribute.append("")
         else:
            To.attribute.append(From.email)
         if not From.IsFieldSet('institution'):
            To.attribute.append("")
         else:
            To.attribute.append(From.institution)
         if not From.IsFieldSet('subject'):
            To.attribute.append("")
         else:
            To.attribute.append(From.subject)

         Role = ', '.join([all_roles[role] for role in get_current_roles(Id)])
         To.attribute.append(Role)
      
      except:
         estr = 'Object ERROR!'
         log.exception(estr)


   def __LoadDatasourceColumnData(self, To, From, Id):
      #log.debug("To is:\n"+To.MessageType)
      try:
         To.attribute.append(Id)
         if not From.IsFieldSet('station_id'):            
            To.attribute.append("")
         else:
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
      if log.getEffectiveLevel() <= logging.DEBUG:
         log.debug('ManageResources.getResourcesOfType()\n'+str(msg))
      
      # check that the GPB is correct type & has a payload
      result = yield self._CheckRequest(msg)
      if result != None:
         result.error_str = "AIS.getResourcesOfType: " + result.error_str
         defer.returnValue(result)
         
      # check that resourceType is present in GPB
      if not msg.message_parameters_reference.IsFieldSet('resource_type'):
         # build AIS error response
         Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS getResourcesOfType error response')
         Response.error_num = Response.ResponseCodes.BAD_REQUEST
         Response.error_str = "AIS.getResourcesOfType: Required field [resource_type] not found in message"
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
         Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS getResourcesOfType error response')
         Response.error_num = Response.ResponseCodes.BAD_REQUEST
         Response.error_str = "AIS.getResourcesOfType: Unknown resource type [%s] in message"%msg.message_parameters_reference.resource_type
         defer.returnValue(Response)

      try:
          # Get the list of resource IDs for this type of resource
          Result = yield self.__findResourcesOfType(ResourceType)
          log.debug('Found ' + str(len(Result.idrefs)) + ' resources.')
      except ApplicationError, ex:
          # build AIS error response
          Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS getResourcesOfType error response')
          Response.error_num = Response.ResponseCodes.NOT_FOUND
          Response.error_str = 'AIS.getResourcesOfType: Error calling __findResourcesOfType: '+str(ex)
          defer.returnValue(Response)
   
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
            try:
                Resource = yield self.rc.get_instance(ResID)
            except ApplicationError, ex:
                # build AIS error response
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS getResource error response')
                Response.error_num = Response.ResponseCodes.NOT_FOUND
                Response.error_str = 'AIS.getResourcesOfType: Error calling get_instance: '+str(ex)
                defer.returnValue(Response)
            # debug print for dumping the attributes of the resource
            PrintFunc(Resource)           
            LoaderFunc(Response.message_parameters_reference[0].resources[i], Resource, ResID)
         i = i + 1

      if log.getEffectiveLevel() <= logging.DEBUG:
         log.debug('ManageResources.getResourcesOfType(): returning\n'+str(Response))        
      defer.returnValue(Response)


   @defer.inlineCallbacks
   def __LoadDatasetAttributes(self, To, From):
      
      class namespace: pass   # stupid hack to get around python variable scoping limitation

      def AddItem(Name, Value):  # worker function to hide ugly GPB methodology
         To.resource.add()
         To.resource[ns.Index].name = Name
         To.resource[ns.Index].value = Value
         ns.Index = ns.Index + 1
         
      Result = yield self.rc.get_instance(From.ResourceTypeID.key)
      ns = namespace()   # create wrapper class for scoping so worker function can set variable
      ns.Index = 0 

      try:
         for atrib in From.root_group.attributes:
            #log.debug('Root Attribute: %s = %s'  % (str(atrib.name), str(atrib.GetValue())))
            AddItem(str(atrib.name), str(atrib.GetValue()))
         AddItem('LifeCycleState', From.ResourceLifeCycleState)
         AddItem('fields set in data set resource', str(From.ListSetFields()).replace("'", ""))
         AddItem('resource identity', From.ResourceIdentity)
         AddItem('resource object type', str(From.ResourceObjectType).replace('\n', ', ', 1).strip())
         AddItem('resource type name', Result.Repository._workspace_root.name)
         AddItem('resource name', From.ResourceName)
         AddItem('resource description', From.ResourceDescription)
      
      except:
         estr = 'Object ERROR!'
         log.exception(estr)
         
      defer.returnValue(ns.Index)


   @defer.inlineCallbacks
   def __LoadIdentityAttributes(self, To, From):   
      
      class namespace: pass   # stupid hack to get around python variable scoping limitation

      def AddItem(Name, Value):  # worker function to hide ugly GPB methodology
         To.resource.add()
         To.resource[ns.Index].name = Name
         To.resource[ns.Index].value = Value
         ns.Index = ns.Index + 1
         
      Result = yield self.rc.get_instance(From.ResourceTypeID.key)
      ns = namespace()   # create wrapper class for scoping so worker function can set variable
      ns.Index = 0 

      try:
         AddItem('subject', From.subject)
         AddItem('name', From.name)
         AddItem('institution', From.institution)
         AddItem('authenticating organization', From.authenticating_organization)
         AddItem('email', From.email)
         for item in From.profile:
            AddItem(item.name, item.value)
         AddItem('LifeCycleState', From.ResourceLifeCycleState)
         AddItem('fields set in identity resource', str(From.ListSetFields()).replace("'", ""))
         AddItem('resource identity', From.ResourceIdentity)
         AddItem('resource object type', str(From.ResourceObjectType).replace('\n', ', ', 1).strip())
         AddItem('resource type name', Result.Repository._workspace_root.name)
         AddItem('resource name', From.ResourceName)
         AddItem('resource description', From.ResourceDescription)
              
      except:
         estr = 'Object ERROR!'
         log.exception(estr)
         
      defer.returnValue(ns.Index)
      
         
         
   @defer.inlineCallbacks
   def __LoadDatasourceAttributes(self, To, From):
      
      class namespace: pass   # stupid hack to get around python variable scoping limitation

      def AddItem(Name, Value):  # worker function to hide ugly GPB methodology
         To.resource.add()
         To.resource[ns.Index].name = Name
         To.resource[ns.Index].value = Value
         ns.Index = ns.Index + 1
         
      Result = yield self.rc.get_instance(From.ResourceTypeID.key)
      ns = namespace()   # create wrapper class for scoping so worker function can set variable
      ns.Index = 0 

      try:
         AddItem('source_type', self.SourceTypes[From.source_type])
         AddItem('property', From.property[0])
         AddItem('station_id', From.station_id[0])
         AddItem('request_type', self.RequestTypes[From.request_type])
         AddItem('request_bounds_north', str(From.request_bounds_north))
         AddItem('request_bounds_south', str(From.request_bounds_south))
         AddItem('request_bounds_west', str(From.request_bounds_west))
         AddItem('request_bounds_east', str(From.request_bounds_east))
         AddItem('base_url', From.base_url)
         AddItem('dataset_url', From.dataset_url)
         AddItem('ncml_mask', From.ncml_mask)
         AddItem('max_ingest_millis', str(From.max_ingest_millis))
         AddItem('ion_title', From.ion_title)
         AddItem('ion_description', From.ion_description)
         AddItem('registration_datetime_millis', str(From.registration_datetime_millis))
         AddItem('ion_institution_id', From.ion_institution_id)
         AddItem('update_interval_seconds', str(From.update_interval_seconds))
         AddItem('visualization_url', From.visualization_url)
         AddItem('is_public', str(From.is_public))
         AddItem('LifeCycleState', From.ResourceLifeCycleState)
         AddItem('fields set in data source resource', str(From.ListSetFields()).replace("'", ""))
         AddItem('resource identity', From.ResourceIdentity)
         AddItem('resource object type', str(From.ResourceObjectType).replace('\n', ', ', 1).strip())
         AddItem('resource type name', Result.Repository._workspace_root.name)
         AddItem('resource name', From.ResourceName)
         AddItem('resource description', From.ResourceDescription)
      
      except:
         estr = 'Object ERROR!'
         log.exception(estr)
         
      defer.returnValue(ns.Index)


   def __LoadEpucontrollerAttributes(self, To, From):
      
      class namespace: pass   # stupid hack to get around python variable scoping limitation

      def AddItem(Name, Value):  # worker function to hide ugly GPB methodology
         To.resource.add()
         To.resource[ns.Index].name = Name
         if Value == None:
            To.resource[ns.Index].value = 'None'
         else:
            To.resource[ns.Index].value = Value
         ns.Index = ns.Index + 1
         
      ns = namespace()   # create wrapper class for scoping so worker function can set variable
      ns.Index = 0
      
      try:
         AddItem('Decision Engine State', From['de_state'])
         AddItem('Decision Engine Configuration Report', From['de_conf_report'])
         for instance in From['instances']:
            AddItem('Instance Name', instance)
            AddItem('Instance IAAS ID', From['instances'][instance]['iaas_id'])
            AddItem('Instance Public IP', From['instances'][instance]['public_ip'])
            AddItem('Instance Private IP', From['instances'][instance]['private_ip'])
            AddItem('Instance State', From['instances'][instance]['iaas_state'])
            if From['instances'][instance]['iaas_state_time'] == None:
               AddItem('Instance State Time', 'None')
            else:
               AddItem('Instance State Time', time.strftime("%a %b %d %Y %H:%M:%S", time.localtime(From['instances'][instance]['iaas_state_time'])))
            if From['instances'][instance]['heartbeat_time'] == None:
               AddItem('Heartbeat Time', 'None')
            else:
               AddItem('Heartbeat Time', time.strftime("%a %b %d %Y %H:%M:%S", time.localtime(From['instances'][instance]['heartbeat_time'])))
            AddItem('Heartbeat State', From['instances'][instance]['heartbeat_state'])
    
      except:
         estr = 'Object ERROR!'
         log.exception(estr)
         
      return ns.Index


   @defer.inlineCallbacks
   def __GetEpuControllerInfo(self, Id):
      ecc = EPUControllerClient(targetname=Id)
      Result = yield ecc.whole_state()
      defer.returnValue(Result)


   @defer.inlineCallbacks
   def getResource (self, msg):
      if log.getEffectiveLevel() <= logging.DEBUG:
         log.debug('ManageResources.getResource()\n'+str(msg))
      
      # check that the GPB is correct type & has a payload
      result = yield self._CheckRequest(msg)
      if result != None:
         result.error_str = "AIS.getResource: " + result.error_str
         defer.returnValue(result)
         
      # check that ooi_id is present in GPB
      if not msg.message_parameters_reference.IsFieldSet('ooi_id'):
         # build AIS error response
         Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS getResource error response')
         Response.error_num = Response.ResponseCodes.BAD_REQUEST
         Response.error_str = "AIS.getResource: Required field [ooi_id] not found in message"
         defer.returnValue(Response)
         
      if 'epu_controller' in msg.message_parameters_reference.ooi_id:
         try:
            Result = yield self.__GetEpuControllerInfo(msg.message_parameters_reference.ooi_id)
         except ApplicationError, ex:
            # build AIS error response
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS getResource error response')
            Response.error_num = Response.ResponseCodes.NOT_FOUND
            Response.error_str = 'AIS.getResource: Error calling __GetEpuControllerInfo: '+str(ex)
            defer.returnValue(Response)
         # debug print for dumping the attributes of the resource
         if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug("got back resource \n"+str(Result))
         ResourceType = EPUCONTROLLER_KEY
      else:
         # get resource from resource registry
         log.debug("attempting to get resource with id = "+msg.message_parameters_reference.ooi_id)
         try:
            Result = yield self.rc.get_instance(msg.message_parameters_reference.ooi_id)
         except ApplicationError, ex:
            # build AIS error response
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS getResource error response')
            Response.error_num = Response.ResponseCodes.NOT_FOUND
            Response.error_str = 'AIS.getResource: Error calling get_instance: '+str(ex)
            defer.returnValue(Response)
   
         # debug print for dumping the attributes of the resource
         if log.getEffectiveLevel() <= logging.DEBUG:
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
      Index = yield LoaderFunc(Response.message_parameters_reference[0], Result)
      if (ResourceType == DATASET_KEY):
         ResourceID = yield self.metadataCache.getAssociatedSource(Result.ResourceIdentity)
         Response.message_parameters_reference[0].resource.add()
         Response.message_parameters_reference[0].resource[Index].name = "Data Source ID"
         Response.message_parameters_reference[0].resource[Index].value = ResourceID
      elif (ResourceType == DATASOURCE_KEY):
         ResourceIDs = yield self.metadataCache.getAssociatedDatasets(Result)
         if len(ResourceIDs) == 0:
            ResourceID = 'None'
         else:
            ResourceID = ResourceIDs[0]
         Response.message_parameters_reference[0].resource.add()
         Response.message_parameters_reference[0].resource[Index].name = "Data Set ID"
         Response.message_parameters_reference[0].resource[Index].value = ResourceID
      Response.result = Response.ResponseCodes.OK
      if log.getEffectiveLevel() <= logging.DEBUG:
         log.debug('ManageResources.getResource(): returning\n'+str(Response))
      defer.returnValue(Response)


   @defer.inlineCallbacks
   def _CheckRequest(self, request):
      # Check for correct request protocol buffer type
      if request.MessageType != AIS_REQUEST_MSG_TYPE:
         # build AIS error response
         Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
         Response.error_num = Response.ResponseCodes.BAD_REQUEST
         Response.error_str = 'Bad message type receieved, ignoring'
         defer.returnValue(Response)

      # Check payload in message
      if not request.IsFieldSet('message_parameters_reference'):
         # build AIS error response
         Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
         Response.error_num = Response.ResponseCodes.BAD_REQUEST
         Response.error_str = "Required field [message_parameters_reference] not found in message"
         defer.returnValue(Response)
  
      defer.returnValue(None)
