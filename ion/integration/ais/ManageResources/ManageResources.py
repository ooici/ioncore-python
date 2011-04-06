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
from ion.core.exception import ReceivedApplicationError, ReceivedContainerError
from ion.services.dm.inventory.association_service import AssociationServiceClient
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient

from ion.services.coi.datastore_bootstrap.ion_preload_config import topic_res_type_name, \
                                                                    dataset_res_type_name, \
                                                                    identity_res_type_name, \
                                                                    datasource_res_type_name

from ion.integration.ais.ais_object_identifiers import AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE, \
                                                       GET_RESOURCE_TYPES_RESPONSE_TYPE,\
                                                       GET_RESOURCES_OF_TYPE_REQUEST_TYPE, \
                                                       GET_RESOURCES_OF_TYPE_RESPONSE_TYPE
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
                                                                    TOPIC_RESOURCE_TYPE_ID, \
                                                                    DATASET_RESOURCE_TYPE_ID, \
                                                                    IDENTITY_RESOURCE_TYPE_ID, \
                                                                    DATASOURCE_RESOURCE_TYPE_ID
                                                                    

PREDICATE_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=25, version=1)

class ManageResources(object):
    
   def __init__(self, ais):
      log.debug('ManageResources.__init__()')
      TopicValues = TOPIC_RESOURCE_TYPE_ID, \
                    self.__LoadTopicAttributes, \
                    self.__PrintTopicAttributes, \
                    self.__LoadTopicColumnHeadrers
      DatasetValues = DATASET_RESOURCE_TYPE_ID, \
                      self.__LoadDatasetAttributes, \
                      self.__PrintDatasetAttributes, \
                      self.__LoadDatasetColumnHeadrers
      IdentityValues = IDENTITY_RESOURCE_TYPE_ID, \
                       self.__LoadIdentityAttributes, \
                       self.__PrintIdentityAttributes, \
                       self.__LoadIdentityColumnHeadrers
      DatasourceValues = DATASOURCE_RESOURCE_TYPE_ID, \
                         self.__LoadDatasourceAttributes, \
                         self.__PrintDatasourceAttributes, \
                         self.__LoadDatasourceColumnHeadrers
      self.ResourceTypes = {'topics' : TopicValues,
                            'datasets' : DatasetValues,
                            'identities' : IdentityValues,
                            'datasources' : DatasourceValues
                           }
      self.mc = ais.mc
      self.asc = AssociationServiceClient()
      self.rc = ResourceClient()
        

   @defer.inlineCallbacks
   def __findResourcesOfType(self, resourceType):

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
   def getResourcesOfType (self, msg):
      log.debug('ManageResources.getResourcesOfType()\n'+str(msg))
      
      # check that the GPB is correct type & has a payload
      result = yield self.CheckRequest(msg)
      if result != None:
         defer.returnValue(result)
         
      # check that resourceType is present in GPB
      if not msg.message_parameters_reference.IsFieldSet('resource_type'):
         # build AIS error response
         Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE, MessageName='AIS error response')
         Response.error_num = Response.ResponseCodes.BAD_REQUEST
         Response.error_str = "Required field [resource_type] not found in message"
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
         Response.error_str = "Unknown resource type [%s] in message"%msg.message_parameters_reference.resource_type
         defer.returnValue(Response)

      # Get the list of resource IDs for this type of resource
      Result = yield self.__findResourcesOfType(ResourceType)
      log.debug('Found ' + str(len(Result.idrefs)) + ' resources.')

      # build AIS response with list of resources
      Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE, MessageName='AIS getResourcesOfType response')
      Response.message_parameters_reference.add()
      Response.message_parameters_reference[0] = Response.CreateObject(GET_RESOURCES_OF_TYPE_RESPONSE_TYPE)
      HeaderFunc(Response.message_parameters_reference[0])
      Response.result = Response.ResponseCodes.OK

      # load the attributes for each resource found
      i = 0
      while i < len(Result.idrefs):
         ResID = Result.idrefs[i].key
         log.debug('Working on ResID: ' + ResID)        
         Resource = yield self.rc.get_instance(ResID)
         PrintFunc(Resource)
         Response.message_parameters_reference[0].resources.add()
         LoaderFunc(Response.message_parameters_reference[0].resources[i], Resource)
         i = i + 1

      log.debug('ManageResources.getResourcesOfType(): returning\n'+str(Response))        
      defer.returnValue(Response)


   def __LoadTopicColumnHeadrers(self, To):
      To.column_names.append('title')


   def __LoadDatasetColumnHeadrers(self, To):
      To.column_names.append('title')


   def __LoadIdentityColumnHeadrers(self, To):
      To.column_names.append('subject')


   def __LoadDatasourceColumnHeadrers(self, To):
      To.column_names.append('base url')


   def __PrintTopicAttributes(self, ds):
      log.debug("Identity = \n"+str(ds))
    

   def __PrintDatasetAttributes(self, ds):
      print ds.ListSetFields()
      log.debug("Dataset = \n"+str(ds))
      for atrib in ds.root_group.attributes:
         log.debug('Root Attribute: %s = %s'  % (str(atrib.name), str(atrib.GetValue())))
    

   def __PrintIdentityAttributes(self, ds):
      log.debug("Identity = \n"+str(ds))
    

   def __PrintDatasourceAttributes(self, ds):
      log.debug("Datasource = \n"+str(ds))
      log.debug('source_type: ' + str(ds.source_type))
      for property in ds.property:
          log.debug('Property: ' + property)
      for sid in ds.station_id:
          log.debug('Station ID: ' + sid)
      log.debug('request_type: ' + str(ds.request_type))
      log.debug('base_url: ' + ds.base_url)
      log.debug('max_ingest_millis: ' + str(ds.max_ingest_millis))


   def __LoadTopicAttributes(self, To, From):
      try:
         To.resource_attribute.add()
         To.resource_attribute[0].name = 'title'
         To.resource_attribute[0].value = From.root_group.FindAttributeByName('title').GetValue()
      
      except:
         estr = 'Object ERROR!'
         log.exception(estr)


   def __LoadDatasetAttributes(self, To, From):
      #log.debug("To is:\n"+To.MessageType)
      try:
         To.resource_attribute.add()
         To.resource_attribute[0].name = 'title'
         To.resource_attribute[0].value = From.root_group.FindAttributeByName('title').GetValue()
      
      except:
         estr = 'Object ERROR!'
         log.exception(estr)


   def __LoadIdentityAttributes(self, To, From):
      try:
         To.resource_attribute.add()
         To.resource_attribute[0].name = 'subject'
         To.resource_attribute[0].value = From.subject
      
      except:
         estr = 'Object ERROR!'
         log.exception(estr)


   def __LoadDatasourceAttributes(self, To, From):
      #log.debug("To is:\n"+To.MessageType)
      try:
         To.resource_attribute.add()
         To.resource_attribute[0].name = 'base_url'
         To.resource_attribute[0].value = From.base_url
      
      except:
         estr = 'Object ERROR!'
         log.exception(estr)


   @defer.inlineCallbacks
   def CheckRequest(self, request):
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

      