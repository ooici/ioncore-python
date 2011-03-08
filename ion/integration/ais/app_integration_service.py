#!/usr/bin/env python

"""
@file ion/integration/app_integration_service.py
@author David Everett
@brief Core service frontend for Application Integration Services 
"""

import sys
import traceback

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.object import object_utils
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient, ResourceInstance
from ion.core.messaging.message_client import MessageClient

# import GPB type identifiers for AIS
from ion.integration.ais.ais_object_identifiers import AIS_REQUEST_MSG_TYPE, AIS_RESPONSE_MSG_TYPE
from ion.integration.ais.ais_object_identifiers import UPDATE_USER_EMAIL_TYPE, UPDATE_USER_DISPATCH_QUEUE_TYPE
from ion.integration.ais.ais_object_identifiers import FIND_DATA_RESOURCES_MSG_TYPE

# import working classes for AIS
from ion.integration.ais.findDataResources.findDataResources import FindDataResources
from ion.integration.ais.RegisterUser.RegisterUser import RegisterUser

addresslink_type = object_utils.create_type_identifier(object_id=20003, version=1)
person_type = object_utils.create_type_identifier(object_id=20001, version=1)


class AppIntegrationService(ServiceProcess):
    """
    Service to provide clients access to backend data
    """
    # Declaration of service
    declare = ServiceProcess.service_declare(name='app_integration',
                                             version='0.1.0',
                                             dependencies=[])

    def __init__(self, *args, **kwargs):
        ServiceProcess.__init__(self, *args, **kwargs)
        
        self.rc = ResourceClient(proc = self)
        self.mc = MessageClient(proc = self)
    
        log.info('AppIntegrationService.__init__()')

    def slc_init(self):
        pass

    @defer.inlineCallbacks
    def op_findDataResources(self, content, headers, msg):
        """
        @brief Find data resources associated with given userID
        @param reference to instrument protocol object.
        @retval none
        """
        log.info('op_findDataResources: '+str(content))
        try:
            worker = FindDataResources()
            # THIS WILL DO A YIELD IN NEAR FUTURE
            """
            Need to build up a GPB Message;
             - get request message object from message client
             - build up request  message
             - get find data resources message from message client
             - build up find data resources payload message
             - send to worker class
             - get results
             - get response message object from messagse client
             - build up response message
             - get response message payload
             - build up response message payload
            """

            returnValue = worker.findDataResources(msg)
            log.debug('worker returned: ' + returnValue)
            yield self.reply_ok(msg, {'value' : 'newDataResourceIdentity'})
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            yield self.reply_err(msg, estr)

        return

    @defer.inlineCallbacks
    def op_getDataResourceDetail(self, content, headers, msg):
        log.info('op_getDataResourceDetail: '+str(content))
        try:
            yield self.reply_ok(msg, {'value' : 'value'})
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            yield self.reply_err(msg, estr)

        return

    @defer.inlineCallbacks
    def op_createDownloadURL(self, content, headers, msg):
        log.info('op_createDownloadURL: '+str(content))
        try:
            userId = content['userId']
            dataResourceId = content['dataResourceId']
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            yield self.reply_err(msg, estr)
            return
        
        yield self.reply_ok(msg, {'value' : 'http://a.download.url.edu'})

    @defer.inlineCallbacks
    def op_registerUser(self, content, headers, msg):
        log.info('op_registerUser: \n'+str(content))
        try:
            worker = RegisterUser()
            worker.init(self)
            log.info('op_registerUser: calling worker')
            response = yield worker.registerUser(content);
            yield self.reply_ok(msg, response)
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            yield self.reply_err(msg, estr)

        return
        
    @defer.inlineCallbacks
    def findInstance(self, id, msg):
        try:
            yield self.rc.get_instance(id)
            yield self.reply_ok(msg, {'value' : 'value'})
        except Exception, ex:
            exInfo = self.formatExceptionInfo()
            estr = 'Object lookup failed with exception: ' + exInfo
            log.exception(estr)
            yield self.reply_err(msg, estr)
            return
       
    def formatExceptionInfo(self, maxTBlevel=5):
        cla, exc, trbk = sys.exc_info()
        excName = cla.__name__
        try:
            excArgs = exc.__dict__["args"]
        except KeyError:
            excArgs = "<no args>"
        excTb = traceback.format_exception(cla, exc, trbk)
        return '\n'.join(excTb)
                  

class AppIntegrationServiceClient(ServiceClient):
    """
    This is a service client for AppIntegrationServices.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "app_integration"
        ServiceClient.__init__(self, proc, **kwargs)
        
    @defer.inlineCallbacks
    #def findDataResources(self, userId, published, spacial, temporal):
    def findDataResources(self, msg):
        yield self._check_init()
        log.debug("findDataResources: sending %s message to findDataResources" % str(msg))
        (content, headers, payload) = yield self.rpc_send('findDataResources', msg)
        log.info('Service reply: ' + str(content))
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def getDataResourceDetail(self, userId, dataResourceId, detailType):
        yield self._check_init()
        payload = {'userId' : userId,
                   'dataResourceId' : dataResourceId,
                   'detailType' : detailType}
        (content, headers, payload) = yield self.rpc_send('getDataResourceDetail', payload)
        log.info('Service reply: ' + str(content))
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def createDownloadURL(self, userId, dataResourceId):
        yield self._check_init()
        payload = {'userId' : userId,
                   'dataResourceId' : dataResourceId}
        (content, headers, payload) = yield self.rpc_send('createDownloadURL', payload)
        log.info('Service reply: ' + str(content))
        defer.returnValue(content)
 
    @defer.inlineCallbacks
    def registerUser(self, message):
        yield self._check_init()
        (content, headers, payload) = yield self.rpc_send('registerUser', message)
        log.info('Service reply: ' + str(content))
        defer.returnValue(content)
        

# Spawn of the process using the module name
factory = ProcessFactory(AppIntegrationService)

