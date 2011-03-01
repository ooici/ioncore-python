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

dataResource_type = object_utils.create_type_identifier(object_id=2301, version=1)

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
        self.rc = ResourceClient(proc=self)
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
            yield self.reply_ok(msg, {'value' : 'newDataResourceIdentity'})
        #    userId = content['userId']
        #    published = content['published']
        #    spacial = content['spacial']
        #    temporal = content['temporal']
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            yield self.reply_err(msg, estr)

        return

    @defer.inlineCallbacks
    def op_createDataResource(self, dataResource, headers, msg):
        log.info('op_createDataResource:\n'+str(dataResource))
 
        try:
            yield self.reply_ok(msg, {'value' : 'value'})
        except:
            exInfo = self.formatExceptionInfo()
            estr = 'Object update failed with exception: ' + exInfo
            log.exception(estr)
            yield self.reply_err(msg, estr)

        return

    @defer.inlineCallbacks
    def op_updateDataResource(self, content, headers, msg):
        log.info('op_updateDataResource: '+str(content))
        
        try:
            yield self.reply_ok(msg, {'value' : 'value'})
        except Exception, ex:
            exInfo = self.formatExceptionInfo()
            estr = 'Object update failed with exception: ' + exInfo
            log.exception(estr)
            yield self.reply_err(msg, estr)

        return
        
    @defer.inlineCallbacks
    def op_deleteDataResource(self, content, headers, msg):
        log.info('op_deleteDataResource: '+str(content))
        try:
            yield self.reply_ok(msg, {'value' : 'value'})
        except Exception, ex:
            exInfo = self.formatExceptionInfo()
            estr = 'Object deletion failed with exception: ' + exInfo
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
    def op_findUserSubscriptions(self, content, headers, msg):
        log.info('op_findUserSubscriptions: '+str(content))
        try:
            yield self.reply_ok(msg, {'value' : 'value'})
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            yield self.reply_err(msg, estr)
            return
        
    @defer.inlineCallbacks
    def op_createSubscription(self, content, headers, msg):
        log.info('op_createSubscription: '+str(content))
        try:
            yield self.reply_ok(msg, {'value' : 'value'})
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            yield self.reply_err(msg, estr)
            return
        
    @defer.inlineCallbacks
    def op_deleteSubscription(self, content, headers, msg):
        log.info('op_deleteSubscription: '+str(content))
        try:
            yield self.reply_ok(msg, {'value' : 'value'})
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            yield self.reply_err(msg, estr)
            return
        
    @defer.inlineCallbacks
    def op_getUserNotifications(self, content, headers, msg):
        log.info('op_getUserNotifications: '+str(content))
        try:
            yield self.reply_ok(msg, {'value' : 'value'})
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            yield self.reply_err(msg, estr)
            return
        
    @defer.inlineCallbacks
    def op_updateNotification(self, content, headers, msg):
        log.info('op_updateNotification: '+str(content))

        try:
            yield self.reply_ok(msg, {'value' : 'value'})
        except KeyError:
            estr = 'Missing information in message!'
            log.exception(estr)
            yield self.reply_err(msg, estr)
            return
        
        yield self.reply_ok(msg, {'value' : ''})

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
        log.debug("findDataResources: sending message to findDataResources")
        #payload = {'userId' : userId,
        #           'published' : published,
        #           'spacial' : spacial,
        #           'temporal' : temporal}
        (content, headers, payload) = yield self.rpc_send('findDataResources', msg)
        #(content, headers, payload) = yield self.rpc_send('createDataResource', msg)
        #content = 'test'
        log.info('Service reply: ' + str(content))
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def createDataResource(self, msg):
        yield self._check_init()
        #(content, headers, payload) = yield self.rpc_send('createDataResource', payload)
        (content, headers, payload) = yield self.rpc_send('createDataResource', msg)
        log.info('Service reply: ' + str(content))
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def updateDataResource(self, userId, dataResourceId, provider, format, protocol, type, title, dataFormat, dataType,
                           namingAuthority, summary, publisherInstitution, publisherName,
                           publisherEmail, publisherWebsite, creatorInstitution, createName,
                           openDAP, wcs, wms, ncml, uddc, iso, viewer):
        yield self._check_init()
        payload = {'userId' : userId,
                   'dataResourceId' : dataResourceId,
                   'provider' : provider,
                   'format' : format,
                   'protocol' : protocol,
                   'type' : type,
                   'title' : title,
                   'dataFormat' : dataFormat,
                   'dataType' : dataType,
                   'namingAuthority' : namingAuthority,
                   'summary' : summary,
                   'publisherInstitution' : publisherInstitution,
                   'publisherName' : publisherName,
                   'publisherEmail' : publisherEmail,
                   'publisherWebsite' : publisherWebsite,
                   'creatorInstitution' : creatorInstitution,
                   'createName' : createName,
                   'openDAP' : openDAP,
                   'wcs' : wcs,
                   'wms' : wms,
                   'ncml' : ncml,
                   'uddc' : uddc,
                   'iso' : iso,
                   'viewer' : viewer}
        (content, headers, payload) = yield self.rpc_send('updateDataResource', payload)
        log.info('Service reply: ' + str(content))
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def deleteDataResource(self, userId, dataResourceId):
        yield self._check_init()
        payload = {'userId' : userId,
                   'dataResourceId' : dataResourceId}
        (content, headers, payload) = yield self.rpc_send('deleteDataResource', payload)
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
    def findUserSubscriptions(self, userId):
        yield self._check_init()
        payload = {'userId' : userId}
        (content, headers, payload) = yield self.rpc_send('findUserSubscriptions', payload)
        log.info('Service reply: ' + str(content))
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def createSubscription(self, userId, dataResourceId, subscriptionName, deliveryMode, deliveryFrequency,
                           deliveryChannel, deliveryChannelId):
        print 'In createDataResourceClient'
        yield self._check_init()
        payload = {'userId' : userId,
                   'dataResourceId' : dataResourceId,
                   'subscriptionName' : subscriptionName,
                   'deliveryMode' : deliveryMode,
                   'deliveryFrequency' : deliveryFrequency,
                   'deliveryChannel' : deliveryChannel,
                   'deliveryChannelId' : deliveryChannelId}
        (content, headers, payload) = yield self.rpc_send('createSubscription', payload)
        log.info('Service reply: ' + str(content))
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def getUserNotifications(self, userId):
        yield self._check_init()
        payload = {'userId' : userId}
        (content, headers, payload) = yield self.rpc_send('getUserNotifications', payload)
        log.info('Service reply: ' + str(content))
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def updateNotification(self, userId, notificationId, status):
        yield self._check_init()
        payload = {'userId' : userId,
                   'notificationId' : notificationId,
                   'status' : status}
        (content, headers, payload) = yield self.rpc_send('updateNotification', payload)
        log.info('Service reply: ' + str(content))
        defer.returnValue(content)
        
    @defer.inlineCallbacks
    def deleteSubscription(self, userId, subscriptionId):
        yield self._check_init()
        payload = {'userId' : userId,
                   'subscriptionId' : subscriptionId}
        (content, headers, payload) = yield self.rpc_send('deleteSubscription', payload)
        log.info('Service reply: ' + str(content))
        defer.returnValue(content)

# Spawn of the process using the module name
factory = ProcessFactory(AppIntegrationService)


# r1 = R1IntegrationService()
# retvalue = r1.loadSampleObjects('DataResource', -1)
# print retvalue

