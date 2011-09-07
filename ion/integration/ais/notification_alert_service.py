__author__ = 'mauricemanning'
#!/usr/bin/env python

"""
@file ion/integration/notification_alert_service.py
@author Maurice Manning
@author Matt Rodriguez
@brief Utility service to send email alerts for user subscriptions
"""


from ion.core import ioninit
CONF = ioninit.config(__name__)

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.object import object_utils
import ion.util.procutils as pu


import string
import time
from datetime import datetime

from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.messaging.message_client import MessageClient
from ion.services.coi.attributestore import AttributeStoreClient
from ion.services.coi.identity_registry import IdentityRegistryClient
from ion.core.process.process import ProcessFactory
from ion.core.data import cassandra_bootstrap

from twisted.mail.smtp import SMTPSenderFactory, SMTPClientError
from email.mime.text import MIMEText
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
from twisted.internet import reactor

from ion.core.exception import ReceivedApplicationError, ApplicationError
from ion.core.data.store import Query
from ion.services.dm.distribution.events import DatasetSupplementAddedEventSubscriber, DatasourceUnavailableEventSubscriber

from ion.core.data.storage_configuration_utility import STORAGE_PROVIDER, PERSISTENT_ARCHIVE, get_cassandra_configuration

from ion.integration.ais.ais_object_identifiers import AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE, \
                                                       SUBSCRIPTION_INFO_TYPE, \
                                                       GET_SUBSCRIPTION_LIST_RESP_TYPE

RESOURCE_CFG_REQUEST_TYPE = object_utils.create_type_identifier(object_id=10, version=1)
USER_OOIID_TYPE = object_utils.create_type_identifier(object_id=1403, version=1)

ION_DATA_ALERTS_EMAIL_ADDRESS = 'data_alerts@oceanobservatories.org'


class NotificationAlertError(ApplicationError):
    """
    A class for Notification exceptions 
    """

class NotificationAlertService(ServiceProcess):
    """
    Service to provide clients access to backend data
    """
    # Declaration of service
    declare = ServiceProcess.service_declare(name='notification_alert',
                                             version='0.1.0',
                                             dependencies=[])

    def __init__(self, *args, **kwargs):
        log.debug('NotificationAlertService.__init__()')
        ServiceProcess.__init__(self, *args, **kwargs)

        #self.rc =    ResourceClient(proc = self)
        self.mc =    MessageClient(proc = self)
        self.irc =   IdentityRegistryClient(proc = self)
        self.store = AttributeStoreClient(proc = self)
        #self.aisc =  AppIntegrationServiceClient(proc = self)


        #if this is a re-start, must create listeners for each data source
 
        index_store_class_name = self.spawn_args.get('index_store_class', CONF.getValue('index_store_class', default='ion.core.data.store.IndexStore'))
        
        self.MailServer = CONF.getValue('mail_server', default='mail.oceanobservatories.org')
        self.update_event_queue_name = CONF.getValue('update_event_queue_name', default='nas_update_event')
        self.offline_event_queue_name = CONF.getValue('offline_event_queue_name', default='nas_offline_event')
        
        self.index_store_class = pu.get_class(index_store_class_name)
        self._storage_conf = get_cassandra_configuration()
        self.storage_provider = self._storage_conf[STORAGE_PROVIDER]
        
        self.username = self.spawn_args.get("cassandra_username", CONF.getValue("cassandra_username", default=None))
        self.password = self.spawn_args.get("cassandra_password", CONF.getValue("cassandra_password", default=None))
        self.column_family  = self.spawn_args.get("column_family", CONF.getValue("column_family", default=None))
        


    @defer.inlineCallbacks
    def slc_init(self):

        #initialize index store for subscription information
        SUBSCRIPTION_INDEXED_COLUMNS = ['user_ooi_id', 'data_src_id', 'subscription_type', 'email_alerts_filter', 'dispatcher_alerts_filter', 'dispatcher_script_path', \
                                        'date_registered', 'title', 'institution', 'source', 'references', 'conventions', 'summary', 'comment', \
                                        'ion_time_coverage_start', 'ion_time_coverage_end', 'ion_geospatial_lat_min', 'ion_geospatial_lat_max', \
                                        'ion_geospatial_lon_min', 'ion_geospatial_lon_max', \
                                        'ion_geospatial_vertical_min', 'ion_geospatial_vertical_max', 'ion_geospatial_vertical_positive', 'download_url']
        
        if issubclass(self.index_store_class , cassandra_bootstrap.CassandraIndexedStoreBootstrap):
            log.info("Instantiating CassandraStore")
            keyspace = self._storage_conf[PERSISTENT_ARCHIVE]['name']
            self.index_store = self.index_store_class(self.username, self.password, self.storage_provider, keyspace, self.column_family)
            yield self.index_store.initialize()
            yield self.index_store.activate()
            
            yield self.register_life_cycle_object(self.index_store)
            log.info("Done with instantiating the Cassandra store")
        else: 
            log.info("Instantiating Memory Store")
            self.index_store = self.index_store_class(self, indices=SUBSCRIPTION_INDEXED_COLUMNS )

        # Create the subscribers for the event handlers

        self.sub = DatasetSupplementAddedEventSubscriber(process=self, queue_name=self.update_event_queue_name)
        self.sub.ondata = self.handle_update_event                     # need to do something with the data when it is received
        yield self.sub.initialize()
        yield self.sub.activate()
        log.info('NotificationAlertService.slc_init DatasetSupplementAddedEventSubscriber activation complete')


        self.sub = DatasourceUnavailableEventSubscriber(process=self, queue_name=self.offline_event_queue_name)
        self.sub.ondata = self.handle_offline_event                    # need to do something with the data when it is received
        yield self.sub.initialize()
        yield self.sub.activate()
        log.info('NotificationAlertService.slc_init DatasourceUnavailableEventSubscriber activation complete')     

        
    @defer.inlineCallbacks
    def handle_offline_event(self, content):
        log.info('NotificationAlertService.handle_offline_event notification event received ')
        log.debug('NotificationAlertService.handle_offline_event content   : %s', content)
        msg = content['content'];
        log.debug('NotificationAlertService.handle_offline_event msg.additional_data.dataset_id   : %s', msg.additional_data.dataset_id)
        log.debug('NotificationAlertService.handle_offline_event msg.additional_data.datasource_id   : %s', msg.additional_data.datasource_id)

        # build the email from the event content
        BODY = string.join(("This data resource is currently unavailable.",
                            "",
                            "Explanation: %s" %  msg.additional_data.error_explanation,
                            "",
                            "You received this notification from ION because you asked to be notified about changes to this data resource. ",
                            "To modify or remove notifications about this data resource, please access My Notifications Settings in the ION Web UI."  ), "\r\n")

        # get the list of subscriptions for this datasource
        query = Query()
        query.add_predicate_eq('data_src_id', msg.additional_data.datasource_id)
        rows = yield self.index_store.query(query)
        log.info("NotificationAlertService.handle_offline_event  Rows returned %s " % (rows,))

        subscriptionInfo = yield self.mc.create_instance(SUBSCRIPTION_INFO_TYPE)

        # send notification email to each user that is monitoring this dataset
        for key, row in rows.iteritems():
            log.info("NotificationAlertService.handle_offline_event  First row data set id %s", rows[key]['data_src_id'] )
            if rows[key]['dispatcher_script_path'] == "AutomaticallyCreatedInitialIngestionSubscription":
                InitialIngestion = True
                SUBJECT = "(SysName " + self.sys_name + ") ION Initial Ingestion Data Alert for data set " +  msg.additional_data.dataset_id
            else:
                InitialIngestion = False
                SUBJECT = "(SysName " + self.sys_name + ") ION Data Alert for data set " +  msg.additional_data.dataset_id
            log.info('NotificationAlertService.handle_update_event: ' + SUBJECT)

            tempTbl = {}
            # get the user information from the Identity Registry
            yield self.GetUserInformation(rows[key]['user_ooi_id'], tempTbl)
            log.info('NotificationAlertService.handle_offline_event user email: %s', tempTbl['user_email'] )

            subscription_type = int(row['subscription_type'])
            email_alerts_filter = int (row['email_alerts_filter'])
            if (subscription_type in (subscriptionInfo.SubscriptionType.EMAIL, subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER) 
                and (email_alerts_filter in (subscriptionInfo.AlertsFilter.DATASOURCEOFFLINE,subscriptionInfo.AlertsFilter.UPDATESANDDATASOURCEOFFLINE))) :
                # Send the message via our own SMTP server, but don't include the envelope header.
                # Create the container (outer) email message.
                log.info('NotificationAlertService.handle_offline_event CREATE EMAIL')
                FROM = ION_DATA_ALERTS_EMAIL_ADDRESS
                TO = tempTbl['user_email']

                msg = MIMEText(BODY)
                msg['Subject'] = SUBJECT
                msg['From'] = FROM
                msg['To'] = ', '.join([TO])
                    
                try:
                    log.debug("NotificationAlertService.handle_offline_event sending email to %s using the mail server at %s" %(TO, self.MailServer))
                    yield self._sendmail(self.MailServer, FROM, [TO], msg)
                    log.info('NotificationAlertService.handle_offline_event Successfully sent email' )
                except SMTPClientError:
                    log.info('NotificationAlertService.handle_offline_event Error: unable to send email')
                except Exception, ex:
                    log.warning('NotificationAlertService.handle_offline_event Error: unable to send email - %s' %str(ex))

                ## Do not delete the initial notification for an unavailable!
                # delete subscription if it was automatically created by the AIS for an initial ingestion at
                # dataset creation
                #if InitialIngestion == True:
                #    yield self.index_store.remove(rows[key]['data_src_id'] + rows[key]['user_ooi_id'])
                #    log.info('NotificationAlertService.handle_offline_event deleted InitialIngestionSubscription for ' + rows[key]['data_src_id'])

                log.info('NotificationAlertService.handle_offline_event completed ')

    
    @defer.inlineCallbacks
    def handle_update_event(self, content):
        log.info('NotificationAlertService.handle_update_event notification event received')
        log.debug('NotificationAlertService.handle_update_event content   : %s', content)
        msg = content['content']
        log.debug('NotificationAlertService.handle_update_event msg.additional_data.dataset_id   : %s', msg.additional_data.dataset_id)
        log.debug('NotificationAlertService.handle_update_event msg.additional_data.datasource_id   : %s', msg.additional_data.datasource_id)
        
        # build the email from the event content
        startdt = str( datetime.fromtimestamp(time.mktime(time.gmtime(msg.additional_data.start_datetime_millis/1000))))
        enddt =  str( datetime.fromtimestamp(time.mktime(time.gmtime(msg.additional_data.end_datetime_millis/1000))) )
        steps =  str(msg.additional_data.number_of_timesteps)
        log.info('NotificationAlertService.handle_update_event START and END time: %s    %s ', startdt, enddt)

        BODY = string.join((
                        "Additional data have been received.",
                        "",
                        "Data Source Title: %s" %  msg.additional_data.title,
                        "Data Source URL: %s" %  msg.additional_data.url,
                        "Start time: %s" % startdt,
                        "End time: %s" % enddt,
                        "Number of time steps: %s" % steps,
                        "",
                        "You received this notification from ION because you asked to be notified about changes to this data resource. ",
                        "To modify or remove notifications about this data resource, please access My Notifications Settings in the ION Web UI."  ), "\r\n")

        # get the list of subscriptions for this datasource
        query = Query()
        query.add_predicate_eq('data_src_id', msg.additional_data.datasource_id)
        rows = yield self.index_store.query(query)
        log.info("NotificationAlertService.handle_update_event  Rows returned %s " % (rows,))

        subscriptionInfo = yield self.mc.create_instance(SUBSCRIPTION_INFO_TYPE)
        
        # send notification email to each user that is monitoring this dataset
        for key, row in rows.iteritems():
            log.info("NotificationAlertService.handle_update_event  First row data set id %s", rows[key]['data_src_id'] )
            if rows[key]['dispatcher_script_path'] == "AutomaticallyCreatedInitialIngestionSubscription":
                InitialIngestion = True
                SUBJECT = "(SysName " + self.sys_name + ") ION Initial Ingestion Data Alert for data set " +  msg.additional_data.dataset_id
            else:
                InitialIngestion = False
                SUBJECT = "(SysName " + self.sys_name + ") ION Data Alert for data set " +  msg.additional_data.dataset_id
            log.info('NotificationAlertService.handle_update_event: ' + SUBJECT)

            tempTbl = {}
            # get the user information from the Identity Registry
            yield self.GetUserInformation(rows[key]['user_ooi_id'], tempTbl)
            log.info('NotificationAlertService.handle_update_event user email: %s', tempTbl['user_email'] )
            subscription_type = int(row['subscription_type'])
            email_alerts_filter = int (row['email_alerts_filter'])
            if (subscription_type in (subscriptionInfo.SubscriptionType.EMAIL, subscriptionInfo.SubscriptionType.EMAILANDDISPATCHER) 
                and (email_alerts_filter in (subscriptionInfo.AlertsFilter.UPDATES, subscriptionInfo.AlertsFilter.UPDATESANDDATASOURCEOFFLINE))) :
                # Send the message via our own SMTP server, but don't include the envelope header.
                # Create the container (outer) email message.
                log.info('NotificationAlertService.handle_update_event CREATE EMAIL')
                FROM = ION_DATA_ALERTS_EMAIL_ADDRESS
                TO = tempTbl['user_email']

                msg = MIMEText(BODY)
                msg['Subject'] = SUBJECT
                msg['From'] = FROM
                msg['To'] = ', '.join([TO])

                try:
                    log.debug("NotificationAlertService.handle_update_event: sending email to %s using the mail server at %s" %(TO, self.MailServer))
                    yield self._sendmail(self.MailServer, FROM, [TO], msg)
                    log.info('NotificationAlertService.handle_update_event Successfully sent email' )
                except SMTPClientError:
                    log.info('NotificationAlertService.handle_update_event Error: unable to send email')
                except Exception, ex:
                    log.warning('NotificationAlertService.handle_update_event Error: unable to send email - %s'% str(ex))

                # delete subscription if it was automatically created by the AIS for an initial ingestion at
                # dataset creation
                if InitialIngestion == True:
                    yield self.index_store.remove(rows[key]['data_src_id'] + rows[key]['user_ooi_id'])
                    log.info('NotificationAlertService.handle_update_event deleted InitialIngestionSubscription for ' + rows[key]['data_src_id'])

                log.info('NotificationAlertService.handle_update_event completed ')


    @defer.inlineCallbacks
    def op_addSubscription(self, content, headers, msg):
        """
        @brief Add a new subscription to the set of queue to monitor
        @param userId, queueId
        @retval none
        """
        log.info('NotificationAlertService.op_addSubscription()\n ')
        yield self.CheckRequest(content)

        # Check only the type received
        if content.MessageType != AIS_REQUEST_MSG_TYPE:
            raise NotificationAlertError('Bad message type receieved, ignoring',
                                            content.ResponseCodes.BAD_REQUEST)

        # check that subscriptionInfo is present in GPB
        if not content.message_parameters_reference.IsFieldSet('subscriptionInfo'):
            raise NotificationAlertError('Incomplete message format receieved, ignoring',
                                            content.ResponseCodes.BAD_REQUEST)

        # check that AisDatasetMetadataType is present in GPB
        if not content.message_parameters_reference.IsFieldSet('datasetMetadata'):
            raise NotificationAlertError('Incomplete message format receieved, ignoring',
                                            content.ResponseCodes.BAD_REQUEST)

        if not content.message_parameters_reference.subscriptionInfo.IsFieldSet('user_ooi_id'):
            raise NotificationAlertError('Incomplete message format receieved, ignoring',
                                            content.ResponseCodes.BAD_REQUEST)

        if not content.message_parameters_reference.subscriptionInfo.IsFieldSet('data_src_id'):
            raise NotificationAlertError('Incomplete message format receieved, ignoring',
                                            content.ResponseCodes.BAD_REQUEST)

        # check that subscription type enum is present in GPB
        if not content.message_parameters_reference.subscriptionInfo.IsFieldSet('subscription_type'):
            raise NotificationAlertError('Incomplete message format receieved, ignoring',
                                            content.ResponseCodes.BAD_REQUEST)

        if not content.message_parameters_reference.subscriptionInfo.IsFieldSet('date_registered'):
            raise NotificationAlertError('date_registered (provided by AIS) missing, ignoring',
                                            content.ResponseCodes.BAD_REQUEST)

        #Check that user ids in both GPBs match - decided not to do this check as one id is for the user requesting the subscription and the other is for the user who owns the data source
        #if not (content.message_parameters_reference.subscriptionInfo.user_ooi_id == content.message_parameters_reference.datasetMetadata.user_ooi_id ):
        #   raise NotificationAlertError('Inconsistent data in create subscription information, ignoring',
        #                                    content.ResponseCodes.BAD_REQUEST)
        #Check that data source ids in both GPBs match
        log.info('NotificationAlertService.op_addSubscription subscriptionInfo.data_src_id %s', content.message_parameters_reference.subscriptionInfo.data_src_id )
        log.info('NotificationAlertService.op_addSubscription datasetMetadata.data_resource_id %s', content.message_parameters_reference.datasetMetadata.data_resource_id )
        if not (content.message_parameters_reference.subscriptionInfo.data_src_id == content.message_parameters_reference.datasetMetadata.data_resource_id ):
            raise NotificationAlertError('Inconsistent data in create subscription information, ignoring',
                                            content.ResponseCodes.BAD_REQUEST)


        log.info("NotificationAlertService.op_addSubscription subscription_type' %s", content.message_parameters_reference.subscriptionInfo.subscription_type)
        #add the subscription to the index store
        log.info('NotificationAlertService.op_addSubscription add attributes\n ')
    
        self.attributes = {'user_ooi_id':content.message_parameters_reference.subscriptionInfo.user_ooi_id,
                   'data_src_id': content.message_parameters_reference.subscriptionInfo.data_src_id,
                   'subscription_type': str(content.message_parameters_reference.subscriptionInfo.subscription_type),
                   'email_alerts_filter': str(content.message_parameters_reference.subscriptionInfo.email_alerts_filter),
                   'dispatcher_alerts_filter':str(content.message_parameters_reference.subscriptionInfo.dispatcher_alerts_filter),
                   'dispatcher_script_path': content.message_parameters_reference.subscriptionInfo.dispatcher_script_path,
                   'date_registered': str(content.message_parameters_reference.subscriptionInfo.date_registered),

                   'title' : content.message_parameters_reference.datasetMetadata.title,
                   'institution' : content.message_parameters_reference.datasetMetadata.institution,
                   'source' : content.message_parameters_reference.datasetMetadata.source,
                   'references' : content.message_parameters_reference.datasetMetadata.references,
                   'conventions' : content.message_parameters_reference.datasetMetadata.conventions,
                   'summary' : content.message_parameters_reference.datasetMetadata.summary,
                   'comment' : content.message_parameters_reference.datasetMetadata.comment,
                   'ion_time_coverage_start' : content.message_parameters_reference.datasetMetadata.ion_time_coverage_start,
                   'ion_time_coverage_end' : content.message_parameters_reference.datasetMetadata.ion_time_coverage_end,
                   'ion_geospatial_lat_min' : str(content.message_parameters_reference.datasetMetadata.ion_geospatial_lat_min),
                   'ion_geospatial_lat_max' : str(content.message_parameters_reference.datasetMetadata.ion_geospatial_lat_max),
                   'ion_geospatial_lon_min' : str(content.message_parameters_reference.datasetMetadata.ion_geospatial_lon_min),
                   'ion_geospatial_lon_max' : str(content.message_parameters_reference.datasetMetadata.ion_geospatial_lon_max),
                   'ion_geospatial_vertical_min' : str(content.message_parameters_reference.datasetMetadata.ion_geospatial_vertical_min),
                   'ion_geospatial_vertical_max' : str(content.message_parameters_reference.datasetMetadata.ion_geospatial_vertical_max),
                   'ion_geospatial_vertical_positive' : content.message_parameters_reference.datasetMetadata.ion_geospatial_vertical_positive,
                   'download_url' : content.message_parameters_reference.datasetMetadata.download_url,
                   
        }
        
        log.info('NotificationAlertService.op_addSubscription attributes userid: %s', content.message_parameters_reference.subscriptionInfo.user_ooi_id )
        log.info('NotificationAlertService.op_addSubscription attributes datasrc id: %s', content.message_parameters_reference.subscriptionInfo.data_src_id )
        self.keyval = content.message_parameters_reference.subscriptionInfo.data_src_id + content.message_parameters_reference.subscriptionInfo.user_ooi_id
        log.info('NotificationAlertService.op_addSubscription attributes keyval id: %s', self.keyval )
        yield self.index_store.put(self.keyval , self.keyval, self.attributes)

        # create the AIS response GPBs
        log.info('NotificationAlertService.op_addSubscription construct response message')
        respMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        respMsg.result = respMsg.ResponseCodes.OK;

        log.info('NotificationAlertService.op_addSubscription complete')
        yield self.reply_ok(msg, respMsg)


    @defer.inlineCallbacks
    def op_removeSubscription(self, content, headers, msg):
        """
        @brief remove a subscription from the set of queues to monitor
        @param
        @retval none
        """
        log.debug('NotificationAlertService.op_removeSubscription: \n'+str(content))

        # Check only the type received
        if content.MessageType != AIS_REQUEST_MSG_TYPE:
            raise NotificationAlertError('Expected message class AIS_REQUEST_MSG_TYPE, received %s'% str(content))

        # check that subscriptionInfo is present in GPB
        if not content.message_parameters_reference.IsFieldSet('subscriptionInfo'):
            raise NotificationAlertError('Incomplete message format receieved, ignoring',
                                            content.ResponseCodes.BAD_REQUEST)

        if not content.message_parameters_reference.subscriptionInfo.IsFieldSet('user_ooi_id'):
            raise NotificationAlertError('Incomplete message format receieved, ignoring',
                                            content.ResponseCodes.BAD_REQUEST)

        if not content.message_parameters_reference.subscriptionInfo.IsFieldSet('data_src_id'):
            raise NotificationAlertError('Incomplete message format receieved, ignoring',
                                            content.ResponseCodes.BAD_REQUEST)

        log.info('NotificationAlertService.op_removeSubscription  Removing subscription %s from store...', content.message_parameters_reference.subscriptionInfo.data_src_id)

        self.keyval = content.message_parameters_reference.subscriptionInfo.data_src_id + content.message_parameters_reference.subscriptionInfo.user_ooi_id
        log.info("NotificationAlertService.op_removeSubscription key: %s ", self.keyval)

        if not ( yield self.index_store.has_key(self.keyval) ):
            raise NotificationAlertError('Invalid request, subscription does not exist, ignoring',
                                            content.ResponseCodes.BAD_REQUEST)
        yield self.index_store.remove(self.keyval)

        # create the AIS response GPB
        respMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        respMsg.result = respMsg.ResponseCodes.OK

        log.info('NotificationAlertService..op_removeSubscription complete')
        yield self.reply_ok(msg, respMsg)


    @defer.inlineCallbacks
    def op_getSubscription(self, content, headers, msg):
        """
        @brief return a subscription from the list
        @param
        @retval a list with 1 item
        """
        log.debug('NotificationAlertService.op_getSubscription: \n'+str(content))

        # Check only the type received
        if content.MessageType != AIS_REQUEST_MSG_TYPE:
            raise NotificationAlertError('Expected message class AIS_REQUEST_MSG_TYPE, received %s'% str(content))

        # check that subscriptionInfo is present in GPB
        if not content.message_parameters_reference.IsFieldSet('subscriptionInfo'):
            raise NotificationAlertError('Incomplete message format receieved, ignoring',
                                            content.ResponseCodes.BAD_REQUEST)

        if not content.message_parameters_reference.subscriptionInfo.IsFieldSet('user_ooi_id'):
            raise NotificationAlertError('Incomplete message format receieved, ignoring',
                                            content.ResponseCodes.BAD_REQUEST)

        if not content.message_parameters_reference.subscriptionInfo.IsFieldSet('data_src_id'):
            raise NotificationAlertError('Incomplete message format receieved, ignoring',
                                            content.ResponseCodes.BAD_REQUEST)

        log.info('NotificationAlertService.op_getSubscription  Returning subscription %s from store...', content.message_parameters_reference.subscriptionInfo.data_src_id)

        self.keyval = content.message_parameters_reference.subscriptionInfo.data_src_id + content.message_parameters_reference.subscriptionInfo.user_ooi_id
        log.info("NotificationAlertService.op_getSubscription key: %s ", self.keyval)

        if not ( yield self.index_store.has_key(self.keyval) ):
            raise NotificationAlertError('Invalid request, subscription does not exist',
                                             content.ResponseCodes.BAD_REQUEST)
        query = Query()
        query.add_predicate_eq('user_ooi_id', content.message_parameters_reference.subscriptionInfo.user_ooi_id)
        query.add_predicate_eq('data_src_id', content.message_parameters_reference.subscriptionInfo.data_src_id)
        rows = yield self.index_store.query(query)
        log.info("NotificationAlertService.op_getSubscription rows: %s ", str(rows))

        # create the AIS response GPB
        respMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        respMsg.message_parameters_reference.add()
        respMsg.message_parameters_reference[0] = respMsg.CreateObject(GET_SUBSCRIPTION_LIST_RESP_TYPE)
        for key, row in rows.iteritems ( ) :
            respMsg.message_parameters_reference[0].subscriptionListResults.add()
            respMsg.message_parameters_reference[0].subscriptionListResults[0].subscriptionInfo.user_ooi_id = rows[key]['user_ooi_id']
            respMsg.message_parameters_reference[0].subscriptionListResults[0].subscriptionInfo.data_src_id = rows[key]['data_src_id']
            respMsg.message_parameters_reference[0].subscriptionListResults[0].subscriptionInfo.subscription_type = int(rows[key]['subscription_type'])
            respMsg.message_parameters_reference[0].subscriptionListResults[0].subscriptionInfo.email_alerts_filter = int(rows[key]['email_alerts_filter'])
            respMsg.message_parameters_reference[0].subscriptionListResults[0].subscriptionInfo.dispatcher_alerts_filter = int(rows[key]['dispatcher_alerts_filter'])
            respMsg.message_parameters_reference[0].subscriptionListResults[0].subscriptionInfo.dispatcher_script_path = rows[key]['dispatcher_script_path']
            respMsg.message_parameters_reference[0].subscriptionListResults[0].subscriptionInfo.date_registered = long(rows[key]['date_registered'])
    
            respMsg.message_parameters_reference[0].subscriptionListResults[0].datasetMetadata.user_ooi_id = rows[key]['user_ooi_id']
            respMsg.message_parameters_reference[0].subscriptionListResults[0].datasetMetadata.data_resource_id = rows[key]['data_src_id']
            respMsg.message_parameters_reference[0].subscriptionListResults[0].datasetMetadata.title = rows[key]['title']
            respMsg.message_parameters_reference[0].subscriptionListResults[0].datasetMetadata.institution = rows[key]['institution']
            respMsg.message_parameters_reference[0].subscriptionListResults[0].datasetMetadata.source = rows[key]['source']
            respMsg.message_parameters_reference[0].subscriptionListResults[0].datasetMetadata.references = rows[key]['references']
            respMsg.message_parameters_reference[0].subscriptionListResults[0].datasetMetadata.summary = rows[key]['summary']
            respMsg.message_parameters_reference[0].subscriptionListResults[0].datasetMetadata.conventions = rows[key]['conventions']
            respMsg.message_parameters_reference[0].subscriptionListResults[0].datasetMetadata.comment = rows[key]['comment']
            respMsg.message_parameters_reference[0].subscriptionListResults[0].datasetMetadata.ion_time_coverage_start = rows[key]['ion_time_coverage_start']
            respMsg.message_parameters_reference[0].subscriptionListResults[0].datasetMetadata.ion_time_coverage_end = rows[key]['ion_time_coverage_end']
            respMsg.message_parameters_reference[0].subscriptionListResults[0].datasetMetadata.ion_geospatial_lat_min = float(rows[key]['ion_geospatial_lat_min'])
            respMsg.message_parameters_reference[0].subscriptionListResults[0].datasetMetadata.ion_geospatial_lat_max = float(rows[key]['ion_geospatial_lat_max'])
            respMsg.message_parameters_reference[0].subscriptionListResults[0].datasetMetadata.ion_geospatial_lon_min = float(rows[key]['ion_geospatial_lon_min'])
            respMsg.message_parameters_reference[0].subscriptionListResults[0].datasetMetadata.ion_geospatial_lon_max = float(rows[key]['ion_geospatial_lon_max'])
            respMsg.message_parameters_reference[0].subscriptionListResults[0].datasetMetadata.ion_geospatial_vertical_min = float(rows[key]['ion_geospatial_vertical_min'])
            respMsg.message_parameters_reference[0].subscriptionListResults[0].datasetMetadata.ion_geospatial_vertical_max = float(rows[key]['ion_geospatial_vertical_max'])
            respMsg.message_parameters_reference[0].subscriptionListResults[0].datasetMetadata.ion_geospatial_vertical_positive = rows[key]['ion_geospatial_vertical_positive']
            respMsg.message_parameters_reference[0].subscriptionListResults[0].datasetMetadata.download_url = rows[key]['download_url']
        respMsg.result = respMsg.ResponseCodes.OK

        log.info('NotificationAlertService..op_getSubscription complete')
        yield self.reply_ok(msg, respMsg)


    @defer.inlineCallbacks
    def op_getSubscriptionList(self, content, headers, msg):
        """
        @brief remove a subscription from the set of queues to monitor
        @param
        @retval none
        """
        log.debug('NotificationAlertService.op_getSubscriptionList \n'+str(content))

        # Check only the type received
        if content.MessageType != AIS_REQUEST_MSG_TYPE:
            raise NotificationAlertError('Expected message class AIS_REQUEST_MSG_TYPE, received %s'% str(content))

        # check that ooi_id is present in GPB
        if not content.message_parameters_reference.IsFieldSet('user_ooi_id'):
            raise NotificationAlertError('Incomplete message format receieved, ignoring',
                                            content.ResponseCodes.BAD_REQUEST)            

        #Check that the item is in the store
        query = Query()
        query.add_predicate_eq('user_ooi_id', content.message_parameters_reference.user_ooi_id)
        rows = yield self.index_store.query(query)
        log.info("NotificationAlertService.op_getSubscriptionList  Rows returned %s " % (rows,))

        # create the register_user request GPBs
        respMsg = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        respMsg.message_parameters_reference.add()
        respMsg.message_parameters_reference[0] = respMsg.CreateObject(GET_SUBSCRIPTION_LIST_RESP_TYPE)

        #add each result row into the response message
        i = 0
        for key, row in rows.iteritems ( ) :
            log.info("NotificationAlertService.op_getSubscriptionList  First row data set id %s", rows[key]['data_src_id'] )
            respMsg.message_parameters_reference[0].subscriptionListResults.add()
            respMsg.message_parameters_reference[0].subscriptionListResults[i].subscriptionInfo.user_ooi_id = rows[key]['user_ooi_id']
            respMsg.message_parameters_reference[0].subscriptionListResults[i].subscriptionInfo.data_src_id = rows[key]['data_src_id']
            respMsg.message_parameters_reference[0].subscriptionListResults[i].subscriptionInfo.subscription_type = int(rows[key]['subscription_type'])
            respMsg.message_parameters_reference[0].subscriptionListResults[i].subscriptionInfo.email_alerts_filter = int(rows[key]['email_alerts_filter'])
            respMsg.message_parameters_reference[0].subscriptionListResults[i].subscriptionInfo.dispatcher_alerts_filter = int(rows[key]['dispatcher_alerts_filter'])
            respMsg.message_parameters_reference[0].subscriptionListResults[i].subscriptionInfo.dispatcher_script_path = rows[key]['dispatcher_script_path']
            respMsg.message_parameters_reference[0].subscriptionListResults[i].subscriptionInfo.date_registered = int(rows[key]['date_registered'])

            respMsg.message_parameters_reference[0].subscriptionListResults[i].datasetMetadata.user_ooi_id = rows[key]['user_ooi_id']
            respMsg.message_parameters_reference[0].subscriptionListResults[i].datasetMetadata.data_resource_id = rows[key]['data_src_id']
            respMsg.message_parameters_reference[0].subscriptionListResults[i].datasetMetadata.title = rows[key]['title']
            respMsg.message_parameters_reference[0].subscriptionListResults[i].datasetMetadata.institution = rows[key]['institution']
            respMsg.message_parameters_reference[0].subscriptionListResults[i].datasetMetadata.source = rows[key]['source']
            respMsg.message_parameters_reference[0].subscriptionListResults[i].datasetMetadata.references = rows[key]['references']
            respMsg.message_parameters_reference[0].subscriptionListResults[i].datasetMetadata.summary = rows[key]['summary']
            respMsg.message_parameters_reference[0].subscriptionListResults[i].datasetMetadata.conventions = rows[key]['conventions']
            respMsg.message_parameters_reference[0].subscriptionListResults[i].datasetMetadata.comment = rows[key]['comment']
            respMsg.message_parameters_reference[0].subscriptionListResults[i].datasetMetadata.ion_time_coverage_start = rows[key]['ion_time_coverage_start']
            respMsg.message_parameters_reference[0].subscriptionListResults[i].datasetMetadata.ion_time_coverage_end = rows[key]['ion_time_coverage_end']
            respMsg.message_parameters_reference[0].subscriptionListResults[i].datasetMetadata.ion_geospatial_lat_min = float(rows[key]['ion_geospatial_lat_min'])
            respMsg.message_parameters_reference[0].subscriptionListResults[i].datasetMetadata.ion_geospatial_lat_max = float(rows[key]['ion_geospatial_lat_max'])
            respMsg.message_parameters_reference[0].subscriptionListResults[i].datasetMetadata.ion_geospatial_lon_min = float(rows[key]['ion_geospatial_lon_min'])
            respMsg.message_parameters_reference[0].subscriptionListResults[i].datasetMetadata.ion_geospatial_lon_max = float(rows[key]['ion_geospatial_lon_max'])
            respMsg.message_parameters_reference[0].subscriptionListResults[i].datasetMetadata.ion_geospatial_vertical_min = float(rows[key]['ion_geospatial_vertical_min'])
            respMsg.message_parameters_reference[0].subscriptionListResults[i].datasetMetadata.ion_geospatial_vertical_max = float(rows[key]['ion_geospatial_vertical_max'])
            respMsg.message_parameters_reference[0].subscriptionListResults[i].datasetMetadata.ion_geospatial_vertical_positive = rows[key]['ion_geospatial_vertical_positive']
            respMsg.message_parameters_reference[0].subscriptionListResults[i].datasetMetadata.download_url = rows[key]['download_url']

            i = i + 1

        respMsg.result = respMsg.ResponseCodes.OK

        log.info('NotificationAlertService.op_getSubscriptionList complete')
        yield self.reply_ok(msg, respMsg)


    @defer.inlineCallbacks
    def CheckRequest(self, request):
      # Check for correct request protocol buffer type
      if request.MessageType != AIS_REQUEST_MSG_TYPE:
         # build AIS error response
         Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
         Response.error_num = Response.ResponseCodes.BAD_REQUEST
         Response.error_str = 'Bad message type receieved, ignoring'
         defer.returnValue(Response)

      # Check payload in message
      if not request.IsFieldSet('message_parameters_reference'):
         # build AIS error response
         Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
         Response.error_num = Response.ResponseCodes.BAD_REQUEST
         Response.error_str = "Required field [message_parameters_reference] not found in message"
         defer.returnValue(Response)

      defer.returnValue(None)

    @defer.inlineCallbacks
    def GetUserInformation(self, user_ooi_id, tempTbl):

        log.info('NotificationAlertService.GetUserInformation user:  %s  attributes: %s', user_ooi_id, tempTbl)
        #Build the Identity Registry request for get_user message
        Request = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE)
        Request.configuration = Request.CreateObject(USER_OOIID_TYPE)
        Request.configuration.ooi_id = user_ooi_id

        # get the user information from the Identity Registry
        try:
            user_info = yield self.irc.get_user(Request)
        except ReceivedApplicationError, ex:
             # build AIS error response
             log.info('NotificationAlertService.GetUserInformation Send Error: %s ', ex.msg_content.MessageResponseBody)
             Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
             Response.error_num = ex.msg_content.MessageResponseCode
             Response.error_str = ex.msg_content.MessageResponseBody
             defer.returnValue(Response)

        tempTbl['user_email'] = user_info.resource_reference.email
        log.info('NotificationAlertService.GetUserInformation user email: %s', user_info.resource_reference.email)

        defer.returnValue(None)


    def _sendmail(self, smtphost, from_addr, to_addrs, msg, senderDomainName=None, port=25):
        # this is a clone of the helper method from twisted.mail.smtp
        # it has been modified to shorten the timeout for the TCP connection from 30 seconds to 3 seconds
        # and to setup the SMTPSenderFactory for 0 retries and 3 second timeout
        
        """Send an email
    
        This interface is intended to be a direct replacement for
        smtplib.SMTP.sendmail() (with the obvious change that
        you specify the smtphost as well). Also, ESMTP options
        are not accepted, as we don't do ESMTP yet. I reserve the
        right to implement the ESMTP options differently.
    
        @param smtphost: The host the message should be sent to
        @param from_addr: The (envelope) address sending this mail.
        @param to_addrs: A list of addresses to send this mail to.  A string will
            be treated as a list of one address
        @param msg: The message, including headers, either as a file or a string.
            File-like objects need to support read() and close(). Lines must be
            delimited by '\\n'. If you pass something that doesn't look like a
            file, we try to convert it to a string (so you should be able to
            pass an email.Message directly, but doing the conversion with
            email.Generator manually will give you more control over the
            process).
    
        @param senderDomainName: Name by which to identify.  If None, try
        to pick something sane (but this depends on external configuration
        and may not succeed).
    
        @param port: Remote port to which to connect.
    
        @rtype: L{Deferred}
        @returns: A L{Deferred}, its callback will be called if a message is sent
            to ANY address, the errback if no message is sent.
    
            The callback will be called with a tuple (numOk, addresses) where numOk
            is the number of successful recipient addresses and addresses is a list
            of tuples (address, code, resp) giving the response to the RCPT command
            for each address.
        """
        if not hasattr(msg,'read'):
            # It's not a file
            msg = StringIO(str(msg))
    
        d = defer.Deferred()
        factory = SMTPSenderFactory(from_addr, to_addrs, msg, d, 0, 3)
    
        if senderDomainName is not None:
            factory.domain = senderDomainName
            
        reactor.connectTCP(smtphost, port, factory, 3)
    
        return d


    """
    @defer.inlineCallbacks
    def GetDatasetInformation(self, data_src_id, attributes):
        #
        # Create a request message to get the metadata details about the
        # source (i.e., where the dataset came from) of a particular dataset
        # resource ID.
        #
        reqMsg = yield mc.create_instance(AIS_REQUEST_MSG_TYPE)
        reqMsg.message_parameters_reference = reqMsg.CreateObject(GET_DATA_RESOURCE_DETAIL_REQ_MSG_TYPE)
        reqMsg.message_parameters_reference.data_resource_id = data_src_id

        log.debug('NotificationAlertService.op_addSubscription Calling getDataResourceDetail.')
        rspMsg = yield self.aisc.getDataResourceDetail(reqMsg)
        log.debug('NotificationAlertService.op_addSubscription getDataResourceDetail returned:\n' + \
            str('resource_id: ') + \
            str(rspMsg.message_parameters_reference[0].data_resource_id) + \
            str('\n'))

        dSource = rspMsg.message_parameters_reference[0]
        attributes['data_resource_id'] = dSource.data_resource_id

        defer.returnValue(None)
    """


class NotificationAlertServiceClient(ServiceClient):
    """
    This is a service client for NotificationAlertService.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "notification_alert"
        ServiceClient.__init__(self, proc, **kwargs)


    @defer.inlineCallbacks
    def addSubscription(self, message):
        yield self._check_init()
        log.debug('NAS_client.addSubscription: sendin g following message to addSubscription:\n%s' % str(message))
        (content, headers, payload) = yield self.rpc_send('addSubscription', message)
        log.debug('NAS_client.addSubscription: reply:\n' + str(content))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def removeSubscription(self, message):
        yield self._check_init()
        log.debug('NAS_client.removeSubscription: sending following message to removeSubscription:\n%s' % str(message))
        (content, headers, payload) = yield self.rpc_send('removeSubscription', message)
        log.debug('NAS_client.removeSubscription: reply:\n' + str(content))
        defer.returnValue(content)

    @defer.inlineCallbacks
    def getSubscriptionList(self, message):
        yield self._check_init()
        log.debug('NAS_client.getSubscriptionList: sending following message to getSubscriptionList:\n%s' % str(message))
        (content, headers, payload) = yield self.rpc_send('getSubscriptionList', message)
        log.debug('NAS_client.getSubscriptionList: reply:\n' + str(content))
        defer.returnValue(content)


    @defer.inlineCallbacks
    def getSubscription(self, message):
        yield self._check_init()
        log.debug('NAS_client.getSubscription: sending following message to getSubscription:\n%s' % str(message))
        (content, headers, payload) = yield self.rpc_send('getSubscription', message)
        log.debug('NAS_client.getSubscription: reply:\n' + str(content))
        defer.returnValue(content)


# Spawn of the process using the module name
factory = ProcessFactory(NotificationAlertService)
