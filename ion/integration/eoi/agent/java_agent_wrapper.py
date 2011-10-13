#!/usr/bin/env python

"""
@file:   ion/integration/eoi/agent/java_agent_wrapper.py
@author: Chris Mueller
@author: Tim LaRocque
@brief:  The External Observatory Integration's JavaAgentWrapper is a Python agent implementation which delegates processing to an underlying Agent process.
         While the JavaAgentWrapper is alive and active its underlying Agent delegate should also be active.  In fact, the whole intent of the JavaAgentWrapper
         is to delegate all processing activities to the underlying (Java) Agent while providing governance mechanics.

@see:    Dataset Ingest MSC -                  https://docs.google.com/drawings/edit?id=1lg2mQOX6gD46_Uhr5h96McSbR4QvhEDMPw5fvKAqAmE&hl=en
@see:    Dataset Agent Overview -              https://docs.google.com/drawings/d/1T03WPCFUElW_ExUPccIodXU8eqy-CKxnZxscuWsBeAs/edit?hl=en_US&authkey=CMDVk_kL
@see:    Dataset Agent Diagram -               https://docs.google.com/drawings/d/1UbQOhga4Qq0rq4-fZWJP3qGybEgrSpe4QzRpKTGRhnc/edit?hl=en_US&authkey=COuN9bUM
@see:    Dataset Agent Diagram (Activation) -  https://docs.google.com/drawings/d/1YVySMhsuzl0PWIW5Xk9BSCz6mYzdmuGXoKXYdzXNvc8/edit?hl=en_US&authkey=CMDan_EN
@see:    Dataset Agent Diagram (Termination) - https://docs.google.com/drawings/d/1zh4d4H_91jF9w9jLVHSJF7Mtm_49MN-zdUKt_TMFLo8/edit?hl=en_US&authkey=CLzOzuQB
"""


# Imports: Builtin
import os, time, calendar
try:
    import json
except:
    import simplejson as json


# Imports: Twisted
from twisted.internet import defer, reactor


# Imports: ION Core
from ion.core import ioninit
from ion.core.exception import IonError, ApplicationError, ReceivedError
from ion.core.object import object_utils
from ion.core.object.gpb_wrapper import OOIObjectError
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient


# Imports: ION Clients
from ion.core.messaging.message_client import MessageClient
from ion.services.dm.inventory.association_service import AssociationServiceClient
from ion.services.coi.resource_registry.resource_client import ResourceClient


# Imports: ION Messages and Events
from ion.services.dm.distribution.events import ScheduleEventSubscriber, IngestionProcessingEventSubscriber
from ion.services.dm.scheduler.scheduler_service import SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE


# Imports: Resources and Associations
from ion.services.dm.inventory.association_service import PREDICATE_OBJECT_QUERY_TYPE, IDREF_TYPE, SUBJECT_PREDICATE_QUERY_TYPE
from ion.services.coi.datastore_bootstrap.ion_preload_config import TESTING_SIGNIFIER
from ion.services.coi.datastore_bootstrap.ion_preload_config import HAS_A_ID
from ion.util import timeout

DATA_CONTEXT_TYPE = object_utils.create_type_identifier(object_id=4501, version=1)
CHANGE_EVENT_TYPE = object_utils.create_type_identifier(object_id=7001, version=1)
PERFORM_INGEST_TYPE = object_utils.create_type_identifier(object_id=2002, version=1)
PREDICATE_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=25, version=1)
DATA_SOURCE_TYPE = object_utils.create_type_identifier(object_id=4503, version=1)
DATASET_TYPE = object_utils.create_type_identifier(object_id=10001, version=1)


# Imports: Utils/Config/Logging
import logging
import ion.util.ionlog
from ion.util.os_process import OSProcess, OSProcessError
from ion.util.state_object import BasicStates

log = ion.util.ionlog.getLogger(__name__)
CONF = ioninit.config(__name__)

class AgentOSProcess(OSProcess):
    """
    The sole purpose of this inherited class is to move debugging log statements into this module.
    """

    def outReceived(self, data):
        """
        Output on stdout has been received.
        Stores the output in a list.
        """
        log.info("JAVA-SO: %s" % str(data).strip())
        self.outlines.append(data)

    def errReceived(self, data):
        """
        Output on stderr has been received.
        Stores the output in a list.
        """
        log.error("JAVA-SE: %s" % data)
        self.errlines.append(data)

class JavaAgentWrapperException(ApplicationError):
    """
    An exception class for the Java Agent Wrapper

    Be careful using this - there is no process to notify of the error on ingest!
    """

class UpdateHandler(ScheduleEventSubscriber):
    """
    This class provides the messaging hooks to invoke rsync on receipt
    of scheduler messages.
    """
    def __init__(self, hook_fn, *args, **kwargs):
        self.hook_fn = hook_fn
        ScheduleEventSubscriber.__init__(self, *args, **kwargs)

    @defer.inlineCallbacks
    def ondata(self, data):
        """
        @brief callback used to handle incoming notifications of this subscriber.
               Overridden to unpack dataset and datasource IDs from the update notification
        @param data: A dictionary of data
        """
        log.info('JAW: Got an update event message from the scheduler')

        content = data['content']
        update_msg = content.additional_data.payload

        try:
            res = yield self.hook_fn(update_msg.dataset_id, update_msg.datasource_id)
        except JavaAgentWrapperException, ex:
            log.error("JAW encountered an error during update: %s" % str(ex))
            res = False

        log.info("JAW (scheduled update event): %s" % str(res))


class JavaAgentWrapper(ServiceProcess):
    """
    Class designed to facilitate (Java) Dataset Agent's tight interaction with ION in
    lieu of an incomplete Java CC.  Wrapping Java Agents in Python processes in this way
    provides the following functionality:
        Agent registration, process lifecycle (governance), and reactivity to other core ION services
    """
    
    
    declare = ServiceProcess.service_declare(name='java_agent_wrapper',
                                             version='0.2.0',
                                             dependencies=[]) # no dependencies

        
    def __init__(self, *args, **kwargs):
        '''
        @brief: Initialize the JavaAgentWrapper instance, init instance fields, etc.
        '''
        # Step 1: Delegate initialization to parent "ServiceProcess"
        log.info('')
        log.info('Initializing class instance...')
        ServiceProcess.__init__(self, *args, **kwargs)
        
        # Step 2: Create class attributes
        self.__agent_spawn_args = None
        self.__ingest_client = None
        self.__ingest_ready_deferred = None
        
        self.queue_name = self.spawn_args.get('queue_name', CONF.getValue('queue_name', default='java_agent_wrapper_updates'))

        # Step 1: Perform Initialization
        self.mc = MessageClient(proc=self)
        self.rc = ResourceClient(proc=self)
        self.asc = AssociationServiceClient(proc=self)

    @defer.inlineCallbacks
    def slc_init(self):
        '''
        Tests the presence and execution of an INIT_TEST context in the eoi-agents jar. If this fails, the service will
        fall over.
        '''
        yield self._spawn_dataset_agent('INIT_TEST')

    @defer.inlineCallbacks
    def slc_activate(self):
        '''
        @brief: Service activation during spawning
        
        @see:   Dataset Agent Diagram (Activation) - https://docs.google.com/drawings/d/1YVySMhsuzl0PWIW5Xk9BSCz6mYzdmuGXoKXYdzXNvc8/edit?hl=en_US&authkey=CMDan_EN
        
        @attention: This activation yields nothing and therefore is NOT an inlineCallback
        '''
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug(" -[]- Entered slc_activate(); state=%s" % (str(self._get_state())))

        # Setup schedule event subscriber
        log.debug('Creating new message receiver for scheduled updates')
        self.update_handler = UpdateHandler(self._update_request,
                                queue_name=self.queue_name,
                                origin=SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE,
                                process=self)
        # Add the receiver as a registered life cycle object
        yield self.register_life_cycle_object(self.update_handler)
        
    def _spawn_dataset_agent(self, context_str):
        '''
        @brief: Spawns the Java Dataset Agent
        @return
        @raise RuntimeError: When invalid arguments are provided for spawning the agent, or if there is some other
                             problem with spawning
        '''
        # Step 1: Acquire the dataset spawn arguments
        (binary, args) = self._get_agent_spawn_args()

        log.info("Spawning external child process with command: \n\n\t'%s %s'\n" % (binary, " ".join(args)))
        log.debug("External child process will have context string: %s" % context_str)
        
        # Step 2: Start the Dataset Agent (java) passing necessary spawn arguments
        try:
            proc = AgentOSProcess(binary, args)
            proc.spawn()
        except ValueError, ex:
            raise RuntimeError("JavaAgentWrapper._spawn_agent(): Received invalid spawn arguments form JavaAgentWrapper.agent_spawn_args" + str(ex))
        except OSError, ex:
            raise RuntimeError("JavaAgentWrapper._spawn_agent(): Failed to spawn the external Dataset Agent.  Error: %s" % (str(ex)))

        # write context json-like to stdin
        proc.transport.write(context_str)
        proc.transport.write('\n')
        proc.transport.closeStdin()
        
        return proc

    @defer.inlineCallbacks
    def op_pretty_print(self, content, headers, msg):
        """
        @brief: OP to grab the state of this service in a pretty formatted string (bow-ties not included)
        @return: Pretty Print
        """
        pretty = "Java Agent Wrapper Status:" + \
                 "\n--------------------------" + \
                 "\nService State:  " + str(self._get_state())
        res = yield self.reply_ok(msg, {'value':pretty}, {})
        defer.returnValue(res)

    @defer.inlineCallbacks
    def op_update_request(self, content, headers, msg):
        """
        @brief: Responds to the any request for update (by the Scheduler Service, or the Dataset Change Event Generator) by
                having the wrapped Dataset Agent perform an update and push the compliant data into the Resource Registry.
                
                This process involves:
                    1) Requesting the datasources current state (known as "context") from the Resource Registry
                    2) The update procedure is invoked via an RPC call to the underlying Dataset Agent using
                       the acquired "context"
                    3) The Dataset Agent performs the update assimilating data into CDM/CF compliant form,
                       pushes that data to the Resource Registry, and returns the new DatasetID.
                       
        @param content: An instance of GPB 7001 (ChangeEventMessage)
        
        @return: Replies to RPC caller with a string message signifying success/failure
        
        @see:   Dataset Agent Overview - https://docs.google.com/drawings/d/1T03WPCFUElW_ExUPccIodXU8eqy-CKxnZxscuWsBeAs/edit?hl=en_US&authkey=CMDVk_kL 
        """
        log.info("<<<---@@@ Service received operation 'update_request'.  Grabbing update context and Delegating to underlying dataset agent...")
        if not hasattr(content, 'MessageType') or content.MessageType != CHANGE_EVENT_TYPE:
            raise TypeError('The given content must be an instance or a wrapped instance %s.  Given: %s' % (repr(CHANGE_EVENT_TYPE), type(content)))

        result = yield self._update_request(content.dataset_id, content.data_source_id)

        if result:
            yield self.reply_ok(msg,'Java agent wrapper update succeeded!')
        else:
            yield self.reply_ok(msg,'Java agent wrapper skipped the update procedure')

    @defer.inlineCallbacks
    def _update_request(self, dataset_id, data_source_id):
        """
        @brief: Responds to the any request for update (by the Scheduler Service, or the Dataset Change Event Generator) by
                having the wrapped Dataset Agent perform an update and push the compliant data into the Resource Registry.
                
                This process involves:
                    1) Verifying the state of the dataset -- update will be cancelled if ResourceLifeCycleState is:
                            INACTIVE
                            DECOMMISSIONED
                            RETIRED
                    2) Requesting the datasources current state (known as "context") from the Resource Registry
                    3) The update procedure is invoked via an RPC call to the underlying Dataset Agent using
                       the acquired "context"
                    4) The Dataset Agent performs the update, organizes data in CDM/CF canonical form,
                       pushes that data to the Resource Registry, and returns the new DatasetID.
        
        @param dataset_id: The OOI Resource ID of the Dataset which has been updated
        @param data_source_id: The OOI Resource ID of the Data Source which has been updated
        @return: True if the update procedure completes normally, False if the update procedure is skipped for any reason
                 which doesn't raise an exception (such as when the dataset is INACTIVE).
        """
        log.info("JAW _update_request - Start")

        # Step 0: retrieve the dataset_id from the data_source_id if it is not provided -- and vise versa
        if dataset_id and not data_source_id:
            data_source_id = yield self._get_associated_data_source_id(dataset_id)
        elif data_source_id and not dataset_id:
            dataset_id = yield self._get_associated_dataset_id(data_source_id)
        elif not dataset_id and not data_source_id:
            raise JavaAgentWrapperException('Must provide data source or dataset ID!')
    
        # Step 1: Check the ResourceLifeCycleState of the dataset -- don't update if it is inactive, decommissioned, etc
        cancel_states = []

        dataset = yield self.rc.get_instance(dataset_id)
        cancel_states.append(dataset.INACTIVE)
        cancel_states.append(dataset.DECOMMISSIONED)
        cancel_states.append(dataset.RETIRED)
        
        r_state = dataset.ResourceLifeCycleState
        if r_state in cancel_states:
            log.warn('Dataset Resource has state "%s" and will not be updated.  (dataset: "%s", datasource: "%s")' % (r_state, dataset_id, data_source_id))
            defer.returnValue(False)

        datasource = yield self.rc.get_instance(data_source_id)
        r_state = datasource.ResourceLifeCycleState
        if r_state in cancel_states:
            log.warn('Datasource Resource has state "%s"; its dataset will not be updated.  (dataset: "%s", datasource: "%s")' % (r_state, dataset_id, data_source_id))
            defer.returnValue(False)
        

        # Step 2: Grab the context for the given dataset ID
        log.debug('Retrieving dataset update context via self._get_dataset_context()')
        try:
            (context, auth_obj, search_pattern_obj) = yield self._get_dataset_context(dataset_id, data_source_id)
        except KeyError, ex:
            err_msg = 'Failed to get dataset context!   %s' % str(ex)
            log.error(err_msg)
            raise JavaAgentWrapperException(err_msg)

        # Step 3: Setup a deferred so that we can wait for the Ingest Service to respond before sending data messages
        log.debug('Setting up ingest ready deferred...')
        self.__ingest_ready_deferred = defer.Deferred()
        
        # Step 4: Tell the Ingest Service to get ready for ingestion (create a new topic and await data messages)
        reply_to        = self.receiver.name
        ingest_timeout  = context['max_ingest_millis']/1000

        if log.getEffectiveLevel() <= logging.DEBUG:        
            log.debug('\n\ndataset_id:\t"%s"\nreply_to:\t"%s"\ntimeout:\t%i' % (dataset_id, reply_to, ingest_timeout))

        # Create the PerformIngestMessage
        begin_msg = yield self.mc.create_instance(PERFORM_INGEST_TYPE)
        begin_msg.dataset_id                = dataset_id
        begin_msg.datasource_id             = data_source_id
        begin_msg.reply_to                  = reply_to
        begin_msg.ingest_service_timeout    = ingest_timeout

        # @note: Can't use client because we want to access the defered and change the timeout!
        perform_ingest_deferred = self.rpc_send(self.get_scoped_name('system',"ingestion"), "ingest", begin_msg, timeout=ingest_timeout)

        # there is a possibility this ingestion call can error and callback before it can succeed in calling our ingest_ready,
        # which means we'll just spin forever below and cause all kinds of weird state. we need to prevent that from happening.
        def early_err(failure):
            # crash out of yield for ingest ready deferred
            if not self.__ingest_ready_deferred.called:
                self.__ingest_ready_deferred.errback(failure)

            # pass through what we got, no matter what.
            return failure

        def early_cb(result):
            if not self.__ingest_ready_deferred.called:
                self.__ingest_ready_deferred.errback(ReceivedError('Perform ingest deferred called back without calling ingest ready!'))

            # pass through result every time
            return result

        # these are designed to be passthroughs if nothing happened
        perform_ingest_deferred.addCallbacks(early_cb, early_err)
        
        # Step 5: When the deferred comes back, tell the Dataset Agent instance to send data messages to the Ingest Service
        # @note: This deferred is called when the ingest invokes op_ingest_ready()
        log.debug("Yielding on the ingest -- it'll callback when its ready to receive ingestion data")
        try:
            irmsg = yield self.__ingest_ready_deferred
        except ReceivedError, ex:
            log.error("Ingestion did not ever signal it was ready: %s" % str(ex))
            defer.returnValue(False)
        
        log.debug("Ingest is ready to receive data!")
        context['xp_name'] = irmsg.xp_name
        context['ingest_topic'] = irmsg.publish_topic

        # transform context/auth/search pattern objs into a json-like message
        context_str = "# EoiDataContextMessage:4501\n" + json.dumps(context, indent="  ")

        if auth_obj:
            context_str += "\n# ThreddsAuthentication:4504\n" + json.dumps(auth_obj, indent="  ")

        if search_pattern_obj:
            context_str += "\n# SearchPattern:4505\n" + json.dumps(search_pattern_obj, indent="  ")

        log.info('Create subscriber to bump timeouts...')
        self._subscriber = IngestionProcessingEventSubscriber(origin=dataset_id, process=self)
        def _increase_timeout(data):
            if hasattr(perform_ingest_deferred, 'rpc_call') and perform_ingest_deferred.rpc_call.active():
                log.debug("Ingestion (%s/%s) notified it is processing still (step: %s), increasing timeout by %d from now" % (data['content'].additional_data.ingestion_process_id, data['content'].additional_data.conv_id, data['content'].additional_data.processing_step, ingest_timeout))

                callable = perform_ingest_deferred.rpc_call.func   # extract the old callable, as cancel() deletes it
                perform_ingest_deferred.rpc_call.cancel()          # this is just the timeout, not the actual rpc call
                perform_ingest_deferred.rpc_call = reactor.callLater(ingest_timeout, callable)

        self._subscriber.ondata = _increase_timeout

        yield self.register_life_cycle_object(self._subscriber) # move subscriber to active state

        if log.getEffectiveLevel() == logging.INFO:
            log.info("@@@--->>> Sending update request to Dataset Agent with context...")
        elif log.getEffectiveLevel() <= logging.DEBUG:
            log.debug("@@@--->>> Sending update request to Dataset Agent with context...\n %s" % str(context_str))

        # spawn agent, sending it the context json-like str
        agent_proc = self._spawn_dataset_agent(context_str)

        # chain agent_proc's errback to perform_ingest_deferred's - just in case of a bad startup
        def _chain_agent_errback(failure):
            if not perform_ingest_deferred.called:
                log.error("I DYDE A DEATH %s" % str(failure))
                perform_ingest_deferred.errback(failure)
        agent_proc.deferred_exited.addErrback(_chain_agent_errback)
        
        log.debug('Yielding until ingestion is complete on the ingestion services side...')

        try:
            (ingest_result, ingest_headers, ingest_msg) = yield perform_ingest_deferred
        except defer.TimeoutError:
            err_msg = 'JAW timed out client side and will tell DAC to cease transmission (while updating dataset "%s")' % dataset_id
            log.error(err_msg)

            raise JavaAgentWrapperException(err_msg)

        except ReceivedError, ex:
            err_msg = 'Ingestion raised an error (while updating dataset "%s"): %s' % (dataset_id, str(ex.msg_content.MessageResponseBody))
            log.error(err_msg)

            raise JavaAgentWrapperException(err_msg)

        except OSProcessError, ex:
            err_msg = 'Agent had a bad startup or exited with non-zero status: %s' % str(ex)
            log.error(err_msg)

            raise JavaAgentWrapperException(err_msg)

        finally:
            # cleanup timeout-increasing subscriber
            self._registered_life_cycle_objects.remove(self._subscriber)
            yield self._subscriber.terminate()
            self._subscriber = None

            # tell dataset agent to stop doing its thing - it's likely closed at this point anyway if success
            # will force after default timeout of 5 sec
            try:
                yield agent_proc.close()
            except OSProcessError:
                pass

        log.debug('Ingestion is complete on the ingestion services side...')

        defer.returnValue(True)

    @defer.inlineCallbacks
    def op_ingest_ready(self, content, headers, msg):
        '''
        @brief: OP invoked when the ingestion service is ready to receive dataset updates.  When this
                op is invoked it calls back to the ingest_ready_deferred which is being yielded on
                in the update notification response method _update_request()
        
        @param content: The Ingest Ready Message
        '''
        log.info('<<<---@@@ Incoming notification: Ingest is ready to receive data')
        if self.__ingest_ready_deferred is not None:
            self.__ingest_ready_deferred.callback(content)
        
        yield msg.ack()

    @defer.inlineCallbacks
    def _get_dataset_context(self, datasetID, dataSourceID):
        '''
        @brief: Requests the current state of the given datasetID from the Resource Registry and returns that state as
                "context" for future update procedures.
        
                (For the purposes of elaboration this method simply returns a cached context from a dictionary which has
                been keyed to the given datasetID; communication with the Resource Registry does NOT occur)
        '''
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug(" -[]- Entered _get_dataset_context(datasetID=%s, dataSourceID=%s); state=%s" % (datasetID, dataSourceID, str(self._get_state())))
        
        if not datasetID and not dataSourceID:
            raise JavaAgentWrapperException('Must provide data source or dataset ID!')

        
        log.debug("  |--->  Retrieving dataset instance")
        dataset = yield self.rc.get_instance(datasetID)
        
        log.debug("  |--->  Retrieving datasource instance")
        datasource = yield self.rc.get_instance(dataSourceID)

        context = {}
        
        # Create an instance of the EoiDataContext message
        log.debug("Storing data in EoiDataContext fields")
        testing = False
        
        if str(datasetID).startswith(TESTING_SIGNIFIER):
            testing = True
        context['source_type'] = datasource.source_type
        context['dataset_id'] = datasetID
        context['datasource_id'] = dataSourceID

        try:
            # Set the start time of this update to the last time in the dataset (ion_time_coverage_end)
            # minus the start time offset provided by the datasource (starttime_offset_millis)
            string_time = dataset.root_group.FindAttributeByName('ion_time_coverage_end')
            start_time_seconds = calendar.timegm(time.strptime(string_time.GetValue(), '%Y-%m-%dT%H:%M:%SZ'))
            start_time_seconds -= int(datasource.starttime_offset_millis * 0.001)
            context['is_initial'] = False

        except OOIObjectError, oe:
            # Set the start time of this update to the current time minus the initial start time offset
            # provided by the datasource (initial_starttime_offset_millis)
            # @note: Default to 1 day if initial_starttime_offset_millis is not provided
            log.warn('No start time attribute found in new dataset! This is okay - expected!')
            initial_offset = datasource.initial_starttime_offset_millis
            if initial_offset == 0:
                initial_offset = 86400000 # 1 day in millis
            start_time_seconds = int(time.time() - (initial_offset * 0.001))

            # TESTING-HACK:  Force the start time to reference from the 2nd timestep of the test datasets, 2011 Sep 02 03:00:00
            # ==> must use the second timestep to trigger the erroneous index calculation as a "0" based initial dataset SEEMS to work properly (rivers)!!!
            #start_time_seconds = int(1315018800.0 - (initial_offset * 0.001))
            context['is_initial'] = True

        except AttributeError, ae:
            # Set the start time of this update to the current time minus the initial start time offset
            # provided by the datasource (initial_starttime_offset_millis)
            # @note: Default to 1 day if initial_starttime_offset_millis is not provided
            log.exception('No start time attribute found in empty dataset! This is bad - should not happen!  Setting start time to TODAY minus the initial starttime offset.')
            initial_offset = datasource.initial_starttime_offset_millis
            if initial_offset == 0:
                initial_offset = 86400000 # 1 day in millis
            start_time_seconds = int(time.time() - (initial_offset * 0.001))
            context['is_initial'] = True

        if testing:
            log.debug('Using Test Delta time....')
            deltaTime = 3*86400 # 3 days in seconds
            end_time_seconds = start_time_seconds + deltaTime
        else:
            # Set the end time of this update to the current time plus the end time offset
            # provided by the datasource (endtime_offset_millis)
            # @note: Default to 1 day if endtime_offset_millis is not provided
            end_time_offset = datasource.endtime_offset_millis
            end_time_seconds = int(time.time() + (end_time_offset * 0.001))
            

        stime = time.strftime("%Y-%m-%d'T'%H:%M:%S", time.gmtime(start_time_seconds))
        log.info('Getting Data Start time: %s' % stime)
        
        etime = time.strftime("%Y-%m-%d'T'%H:%M:%S", time.gmtime(end_time_seconds))
        log.info('Getting Data End time: %s' % etime)

        context['start_datetime_millis'] = start_time_seconds * 1000

        context['end_datetime_millis'] = end_time_seconds * 1000

        context['property'] = [str(x) for x in datasource.property]
        context['station_id'] = [str(x) for x in datasource.station_id]

        context['request_type'] = datasource.request_type
        context['request_bounds_north'] = datasource.request_bounds_north
        context['request_bounds_south'] = datasource.request_bounds_south
        context['request_bounds_west'] = datasource.request_bounds_west
        context['request_bounds_east'] = datasource.request_bounds_east
        context['base_url'] = datasource.base_url
        context['dataset_url'] = datasource.dataset_url
        context['ncml_mask'] = datasource.ncml_mask
        context['max_ingest_millis'] = datasource.max_ingest_millis
        context['timestep_file_count'] = datasource.timestep_file_count

        # Add subranges to the context message
        for i, r in enumerate(datasource.sub_ranges):
            s = { 'dim_name':    r.dim_name,
                  'start_index': r.start_index,
                  'end_index':   r.end_index }
            if not context.has_key('sub_ranges'):
                context['sub_ranges'] = []
            context['sub_ranges'].append(s)

        auth_obj = None
        if datasource.IsFieldSet('authentication'):
            log.info('Setting: authentication')
            # object is 4504/1, ThreddsAuthentication
            auth_obj = {'name':     datasource.authentication.name,
                        'password': datasource.authentication.password }

        search_pattern_obj = None
        if datasource.IsFieldSet('search_pattern'):
            log.info('Setting: search_patern')
            # object is 4505/1, SearchPattern
            search_pattern_obj = {'dir_pattern':  datasource.search_pattern.dir_pattern,
                                  'file_pattern': datasource.search_pattern.file_pattern,
                                  'join_name':    datasource.search_pattern.join_name}

        # return a tuple of these three dicts
        defer.returnValue((context, auth_obj, search_pattern_obj))

    @defer.inlineCallbacks
    def _get_associated_data_source_id(self, dataset_id):
        """
        @brief: Utilizes the associations framework to find the OOI Resource ID of the Data Source which is associated
                with the given dataset_id via the HAS_A predicate (association)
        
        @param dataset_id: The OOI Resource ID of the Dataset
        @return: a deferred containing the OOI Resource ID of the Data Source which is associated
        """
        result = ""
        
        # Step 1: Build the request Predicate Object Query message
        request = yield self.mc.create_instance(PREDICATE_OBJECT_QUERY_TYPE)
        pair = request.pairs.add()
        
        # Step 2: Create the predicate and object for the query
        predicate = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        object   = request.CreateObject(IDREF_TYPE) 
        predicate.key = HAS_A_ID
        object.key   = dataset_id
        
        pair.object   = object
        pair.predicate = predicate
        
        # Step 3: Make the request
        # @attention: In future implementations, get_subjects will allow us to specify
        #             that we only want to return DataSourceResource objects -- for now
        #             we have to manually check the object types returned...
        log.info('Loading all associated DataSourceResource objects...')
        associations = yield self.asc.get_subjects(request)

        # Step 4:
        if associations is None or associations.idrefs is None:
            log.info('No prior DataSourceResource associated with this DatasetResource')
            result = ""
        else:
            for idref in associations.idrefs:
                try:
                    id = idref.key
                    res = yield self.rc.get_instance(id)
                    if res.ResourceObjectType == DATA_SOURCE_TYPE:
                        result = res.ResourceIdentity
                        break
                except Exception, ex:
                    log.error('Error retrieving associated resource "%s":  %s' % (str(id), str(ex)))


        defer.returnValue( result)

    @defer.inlineCallbacks
    def _get_associated_dataset_id(self, data_source_id):
        """
        @brief: Utilizes the associations framework to find the OOI Resource ID of the Data Source which is associated
                with the given dataset_id via the HAS_A predicate (association)
        
        @param data_source_id: The OOI Resource ID of a Data Source
        @return: a deferred containing the OOI Resource ID of the Dataset which is associated
        """
        result = ""
        
        # Step 1: Build the request Subject Predicate Query message
        request = yield self.mc.create_instance(SUBJECT_PREDICATE_QUERY_TYPE)
        pair = request.pairs.add()
        
        # Step 2: Create the predicate and subject for the query
        predicate = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        subject   = request.CreateObject(IDREF_TYPE) 
        predicate.key = HAS_A_ID
        subject.key   = data_source_id
        
        pair.subject   = subject
        pair.predicate = predicate
        
        # Step 3: Make the request
        # @attention: In future implementations, get_objects will allow us to specify
        #             that we only want to return DatasetResource objects -- for now
        #             we have to manually check the object types returned...
        log.info('Loading all associated DatasetResource objects...')
        associations = yield self.asc.get_objects(request)

        # Step 4:
        if associations is None or associations.idrefs is None:
            log.info('No prior DatasetResource associated with this DataSourceResource')
            result = ""
        else:
            for idref in associations.idrefs:
                try:
                    id = idref.key
                    res = yield self.rc.get_instance(id)
                    if res.ResourceObjectType == DATASET_TYPE:
                        result = res.ResourceIdentity
                        break
                except Exception, ex:
                    log.error('Error retrieving associated resource "%s":  %s' % (str(id), str(ex)))


        defer.returnValue(result)
    
    def _get_agent_spawn_args(self):
        '''
        @brief: Lazy-initializes self.__agent_spawn_args
        '''
        jar_pathname = CONF.getValue('dataset_agent_jar_path', None)

        if jar_pathname is None or not os.path.exists(jar_pathname):
            raise IonError("JAR for dataset agent not found: (%s)" % jar_pathname)
            #raise JavaAgentWrapperException("JAR for dataset agent not found: (%s)" % jar_pathname)
        
        parent_host_name = self.container.exchange_manager.message_space.hostname
        parent_xp_name = self.container.exchange_manager.exchange_space.name

        binary = "java"
        args = ["-Xmx512m", "-jar", jar_pathname, parent_host_name, parent_xp_name]
        log.debug("Acquired external process's spawn arguments:  %s %s" % (binary, " ".join(args)))
        return (binary, args)
        

class JavaAgentWrapperClient(ServiceClient):
    """
    @brief: Test client for direct (RPC) interaction with the JavaAgentWrapper ServiceProcess
    """
    
    def __init__(self, *args, **kwargs):
        # Step 1: Delegate initialization to parent "ServiceClient"
        kwargs['targetname'] = 'java_agent_wrapper'
        ServiceClient.__init__(self, *args, **kwargs)
        
        # Step 2: Perform Initialization
        self.mc = MessageClient(proc=self.proc)
        self.rc = ResourceClient(proc=self.proc)
        
    
    @defer.inlineCallbacks
    def rpc_pretty_print(self):
        '''
        @brief: Retrieve the state of the JavaAgentWrapper Service
        '''
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('pretty_print', None)
        
        if log.getEffectiveLevel() == logging.INFO:
            log.info("<<<---@@@ Incoming rpc reply")
        elif log.getEffectiveLevel() <= logging.DEBUG:
            # Prevent these lines from getting interlaced with other logs by providing them in one shot
            log.debug("<<<---@@@ Incoming rpc reply...\n\t... Content\t%s\n\t... Headers\t%s\n\t... Message\t%s" % (str(content), str(headers), str(msg)))
            
        
        result = ""
        if 'value' in content:
            result = content['value']
        else:
            result = str(content)

        defer.returnValue(result)


    @defer.inlineCallbacks
    def request_update(self, datasetID, datasourceID):
        '''
        @brief: Simulates an update request to the JavaAgentWrapper as if from the Scheduler Service
        '''
        # Ensure a Process instance exists to send messages FROM...
        #   ...if not, this will spawn a new default instance.
        yield self._check_init()
        
        # Create the Change event (just as the scheduler would)
        change_event = yield self.mc.create_instance(CHANGE_EVENT_TYPE)
        change_event.data_source_id = datasourceID
        change_event.dataset_id = datasetID
        
        # Invoke [op_]update_request() on the target service 'java_agent_wrapper' via RPC
        log.info("@@@--->>> Client sending 'update_request' message to java_agent_wrapper service")

#        (content, headers, msg) = yield self.send('update_request', change_event)
        yield self.rpc_send('update_request', change_event, timeout=300)
        
        defer.returnValue(None)
        
        
    
# Spawn of the process using the module name
factory = ProcessFactory(JavaAgentWrapper)



'''

#----------------------------#
# Application Startup
#----------------------------#
# @todo: change res/apps/eoiagent to start the dependencies in resource.app and call its bootstrap
#        to create the demo dataset and datasource.  Also include ingestion dependency
#        For now..  bootstrap the resource app and spawn the ingest manually 
:: bash ::
bin/twistd -n cc -h amoeba.ucsd.edu -a sysname=eoitest,register=demodata res/apps/resource.app

:: py ::
from ion.services.dm.ingestion.ingestion import IngestionClient
spawn('ingestion')



#----------------------------#
# Update Testing
#----------------------------#
:: py ::
from ion.integration.eoi.agent.java_agent_wrapper import JavaAgentWrapperClient as jawc
client = jawc()
spawn("ion.integration.eoi.agent.java_agent_wrapper")

client.request_update(sample_profile_dataset, sample_profile_datasource)



#----------------------------#
# All together now!
#----------------------------#
from ion.services.dm.ingestion.ingestion import IngestionClient
from ion.integration.eoi.agent.java_agent_wrapper import JavaAgentWrapperClient as jawc
spawn('java_agent_wrapper')
spawn('ingestion')
client = jawc()

client.request_update(sample_profile_dataset, sample_profile_datasource)
client.request_update(sample_station_dataset, sample_station_datasource)
client.request_update(sample_traj_dataset, sample_traj_datasource)

'''












