#!/usr/bin/env python

"""
@file:   ion/integration/eoi/agent/java_agent_wrapper.py
@author: Chris Mueller
@author: Tim LaRocque
@brief:  EOI JavaAgentWrapper and JavaAgentWrapperClient class definitions
"""

# Imports:
import os, time, calendar
from ion.core.messaging.message_client import MessageClient
from ion.core.object import object_utils
from ion.core.object.gpb_wrapper import OOIObjectError
from ion.core.process.process import Process, ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.services.coi.resource_registry.resource_client import \
    ResourceClient
from ion.services.dm.distribution.events import ScheduleEventSubscriber, TRIGGER_EVENT_MESSAGE_TYPE
from ion.services.dm.scheduler.scheduler_service import SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE

from ion.util.os_process import OSProcess
from ion.util.state_object import BasicStates
from twisted.internet import defer, reactor
import ion.util.ionlog

from ion.core.exception import IonError

import ion.util.procutils as pu
log = ion.util.ionlog.getLogger(__name__)
from ion.services.dm.ingestion.ingestion import IngestionClient
from ion.services.dm.inventory.association_service import AssociationServiceClient
from ion.services.coi.datastore_bootstrap.ion_preload_config import TESTING_SIGNIFIER

from ion.services.dm.distribution.publisher_subscriber import Subscriber


from ion.core.exception import ApplicationError

from ion.core import ioninit
CONF = ioninit.config(__name__)

# Imports: Builtin
import datetime

# Imports: Message object creation
from ion.services.dm.inventory.association_service import PREDICATE_OBJECT_QUERY_TYPE, IDREF_TYPE, SUBJECT_PREDICATE_QUERY_TYPE
from ion.services.coi.datastore_bootstrap.ion_preload_config import HAS_A_ID
DATA_CONTEXT_TYPE = object_utils.create_type_identifier(object_id=4501, version=1)
CHANGE_EVENT_TYPE = object_utils.create_type_identifier(object_id=7001, version=1)
PERFORM_INGEST_TYPE = object_utils.create_type_identifier(object_id=2002, version=1)

ASSOCIATION_TYPE = object_utils.create_type_identifier(object_id=13, version=1)
PREDICATE_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=25, version=1)
RESOURCE_CFG_REQUEST_TYPE = object_utils.create_type_identifier(object_id=10, version=1)
IDENTITY_TYPE = object_utils.create_type_identifier(object_id=1401, version=1)

DATA_SOURCE_TYPE = object_utils.create_type_identifier(object_id=4503, version=1)
DATASET_TYPE = object_utils.create_type_identifier(object_id=10001, version=1)

class JavaAgentWrapperException(ApplicationError):
    """
    An exception calss for the Java Agent Wrapper

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
        log.debug('Got an update event message from the scheduler')

        content = data['content']

        update_msg = content.additional_data.payload


        yield self.hook_fn(update_msg.dataset_id, update_msg.datasource_id)


class JavaAgentWrapper(ServiceProcess):
    """
    Class designed to facilitate (Java) Dataset Agent's tight interaction with ION in
    lieu of an incomplete Java CC.  Wrapping Java Agents in Python processes in this way
    provides the following functionality:
    Agent registration, process lifecycle (governance), and reactivity to other core ION services
    """
    
    
    declare = ServiceProcess.service_declare(name='java_agent_wrapper',
                                             version='0.1.0',
                                             dependencies=[]) # no dependencies

        
    def __init__(self, *args, **kwargs):
        '''
        Initialize the JavaAgentWrapper instance, init instance fields, etc.
        '''
        # Step 1: Delegate initialization to parent "ServiceProcess"
        log.info('')
        log.info('Initializing class instance...')
        ServiceProcess.__init__(self, *args, **kwargs)
        
        # Step 2: Create class attributes
        self.__agent_phandle = None
        self.__agent_binding = None
        self.__agent_updt_op = None
        self.__agent_term_op = None
        self.__agent_spawn_args = None
        self.__binding_key_deferred = None
        self.__ingest_client = None
        self.__ingest_ready_deferred = None
        
        self.queue_name = self.spawn_args.get('queue_name',
                                            CONF.getValue('queue_name', default='java_agent_wrapper_updates'))

    @defer.inlineCallbacks
    def slc_init(self):
        '''
        Initialization upon Service spawning.  This life-cycle process, in-turn, spawns the
        a Dataset Agent for which it is providing governance
        '''
        log.debug(" -[]- Entered slc_init(); state=%s" % (str(self._get_state())))
        # Step 1: Delegate initialization to parent class
        # The scl_init method of ServiceProcess is empty - it is there only to be over ridden in sub classes
        #yield defer.maybeDeferred(ServiceProcess.slc_init, self)
        
        # Step 2: Perform Initialization
        self.mc = MessageClient(proc=self)
        self.rc = ResourceClient(proc=self)
        self._asc = None   # Access using the property self.asc
        
        # Step 2: Spawn the associated external child process (if not already done)
        res = yield defer.maybeDeferred(self._spawn_dataset_agent)

        # Step 3: Setup schedule event subscriber
        log.debug('Creating new message receiver for scheduled updates')
        self.update_handler = UpdateHandler(self._update_request,
                                queue_name=self.queue_name,
                                origin=SCHEDULE_TYPE_PERFORM_INGESTION_UPDATE,
                                process=self)
        # Add the receiver as a registered life cycle object
        yield self.register_life_cycle_object(self.update_handler)


    @defer.inlineCallbacks
    def slc_activate(self):
        '''
        Service activation during spawning.  Returns a deferred which does not
        resolve until the external Dataset Agent remotely invokes op_binding_key_callback()
        @return: defer.Deferred()
        '''
        log.debug(" -[]- Entered slc_activate(); state=%s" % (str(self._get_state())))
        # Step 1: Delegate initialization to parent class
        # slc_activate is only for over ride in subclasses - no need to callit!
        yield defer.maybeDeferred(ServiceProcess.slc_activate, self)
        
        # Step 2: Suspend execution until receipt of the external child process's binding key
        def _recieve_binding_key(self):
            d = self.__binding_key_deferred
            if d is None:
                d = defer.Deferred()
                self.__binding_key_deferred = d
            return d
        
        d = defer.Deferred()
        
        reactor.callLater(0, lambda: _recieve_binding_key(self))

    
    @defer.inlineCallbacks
    def slc_deactivate(self):
        '''
        '''
        log.debug(" -[]- Entered JavaAgentWrapper.slc_deactivate(); state=%s" % (str(self._get_state())))
        
        returncode= None
        if self._get_state() == BasicStates.S_ACTIVE:
            # Step 1: Cancel the termination callback of the underlying process
            cb_list = self.__agent_phandle.deferred_exited.callbacks
            cb_tuple = (
                          (self._osp_terminate_callback, (), {}),
                          (self._osp_terminate_callback, (), {})
                       )
            if cb_tuple in cb_list:
                cb_list.remove(cb_tuple)
            
            # Step 2: Terminate the underlying dataset agent
            result = yield self._terminate_dataset_agent()
        
            # Step 3: Log output from the underlying agent
            returncode = result["exitcode"]
#            outlines = result["outlines"]
#            errlines = result["errlines"]
            msg1 = "External child process terminated.  RETURN CODE == %s" % str(returncode)
#            msg2 = "External child process terminated with the following log:\r"
#            if outlines is not None:
#                msg2 += '\r'.join(outlines) + '\r'
#            if errlines is not None:
#                msg2 += '\r'.join(errlines)
            log.info(msg1)
#            log.debug(msg2)
        else:
            log.warning('slc_deactivate():  This Service Process is expected to be in the "%s" state.  Deactivation may have occured prematurely.  state=%s' % (str(BasicStates.S_ACTIVE), str(self._get_state())))
            

        defer.returnValue(returncode)
    
    
    @defer.inlineCallbacks
    def slc_terminate(self):
        '''
        Termination life cycle process.  This affect also terminates the Java Dataset Agent which
        this class is intended to wrap.
        '''
        log.debug(" -[]- Entered slc_terminate(); state=%s" % (str(self._get_state())))
        # Step 1: Perform necessary cleanup
        # ....
        log.info('Cleaning up resources...')
        
        
        # Step 2: Finish termination by delegating to parent
        res = yield defer.maybeDeferred(ServiceProcess.slc_terminate, self)
        log.info('Service Life Cycle termination complete.')
        defer.returnValue(res)


    def on_deactivate(self, *args, **kwargs):
        """
        No imlementation: State change ONLY!
        @todo: change the BasicLifeCycleObject transition to state change only instead of implementing this method
        """
        log.debug(" -[]- Entered on_deactivate(); state=%s" % (str(self._get_state())))
        
    def _spawn_dataset_agent(self):
        '''
        Instantiates the Java Dataset Agent providing appropriate binding information so the underlying agent can establish messaging channels
        '''
        log.debug(" -[]- Entered _spawn_dataset_agent(); state=%s" % (str(self._get_state())))
        # @todo: rethink this check
        if self._get_state() is not BasicStates.S_INIT:
            err_msg = "External child process cannot be spawned unless %s's service state is %s" % (__name__, str(BasicStates.S_READY))
            log.warn(err_msg)
            raise RuntimeError(err_msg)
        
        # Step 1: Acquire the dataset spawn arguments
        (binary, args) = self.agent_spawn_args
        log.info("Spawning external child process with command: \n\n\t'%s %s'\n" % (binary, " ".join(args)))
        
        # Step 2: Start the Dataset Agent (java) passing necessary spawn arguments
        try:
            proc = OSProcess(binary, args)
            proc.spawn()
            proc.deferred_exited.addBoth(self._osp_terminate_callback)
            # @todo Add a callback which forces this service to terminate if the underlying dataset agent exits (when the deferred returns)
        except ValueError, ex:
            raise RuntimeError("JavaAgentWrapper._spawn_agent(): Received invalid spawn arguments form JavaAgentWrapper.agent_spawn_args" + str(ex))
        except OSError, ex:
            raise RuntimeError("JavaAgentWrapper._spawn_agent(): Failed to spawn the external Dataset Agent.  Error: %s" % (str(ex)))

        # Step 3: Maintain a reference to the OSProcess object for later communication
        self.__agent_phandle = proc
        return True
        
    @defer.inlineCallbacks
    def _terminate_dataset_agent(self):
        '''
        Terminates the underlying dataset agent by sending it a 'terminate' message and waiting for the OSProcess object's exit callback.  
        '''
        yield
        log.debug(" -[]- Entered _terminate_dataset_agent(); state=%s" % (str(self._get_state())))
        result = None
        if self._get_state() is BasicStates.S_ACTIVE:
            # Step 1: Send a terminate message to the java dataset agent
            log.info("@@@--->>> Sending termination request to external child process")
            yield self.send(self.agent_binding, self.agent_term_op, {})
            
            # Step 2: Suspend execution until the agent process exits
            result = yield self.__agent_phandle.deferred_exited
            self.__agent_binding = None
            self.__agent_phandle = None
        elif self._get_state() is BasicStates.S_READY:
            errmsg = "External child process has not been spawned, and therefore can not be terminated"
            log.warning(errmsg)

        
        defer.returnValue(result)

    @property
    def asc(self):
        if self._asc is None:
            self._asc = AssociationServiceClient(proc=self)
        return self._asc

    def _osp_terminate_callback(self, result):
        log.info('External child process has terminated; Wrapper Service shutting down...')
        self.deactivate()
        self.terminate()
        return result

    @defer.inlineCallbacks
    def op_pretty_print(self, content, headers, msg):
        pretty = "Java Agent Wrapper Status:" + \
                 "\n--------------------------" + \
                 "\nService State:  " + str(self._get_state())
        res = yield self.reply_ok(msg, {'value':pretty}, {})
        defer.returnValue(res)
        
        
    @defer.inlineCallbacks
    def op_do_shutdown(self, content, headers, msg):
        res = self.shutdown()
        res = yield self.reply_ok(msg, {'value':res}, {})
        defer.returnValue(res)

    
    @defer.inlineCallbacks
    def op_do_deactivate(self, content, headers, msg):
        res = yield defer.maybeDeferred(self.deactivate)
        res = yield self.reply_ok(msg, {'value':res}, {})
        defer.returnValue(res)
    

    @defer.inlineCallbacks
    def op_update_request(self, content, headers, msg):
        '''
        Responds to the Scheduler Service's request for update by having the wrapped Dataset Agent
        perform an update and push the compliant data into the Resource Registry.
        This process involves:
            1) Requesting the datasources current state (known as "context") from the Resource Registry
            2) The update procedure is invoked via an RPC call to the underlying Dataset Agent using
               the acquired "context"
            3) The Dataset Agent performs the update assimilating data into CDM/CF compliant form,
               pushes that data to the Resource Registry, and returns the new DatasetID. 
        '''
        log.info("<<<---@@@ Service received operation 'update_request'.  Grabbing update context and Delegating to underlying dataset agent...")
        if not hasattr(content, 'MessageType') or content.MessageType != CHANGE_EVENT_TYPE:
            raise TypeError('The given content must be an instance or a wrapped instance %s.  Given: %s' % (repr(CHANGE_EVENT_TYPE), type(content)))

        result = yield self._update_request(content.dataset_id, content.data_source_id)

        if result:
            yield self.reply_ok(msg,'Java agent wrapper succeeded!')
            
        else:
            yield self.reply_err(msg,'Java agent wrapper failed!')



    @defer.inlineCallbacks
    def _update_request(self, dataset_id, data_source_id):
    
        # Step 1: Grab the context for the given dataset ID
        log.debug('Retrieving dataset update context via self._get_dataset_context()')
        try:
            context = yield self._get_dataset_context(dataset_id, data_source_id)
        except KeyError, ex:
            log.exception('Faild to get dataset context!')
            defer.returnValue(False)

        # Step 2: Setup a deferred so that we can wait for the Ingest Service to respond before sending data messages
        log.debug('Setting up ingest ready deferred...')
        self.__ingest_ready_deferred = defer.Deferred()
        
        # Step 3: Tell the Ingest Service to get ready for ingestion (create a new topic and await data messages)
        #log.debug('Tell the ingest to start the ingestion procedure via op_perform_ingest()..')
        #if self.__ingest_client is None:
        #    self.__ingest_client = IngestionClient(proc=self)
        
        reply_to        = self.receiver.name
        ingest_timeout  = context.max_ingest_millis
        
        log.debug('\n\ndataset_id:\t"%s"\nreply_to:\t"%s"\ntimeout:/t%i' % (dataset_id, reply_to, ingest_timeout))

        # Create the PerformIngestMessage
        begin_msg = yield self.mc.create_instance(PERFORM_INGEST_TYPE)
        begin_msg.dataset_id                = dataset_id
        begin_msg.datasource_id             = data_source_id
        begin_msg.reply_to                  = reply_to
        begin_msg.ingest_service_timeout    = ingest_timeout

        # Can't use client because we want to access the defered and change the timeout!
        perform_ingest_deferred = self.rpc_send(self.get_scoped_name('system',"ingestion"), "ingest", begin_msg, timeout=ingest_timeout)
        
        # Step 4: When the deferred comes back, tell the Dataset Agent instance to send data messages to the Ingest Service
        # @note: This deferred is called by when the ingest invokes op_ingest_ready()
        log.debug("Yielding on the ingest -- it'll callback when its ready to receive ingestion data")
        irmsg = yield self.__ingest_ready_deferred
        log.debug("Ingest is ready to receive data!")
        context.xp_name = irmsg.xp_name
        context.ingest_topic = irmsg.publish_topic


        log.info('Create subscriber to bump timeouts...')
        self._subscriber = Subscriber(xp_name="magnet.topic",
                                             binding_key=dataset_id,
                                             process=self)

        def increase_timeout(data):
            log.debug("INCREASING TIMEOUT")
            if hasattr(perform_ingest_deferred, 'rpc_call'):
                perform_ingest_deferred.rpc_call.delay(ingest_timeout/1000)

        self._subscriber.ondata = increase_timeout


        yield self.register_life_cycle_object(self._subscriber) # move subscriber to active state


#        yield  self.__ingest_client.demo(ingest_topic)
        log.info("@@@--->>> Sending update request to Dataset Agent with context...")
        log.debug("..." + str(context))
        yield self.send(self.agent_binding, self.agent_update_op, context)
        
        log.debug('Yielding until ingestion is complete on the ingestion services side...')



        (ingest_result, ingest_headers, ingest_msg) = yield perform_ingest_deferred

        self._registered_life_cycle_objects.remove(self._subscriber)
        yield self._subscriber.terminate()
        self._subscriber = None

        log.debug('Ingestion is complete on the ingestion services side...')

        defer.returnValue(True)

    @defer.inlineCallbacks
    def op_ingest_ready(self, content, headers, msg):
        '''
        '''
        log.info('<<<---@@@ Incoming notification: Ingest is ready to receive data')
        if self.__ingest_ready_deferred is not None:
            self.__ingest_ready_deferred.callback(content)
        
        yield msg.ack()

    
    def op_binding_key_callback(self, content, headers, msg):
        '''
        Caches the given binding_key for future communication between the JavaAgentWrapper and its underlying
        Java Dataset Agent.  This method is invoked remotely from the Dataset Agent during its initialization.
        '''
        log.info("<<<---@@@ Incoming callback with binding key message")
        log.debug("...Content:\t" + str(content))
        
        self.__agent_binding = str(content)
        if self.__binding_key_deferred is not None and 'result' not in dir(self.__binding_key_deferred):
            # @todo: This will error if callback is called more than once
            self.__binding_key_deferred.callback(self.__agent_binding)
        else:
            # @todo: this should be an error
            # If this callback is invoked manually before this service is spawned this would occur
            pass
        log.info("Accepted Dataset Agent binding key: '%s'" % (self.__agent_binding))
        return True
    
    @defer.inlineCallbacks
    def _get_dataset_context(self, datasetID, dataSourceID):
        '''
        Requests the current state of the given datasetID from the Resource Registry and returns that state as
        "context" for future update procedures.

        (For the purposes of elaboration this method simply returns a cached context from a dictionary which has
        been keyed to the given datasetID; communication with the Resource Registry does NOT occur)
        '''
        # @todo: this method will be reimplemented so that dataset contexts can be retrieved dynamically
        log.debug(" -[]- Entered _get_dataset_context(datasetID=%s, dataSourceID=%s); state=%s" % (datasetID, dataSourceID, str(self._get_state())))
        
        # Retreive the datasetID from the dataSourceID if it is not provided -- and vise versa
        if datasetID and not dataSourceID:
            dataSourceID = yield self._get_associated_data_source_id(datasetID)
        elif dataSourceID and not datasetID:
            datasetID = yield self._get_associated_dataset_id(dataSourceID)
        elif not datasetID and not dataSourceID:
            raise JavaAgentWrapperException('Must provide data source or dataset ID!')

        
        log.debug("  |--->  Retrieving dataset instance")
        dataset = yield self.rc.get_instance(datasetID)
        
        log.debug("  |--->  Retrieving datasource instance")
        datasource = yield self.rc.get_instance(dataSourceID)
        
        log.debug("  |--->  Creating EoiDataContext instance")
        msg = yield self.mc.create_instance(DATA_CONTEXT_TYPE)
        
        
        # @see: ion.play.hello_message.py for create message object instances
        # @see: use msg_instance.MessageObject.SOS (example of accessing GPB enum fields)
        # Create an instance of the EoiDataContext message


        log.debug("Storing data in EoiDataContext fields")
        testing = False
        if str(datasetID).startswith(TESTING_SIGNIFIER):
            testing = True
        msg.source_type = datasource.source_type

        """
        log.debug(dataset)
        log.debug('SKNIDNSOSKNDSOKNDSLDNJDSNBSD')
        log.debug(dataset.Resource)
        log.debug('SKNIDNSOSKNDSOKNDSLDNJDSNBSD')

        log.debug(dataset.ResourceObject)
        log.debug('SKNIDNSOSKNDSOKNDSLDNJDSNBSD ro' + str(type(dataset.ResourceObject)))
        log.debug(dataset.ResourceObject.Debug())

        log.debug(dir(dataset))

        rg = dataset.root_group
        log.debug('SKNIDNSOSKNDSOKNDSLDNJDSNBSD rg' + str(type(rg)))
        log.debug(rg.Debug())
        log.debug(dataset.ResourceObject.PPrint())
        """

        try:
            # Set the start time of this update to the last time in the dataset (ion_time_coverage_end)
            # minus the start time offset provided by the datasource (starttime_offset_millis)
            string_time = dataset.root_group.FindAttributeByName('ion_time_coverage_end')
            start_time_seconds = calendar.timegm(time.strptime(string_time.GetValue(), '%Y-%m-%dT%H:%M:%SZ'))
            start_time_seconds -= int(datasource.starttime_offset_millis * 0.001)

        except OOIObjectError, oe:
            # Set the start time of this update to the current time minus the initial start time offset
            # provided by the datasource (initial_starttime_offset_millis)
            # @note: Default to 1 day if initial_starttime_offset_millis is not provided
            log.warn('No start time attribute found in new dataset! This is okay - expected!')
            initial_offset = datasource.initial_starttime_offset_millis
            if initial_offset == 0:
                initial_offset = 86400000 # 1 day in millis
            start_time_seconds = int(time.time() - (initial_offset * 0.001))

        except AttributeError, ae:
            # Set the start time of this update to the current time minus the initial start time offset
            # provided by the datasource (initial_starttime_offset_millis)
            # @note: Default to 1 day if initial_starttime_offset_millis is not provided
            log.exception('No start time attribute found in empty dataset! This is bad - should not happen!  Setting start time to TODAY minus the initial starttime offset.')
            initial_offset = datasource.initial_starttime_offset_millis
            if initial_offset == 0:
                initial_offset = 86400000 # 1 day in millis
            start_time_seconds = int(time.time() - (initial_offset * 0.001))


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

        msg.start_datetime_millis = start_time_seconds * 1000

        msg.end_datetime_millis = end_time_seconds * 1000

        msg.property.extend(datasource.property)
        msg.station_id.extend(datasource.station_id)

        msg.request_type = datasource.request_type
        msg.request_bounds_north = datasource.request_bounds_north
        msg.request_bounds_south = datasource.request_bounds_south
        msg.request_bounds_west = datasource.request_bounds_west
        msg.request_bounds_east = datasource.request_bounds_east
        msg.base_url = datasource.base_url
        msg.dataset_url = datasource.dataset_url
        msg.ncml_mask = datasource.ncml_mask
        msg.max_ingest_millis = datasource.max_ingest_millis

        # Add subranges to the context message
        for i, r in enumerate(datasource.sub_ranges):
            s = msg.sub_ranges.add()
            s.dim_name    = r.dim_name
            s.start_index = r.start_index
            s.end_index   = r.end_index
        
        if datasource.IsFieldSet('authentication'):
            log.info('Setting: authentication')
            msg.authentication = datasource.authentication

        if datasource.IsFieldSet('search_pattern'):
            log.info('Setting: search_patern')
            msg.search_pattern = datasource.search_pattern
        
        defer.returnValue(msg)

    @defer.inlineCallbacks
    def _get_associated_data_source_id(self, dataset_id):
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

    @property
    def agent_phandle(self):
        '''
        @return: an the defered result of the last call to OSProcess.spawn() via self._spawn_dataset_agent() as a
                 reference to the underlying dataset agent, if self._spawn_dataset_agent has been successfully invoked,
                 otherwise None
        '''
        # Initialization done upon successfull call to self._spawn_agent()
        return self.__agent_phandle

    @property
    def agent_binding(self):
        '''
        @return: a string representing the reply-to binding key used to send messages to the underlying dataset agent
                 if the dataset agent has responded to spawning through callback self.on_binding_key_callback(),
                 otherwise None
        '''
        # Initialization done by Dataset agent through callback, self.on_binding_key_callback() 
        return self.__agent_binding

    @property
    def agent_spawn_args(self):
        '''
        @return: a list of arguments which can be passed to a spawning mechanism (such as OSProcess) to spawn this JavaAgentWrapper's
        underlying Dataset Agent.
        '''
        # Lazy-initialize the spawn arguments
        if (self.__agent_spawn_args == None):
            self._init_agent_spawn_args()
        return self.__agent_spawn_args

    @property
    def agent_update_op(self):
        '''
        @return: the name of the RPC operation used by the Java Dataset Agent for performing a dataset update.
        '''
        # Lazy-initialize the update operation name
        if (self.__agent_updt_op == None):
            self._init_agent_update_op()
        return self.__agent_updt_op

    @property
    def agent_term_op(self):
        '''
        @return: the name of the RPC operation used by the Java Dataset Agent for performing self-termination.
        '''
        # Lazy-initialize the terminate operation name
        if (self.__agent_term_op == None):
            self._init_agent_term_op()
        return self.__agent_term_op

    def _init_agent_spawn_args(self):
        '''
        Lazy-initializes self.__agent_spawn_args
        '''
        # @todo: Generate jar_pathname dynamically
        # jar_pathname = "/Users/tlarocque/Development/Java/Workspace_eclipse/EOI_dev/build/TryAgent.jar"   # STAR #
        jar_pathname = CONF.getValue('dataset_agent_jar_path', None)

        if jar_pathname is None or not os.path.exists(jar_pathname):
            raise IonError("JAR for dataset agent not found: (%s)" % jar_pathname)
        
        parent_host_name = self.container.exchange_manager.message_space.hostname
        parent_xp_name = self.container.exchange_manager.exchange_space.name
        parent_scoped_name = self.get_scoped_name("system", str(self.declare['name']))      # @todo: validate that 'system' is the correct scope
        parent_callback_op = "binding_key_callback"



        # Do not return anything.  Store spawn arguments in __agent_spawn_args
        binary = "java"
        args = ["-jar", jar_pathname, parent_host_name, parent_xp_name, parent_scoped_name, parent_callback_op]
        log.debug("Acquired external process's spawn arguments:  %s %s" % (binary, " ".join(args)))
        self.__agent_spawn_args = (binary, args)
    
    def _init_agent_update_op(self):
        '''
        Lazy-initializes self.__agent_updt_op
        '''
        # @todo: Acquiring the shutdown op may need to be dynamic in the future
        updt_op= "op_update"
        log.debug("Acquired Dataset Agent update op: %s" % (updt_op))
        self.__agent_updt_op = updt_op

    def _init_agent_term_op(self):
        '''
        Lazy-initializes self.__agent_term_op
        '''
        # @todo: Acquiring the shutdown op may need to be dynamic in the future
        term_op= "op_shutdown"
        log.debug("Acquired external process's terminate op: %s" % (term_op))
        self.__agent_term_op = term_op
        

class JavaAgentWrapperClient(ServiceClient):
    """
    Test client for direct (RPC) interaction with the JavaAgentWrapper ServiceProcess
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
        Retrieve the state of the JavaAgentWrapper Service
        '''
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('pretty_print', None)
        
        log.info("<<<---@@@ Incoming rpc reply...")
        log.debug("... Content\t" + str(content))
        log.debug("... Headers\t" + str(headers))
        log.debug("... Message\t" + str(msg))
        
        result = ""
        if 'value' in content:
            result = content['value']
            print "found value"
        else:
            result = str(content)
            print "couldnt get value"
        defer.returnValue(result)
        
    @defer.inlineCallbacks
    def rpc_do_shutdown(self):
        '''
        Retrieve the state of the JavaAgentWrapper Service
        '''
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('do_shutdown', None)
        defer.returnValue(str(content))

    @defer.inlineCallbacks
    def rpc_do_deactivate(self):
        '''
        Retrieve the state of the JavaAgentWrapper Service
        '''
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('do_deactivate', None)
        defer.returnValue(str(content))
    
    @defer.inlineCallbacks
    def request_update(self, datasetID, datasourceID):
        '''
        Simulates an update request to the JavaAgentWrapper as if from the Scheduler Service
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
# Governance Testing
#----------------------------#
#      @todo:



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












