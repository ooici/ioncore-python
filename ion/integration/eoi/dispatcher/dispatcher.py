#!/usr/bin/env python
"""
Created on Apr 5, 2011

@file:   ion/integration/eoi/dispatcher/workflow_dispatcher_service.py
@author: Timothy LaRocque
@brief:  Dispatching service for starting remote workflows (external scripts) for data assimilation/processing upon changes to availability of data
"""

# Imports: logging
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

# Imports: python-related
import subprocess
import uuid

# Imports: Core
from twisted.internet import defer
from ion.core.object import object_utils
from ion.core.process.process import ProcessFactory, Process, ProcessClient
#from ion.core.messaging.ion_reply_codes import ResponseCodes as RC

# Imports: Messages and events
from ion.services.dm.distribution.publisher_subscriber import SubscriberFactory, PublisherFactory
from ion.services.dm.distribution.events import NewSubscriptionEventPublisher, NewSubscriptionEventSubscriber, \
                                                                       DelSubscriptionEventPublisher, DelSubscriptionEventSubscriber
from ion.services.coi.resource_registry_beta.resource_client import ResourceClient

DISPATCHER_WORKFLOW_RESOURCE_TYPE = object_utils.create_type_identifier(object_id=7003, version=1)


class DispatcherProcess(Process):
    """
    Dispatching service for starting remote workflows (scripts)
    """

    
    def __init__(self, *args, **kwargs):
        """
        Initializes the DispatcherService class
        Checks for the existance of the workflow_dispatcher.id file to procure a system ID for
        this service's Dispatcher Resource.  If one does not exist, it is created
        """
        # Step 1: Delegate initialization to parent
        log.debug('__init__(): Starting initialization...')
        Process.__init__(self, *args, **kwargs)
        
        # Step 2: Check for the workflow_dispatcher.id file, or create it
        f = None
        id = None
        try:
            f = open('dispatcher.id', 'r')
            id = f.read().strip()
        except IOError:
             log.warn('__init__(): Dispatcher ID could not be found.  One will be created instead')
        finally:
            if f is not None:
                f.close()
        
        if id is None:
            id = str(uuid.uuid1())
            try:
                log.info('Writing dispatcher_id to local file dispatcher.id')
                f = open('dispatcher.id', 'w')
                f.write(id)
            except Exception, ex:
                log.error('Could not write dispatcher id to local file "dispatcher.id": %s' % ex)
            finally:
                if f is not None:
                    f.close()
        
        # Step 3: Store the new ID locally
        self.dispatcher_id = id
        log.info('\n\n\n__init__(): Retrieved dispatcher_id "%s"\n\n\n' % id)
        
    @defer.inlineCallbacks
    def plc_init(self):
        """
        Initializes the Dispatching Service when spawned
        (Yields ALLOWED)
        """
        yield
        log.info('plc_init(): LCO (process) initializing...')
        
        # Step 1: Obtain DispatcherResource using self.dispatcher_id
        # @attention: is the DispatcherResource needed for anything??
        
        # Step 2: Generate Subscription Event Subscribers
        self.new_ses = yield self._generate_subscription_subscriber(NewSubscriptionEventSubscriber)
#        yield self._gen_del_sub_event_subscriber()
        
        # Step 3: Create all necessary Update Event Notification Subscribers
#        yield self.

        log.debug('plc_init(): ******** COMPLETE3 ********')
    
    
    @defer.inlineCallbacks
    def _generate_subscription_subscriber(self, subscriber_type):
        log.debug('_generate_subscription_subscriber(): Generating a Subscription Events Subscriber (SES)')
        
        # Step 1: Generate the subscriber
        factory = SubscriberFactory(subscriber_type=subscriber_type, process=self)
        subscriber = yield factory.build(origin=self.dispatcher_id)
        
        # Step 2: Monkey Patch a callback into the subscriber
        def cb(data):
            log.info('<<<---@@@ Subscriber recieved data callback')
            log.info('Creating Update Event Subscriber for dataset_id "%s" and workflow "%s"' % DispatcherProcess.unpack_subscription_content(data))
        subscriber.ondata = cb
        
        log.debug('_generate_subscription_subscriber(): SES bound to topic "%s"' % subscriber.topic(self.dispatcher_id))
        defer.returnValue(subscriber)
        
    
    @staticmethod
    def unpack_subscription_content(data):
        content = data and data.get('content', None)
        chg_evt = content and content.additional_data
        dwf_res = chg_evt and chg_evt.dispatcher_workflow
        
        dataset_id    = dwf_res and str(dwf_res.dataset_id)
        workflow_path = dwf_res and str(dwf_res.workflow_path)
        return (dataset_id, workflow_path)
    
    
    @defer.inlineCallbacks
    def op_test(self, content, headers, msg):
        log.info('op_test(): <<<---@@@ Incoming call to op')
        
        log.info('op_test(): @@@--->>> Sending reply_ok')
        yield self.reply_ok(msg, 'testing, testing, 1.. 2.. 3..')
    
    
#    @defer.inlineCallbacks
#    def op_notify(self, content, headers, msg):
#        """
#        Receives a notification of change to a dataset and launches the associated workflow.
#        Content should be provided as a dictionary with keys {'dataset_id', 'dataset_name', 'workflow'}
#        @see _unpack_notification(content)
#        """
#        # Step 1: Retrieve workflow, dataset ID and name from the message content
#        try:
#            (datasetId, datasetName, workflow) = self._unpack_notification(content)
#        except (TypeError, KeyError), ex:
#            reply = "Invalid notify content: %s" % (str(ex))
#            yield self.reply_uncaught_err(msg, content=reply, response_code=reply)
#            defer.returnValue(None)
#        # Step 2: Build the subprocess arguments to start the workflow
#        args = self._prepare_workflow(datasetId, datasetName, workflow)
#        # Step 3: Start the workflow with the subprocess module
#        try:
##            proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
#            proc = subprocess.Popen(args)
#        except Exception, ex:
#            reply = "Could not start workflow: %s" % (str(ex))
#            (content, headers, msg) = yield self.reply_uncaught_err(msg, content=reply, response_code=reply)
#            defer.returnValue(content)
#        returncode = proc.wait()
#        if returncode == 0:
#            yield self.reply(msg, content="Workflow completed with SUCCESS")
#        else:
#            msgout = proc.communicate()[0]
#            yield self.reply(msg, content="Error on notify.  Retrieved return code: '%s'.  Retrieved message: %s" % (str(returncode), str(msgout)))
#    
#    def _unpack_notification(self, content):
#        """
#        Extracts pertinent data from the given content and returns that data as a 3-tuple.
#        Content should be provided as a dictionary with keys {'dataset_id', 'dataset_name', 'workflow'}
#        @see _unpack_notification(content)
#        """
#        if (type(content) is not dict):
#            raise TypeError("Content must be a dictionary with keys 'workflow' and 'dataset_id'")
#        if ('dataset_id' not in content):
#            raise KeyError("Content dict must contain an entry for 'dataset_id'")
#        if ('dataset_name' not in content):
#            raise KeyError("Content dict must contain an entry for 'dataset_name'")
#        if ('workflow' not in content):
#            raise KeyError("Content dict must contain an entry for 'workflow'")
#        return (content['dataset_id'], content['dataset_name'], content['workflow'])
#            
#            
#    def _prepare_workflow(self, datasetId, datasetName, workflow):
#        '''
#        Generates a list of arguments from the given parameters to start an external workflow.
#        ---Currently just drops the given arguments into a list as they are.---
#        '''
#        #@todo: This unsafe as it permits injection attacks.  Validate inputs here.
#        return [workflow, datasetId, datasetName]
        
class DispatcherProcessClient(ProcessClient):
    """
    This is an example client which calls the DispatcherService.  It's
    intent is to notify the dispatcher of changes to data sources so
    it can make requests to start various data processing/modeling scripts.
    This test client effectually simulates notifications as if by the Scheduler Service.
    """
    
#    DEFAULT_GET_SCRIPT = 'res/apps/eoi_dispatcher/get_ooi_dataset.sh'
#    _next_id = 0
    
    def __init__(self, *args, **kwargs):
        """
        """
        ProcessClient.__init__(self, *args, **kwargs)
        
        self.rc = ResourceClient(proc=self.proc)
    
    @defer.inlineCallbacks
    def test(self):
        yield self._check_init()
        log.info('test() @@@--->>> Sending rpc call to op_test')
        (content, headers, msg) = yield self.rpc_send('test', "")
        log.info('test() <<<---@@@ Recieved response')
        log.debug(str(content))
        defer.returnValue(str(content))
    
    @defer.inlineCallbacks
    def test_newsub(self, dispatcher_id, dataset_id='abcd-1234', workflow='/dispatcher/workflow/script'):
        yield self._check_init()

        # Step 1: Create the publisher
        factory = PublisherFactory(publisher_type=NewSubscriptionEventPublisher, process=self.proc)
        publisher = yield factory.build(routing_key=self.target, origin=dispatcher_id)
        log.debug('test_newsub(): Created publisher; bound to topic "%s" for publishing' % publisher.topic(dispatcher_id))
        
        # Step 2: Create the dispatcher workflow resource
        dwr = yield self.rc.create_instance(DISPATCHER_WORKFLOW_RESOURCE_TYPE, ResourceName='DWR1', ResourceDescription='Dispatcher Workflow Resource')
        dwr.dataset_id = dataset_id
        dwr.workflow_path = workflow
        log.debug('test_newsub(): Created the DispatcherWorkflowResource')
        
        # Step 3: Send the dispatcher workflow resource
        log.info('test_newsub(): @@@--->>> Publishing New Subscription event on topic "%s"' % publisher.topic(dispatcher_id))
#        yield publisher.create_and_publish_event(dispatcher_workflow=dwr.ResourceObject)
        yield publisher.create_and_publish_event(dispatcher_workflow=dwr.ResourceObject)
        log.debug('test_newsub(): Publish test complete!')

    @defer.inlineCallbacks
    def test_delsub(self, dispatcher_id, dataset_id='abcd-1234', workflow='/dispatcher/workflow/script'):
        yield self._check_init()

        # Step 1: Create the publisher
        factory = PublisherFactory(publisher_type=DelSubscriptionEventPublisher, process=self.proc)
        publisher = yield factory.build(routing_key=self.target, origin=dispatcher_id)
        log.debug('test_delsub(): Created publisher; bound to topic "%s" for publishing' % publisher.topic(dispatcher_id))
        
        # Step 2: Create the dispatcher workflow resource
        dwr = yield self.rc.create_instance(DISPATCHER_WORKFLOW_RESOURCE_TYPE, ResourceName='DWR1', ResourceDescription='Dispatcher Workflow Resource')
        dwr.dataset_id = dataset_id
        dwr.workflow_path = workflow
        log.debug('test_delsub(): Created the DispatcherWorkflowResource')
        
        # Step 3: Send the dispatcher workflow resource
        log.info('test_newsub(): @@@--->>> Publishing Del Subscription event on topic "%s"' % publisher.topic(dispatcher_id))
#        yield publisher.create_and_publish_event(dispatcher_workflow=dwr.ResourceObject)
        yield publisher.create_and_publish_event(dispatcher_workflow=dwr.ResourceObject)
        log.debug('test_delsub(): Publish test complete!')
        
        
        
#    @classmethod
#    def next_id(cls):
#        cls._next_id += 1
#        return str(cls._next_id)
#    
#    @defer.inlineCallbacks
#    def rpc_notify(self, datasetId, datasetName=None, workflow=DEFAULT_GET_SCRIPT):
#        '''
#        Dispatches a change notification so that the dispatcher can launch the appropriate
#        workflow using the given dataset_id, dataset_name, and workflow filepath.
#        '''
#        yield self._check_init()
#        if (datasetName == None):
#            datasetName = 'example_dataset_' + DispatcherServiceClient.next_id()
#        (content, headers, msg) = yield self.rpc_send('notify', {"dataset_id":datasetId, "dataset_name": datasetName, "workflow":workflow})
#        log.info("<<<---@@@ Incoming RPC reply...")
#        log.debug("\n\n\n...Content:\t" + str(content))
#        log.debug("...Headers\t" + str(headers))
#        log.debug("...Message\t" + str(msg) + "\n\n\n")
#        
#        if (headers[RC.MSG_STATUS] == RC.ION_OK):
#            defer.returnValue("Notify invokation completed with status OK.  Result: %s" % (str(content)))
#        else:
#            defer.returnValue("Notify invokation completed with status %s.  Response code: %s" % (str(headers[RC.MSG_STATUS]), str(headers[RC.MSG_RESPONSE])))

# Spawn of the process using the module name
factory = ProcessFactory(DispatcherProcess)




"""
#---------------------#
# Copy/paste startup:
#---------------------#
#
#  :Test subscription modification
#
desc = {'name':'dispatcher1',
        'module':'ion.integration.eoi.dispatcher.dispatcher',
        'class':'DispatcherProcess'}
from ion.core.process.process import ProcessDesc
proc = ProcessDesc(**desc)
pid = proc.spawn()


from ion.integration.eoi.dispatcher.dispatcher import DispatcherProcessClient as c
client = c(sup, str(pid.result))
d = client.test_newsub('c1deca54-608f-11e0-8457-c8bcc89d9f0a')
"""
