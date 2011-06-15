#!/usr/bin/env python
"""
Created on Apr 5, 2011

@file:   ion/integration/eoi/dispatcher/dispatcher_service.py
@author: Timothy LaRocque
@brief:  Dispatching service for starting external scripts for data assimilation/processing upon changes to availability/content of data

@see:    Dispatcher Diagram - https://docs.google.com/drawings/d/1x1Vhs_VjJ2uN6LZ53QwYmSVZMRTS4mZmguR6-Z7BQRM/edit?hl=en_US&authkey=CIO93YUO
"""

# Imports: logging
import logging
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

# Imports: python-related
import subprocess

# Imports: Core
from twisted.internet import defer
from ion.core.object import object_utils
from ion.core.process.process import ProcessFactory, Process, ProcessClient

# Imports: Messages and events
from ion.services.dm.distribution.publisher_subscriber import SubscriberFactory, PublisherFactory
from ion.services.dm.distribution.events import NewSubscriptionEventPublisher,     NewSubscriptionEventSubscriber, \
                                                DelSubscriptionEventPublisher,     DelSubscriptionEventSubscriber, \
                                                DatasetSupplementAddedEventPublisher, DatasetSupplementAddedEventSubscriber

# Imports: Associations
from ion.core.messaging.message_client import MessageClient
from ion.services.coi.resource_registry.resource_client import ResourceClient
from ion.services.coi.resource_registry.association_client import AssociationClient
from ion.services.dm.inventory.association_service import AssociationServiceClient
from ion.services.coi.identity_registry import IdentityRegistryClient
from ion.services.dm.inventory.association_service import IDREF_TYPE, SUBJECT_PREDICATE_QUERY_TYPE
from ion.services.coi.datastore_bootstrap.ion_preload_config import HAS_A_ID
from ion.core.exception import ReceivedApplicationError, ApplicationError


DISPATCHER_RESOURCE_TYPE = object_utils.create_type_identifier(object_id=7002, version=1)
DISPATCHER_WORKFLOW_RESOURCE_TYPE = object_utils.create_type_identifier(object_id=7003, version=1)
CHANGE_EVENT_MESSAGE = object_utils.create_type_identifier(object_id=7001, version=1)
PREDICATE_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=25, version=1)
RESOURCE_CFG_REQUEST_TYPE = object_utils.create_type_identifier(object_id=10, version=1)
IDENTITY_TYPE = object_utils.create_type_identifier(object_id=1401, version=1)

class DispatcherProcess(Process):
    """
    @brief: Dispatching service for starting external scripts upon reciept of Dataset Change Notifications (according to subscriptions)
    """
    
    def __init__(self, *args, **kwargs):
        """
        @brief: Initializes the DispatcherService class
                Checks for the existance of the dispatcher.id file to procure a OOI Resource ID
                for this service's Dispatcher Resource.  If one does not exist, it is created
        """
        # Step 1: Delegate initialization to parent
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('__init__(): Starting initialization...')
        Process.__init__(self, *args, **kwargs)
        
        self.dues_dict = {}
        self.dues_factory = None
        self.dispatcher_id = None
        self.new_ses = None
        self.del_ses = None

        # Message Client and AssociationServiceClient will be lazy-initialized
        self._mc = None
        self._asc = None
        self._rc = None
        self._ac = None
        self._irc = None
    
        
    @property
    def mc(self):
        """
        @return: The MessageClient
        """
        if self._mc is None:
            self._mc = MessageClient(proc=self)
        return self._mc


    @property
    def rc(self):
        """
        @return: The ResourceClient
        """
        if self._rc is None:
            self._rc = ResourceClient(proc=self)
        return self._rc

    
    @property
    def asc(self):
        """
        @return: The AssociationServiceClient
        """
        if self._asc is None:
            self._asc = AssociationServiceClient(proc=self)
        return self._asc
    
    
    @property
    def ac(self):
        """
        @return: The AssociationClient
        """
        if self._ac is None:
            self._ac = AssociationClient(proc=self)
        return self._ac
    
    
    @property
    def irc(self):
        """
        @return: The IdentityRegistryClient
        """
        if self._irc is None:
            self._irc = IdentityRegistryClient(proc=self)
        return self._irc
    
    
    @defer.inlineCallbacks
    def plc_activate(self):
        """
        @brief: Initializes the Dispatching Service when spawned
        """
        log.info('plc_activate(): LCO (process) initializing...')
        
        # Step 0: Initialize dependencies
        temp_proc=Process()
        yield temp_proc.spawn()
        # Because the process is in the initialize transition create another process from which to do service ops and deal with resources.
        self._rc = ResourceClient(proc=temp_proc)
        self._asc = AssociationServiceClient(proc=temp_proc)
        self._mc = MessageClient(proc=temp_proc)
        self._ac = AssociationClient(proc=temp_proc)
        self._irc = IdentityRegistryClient(proc=temp_proc)


         # TODO: remove the dispatcher.id file and replace it with a search of all the
        # associations for all the users in the dispatcher_users.id file.  If any of the
        # users have an association to a dispatcher then that dispatcher should be
        # this one, and we can get the dispatcher ID from that association.  Also if
        # any of the users have a different dispatcher that should be logged as an error.
        
        # Step 1: Get this dispatcher's ID from the local dispatcher.id file 
        f = None
        id = None
        
        try:
            f = open('dispatcher.id', 'r')
            id = f.read().strip()
            # @todo: ensure this resource exists in the ResourceRepo
        except IOError, ex:
             log.warn('plc_activate(): Dispatcher ID could not be found.  One will be created instead.  Actual Cause: %s' % str(ex))
        finally:
            if f is not None:
                f.close()

        
        # Step 2: If no ID is found, register this dispatcher
        if id is None:
            id = yield self._register_dispatcher('DispatcherResource')
            try:
                log.info('Writing dispatcher_id to local file dispatcher.id')
                f = open('dispatcher.id', 'w')
                f.write(id)
            except Exception, ex:
                log.error('Could not write dispatcher id to local file "dispatcher.id": %s' % ex)
            finally:
                if f is not None:
                    f.close()
            # create all user HAS_A associations for this dispatcher
            yield self._make_user_associations(id)

        
        # Step 3: Store the new ID locally -- later used to create Subscription Subscribers
        self.dispatcher_id = id
        log.info('\n\nplc_activate(): Retrieved dispatcher_id "%s"\n\n' % id)
        

        # Step 4: Generate Subscription Event Subscribers
        # @attention: Do this before creating the UEN subscribers to ensure
        #             that subscription changes are not lost (see design docs)
        #             -- also, while the subscription subscribers are bound to
        #                non-durable topics, we actually don't have to worry about
        #                this use-case, but for future changes this is correct...
        self.new_ses = yield self._create_subscription_subscriber(NewSubscriptionEventSubscriber, lambda *args, **kw: self.create_dataset_update_subscriber(*args, **kw))
        self.del_ses = yield self._create_subscription_subscriber(DelSubscriptionEventSubscriber, lambda *args, **kw: self.delete_dataset_update_subscriber(*args, **kw))

        
        # Step 5: Create all necessary Update Event Notification Subscribers
        yield self._preload_associated_workflows(self.dispatcher_id)

        # Clean up the temporary process used during init:
        yield temp_proc.terminate()
        self._rc = None
        self._mc = None
        self._asc = None
        self._ac = None
        self._irc = None


        log.debug('plc_activate(): ******** COMPLETE ********')
        
    
    @defer.inlineCallbacks
    def _make_user_associations(self, DispatcherID):
        """
        @brief: Utilizes User Definitions in the local file "dispatcher_users.id" to request the OOI Resource ID
                of each user and create a HAS_A association between it and the given DispatcherID
        
        @param DispatcherID: The OOI Resource ID of the Dispatcher Resource linked to this Dispatcher Process
        """
        # TODO:  Make this routine more intelligent by checking each user's associations for an already existing
        # dispatcher, and only creating a dispatcher association for users w/o a dispatcher
        
        # get dispatcher resource for making associations
        DispatcherRes = yield self.rc.get_instance(DispatcherID)
        
        # open and read users file
        f = None
        try:
            f = open('dispatcher_users.id', 'r')
            Users = f.readlines()
        except IOError, ex:
            log.warn("_make_user_associations: Dispatcher's users file could not be read, no associations will be made.  Cause: %s" % str(ex))
            defer.returnValue(None)
        finally:
            if f is not None:
                f.close()
        
        IdentityRequest = yield self.mc.create_instance(RESOURCE_CFG_REQUEST_TYPE, MessageName='IR request')
        IdentityRequest.configuration = IdentityRequest.CreateObject(IDENTITY_TYPE)
        for User in Users:
            User = User.strip(" \n\r")
            log.info('_make_user_associations: making association for user <%s>' % User)
            
            # get User's resource ID
            IdentityRequest.configuration.subject = User
            try:
                UserID = yield self.irc.get_ooiid_for_user(IdentityRequest)
            except ReceivedApplicationError, ex:
                log.warn('_make_user_associations: can not get ID for user "%s".  Cause: %s' % (User, str(ex)))
                continue
            if log.getEffectiveLevel() <= logging.INFO:
                log.info('_make_user_associations: got id %s for user %s'%(UserID.resource_reference.ooi_id, User))
            
            # get User instance
            try:
                UserRes = yield self.rc.get_instance(UserID.resource_reference.ooi_id)
            except ApplicationError, ex:
                log.warn('_make_user_associations: can not get instance for UserID "%s".  Cause: %s' % (UserID.resource_reference.ooi_id, str(ex)))
            
            # Create an association between the user and the dispatcher
            try:
                association = yield self.ac.create_association(UserRes, HAS_A_ID, DispatcherRes)
                
                # Put the association in datastore
                log.debug('_make_user_associations: Storing association: ' + str(association))
                yield self.rc.put_instance(association)
            except ApplicationError, ex:
                errString = 'Error creating assocation between UserRes: ' + UserID.resource_reference.ooi_id + ' and DispatcherID: ' + DispatcherID + '. ex: ' + str(ex)
                log.error(errString)
 

    @defer.inlineCallbacks
    def _register_dispatcher(self, name):
        """
        @brief: Registers this dispatcher process with ION simply by creating a OOI Dispatcher Resource object and
                committing that resource in the Resource Repository.
        @note:  This method is called during plc_activate of this service
        @note:  The Initialization process (plc_activate) is responsible for locally persisting the returned ID in
                the event that the Dispatcher Process needs to be restarted.

        @param name: An arbitrary name to assign to the Dispatcher Resource Instance
        
        @return: The OOI Resource ID of the newly created Dispatcher Resource
        """
        # Explicitly create a resource client with an anonymous process so that we can push during plc init of the dispatcher process!
        rc = self.rc
        disp_res = yield rc.create_instance(DISPATCHER_RESOURCE_TYPE, ResourceName=name)
        disp_res.dispatcher_name = name
        yield rc.put_instance(disp_res, 'Commiting new dispatcher resource for registration')

        res_id = disp_res.ResourceIdentity

        defer.returnValue(str(res_id))

    
    @defer.inlineCallbacks
    def _preload_associated_workflows(self, dispatcher_id):
        """
        @brief: Loads all DispatcherWorkflowResources (GPB{7003}) which are associated with the given dispatcher_id
                and creates an Update Event Subscriber for each.  The Update Event Subscriber will later invoke the
                workflow in the DispatcherWorkflowResource whenever it is notified of an update to associated dataset.
        
        @param dispatcher_id: The OOI Resource ID for a dispatcher resource
        """
        # Step 1: Request all associated Dispatcher Workflows
        request = yield self.mc.create_instance(SUBJECT_PREDICATE_QUERY_TYPE)
        pair = request.pairs.add()
        
        # Create the predicate and subject for the query
        predicate = request.CreateObject(PREDICATE_REFERENCE_TYPE)
        subject   = request.CreateObject(IDREF_TYPE) 
        predicate.key = HAS_A_ID
        subject.key   = dispatcher_id
        
        pair.subject   = subject
        pair.predicate = predicate
        
        # Make the request
        # @attention: In future implementations, get_objects will allow us to specify
        #             that we only want to return DispatcherWorkflowResource objects
        log.info('Loading all associated DispatcherWorkflowResource objects...')
        associations = yield self.asc.get_objects(request)

        
        # Step 2: Create a Dataset Update Subscriber for each associated workflow
        if associations is None or associations.idrefs is None:
            log.info('No prior Dispatcher Workflow Resources associated with this Dispatcher; this is ok')
        else:
            for idref in associations.idrefs:
                try:
                    id = idref.key
                    res = yield self.rc.get_instance(id)
                    yield self.create_dataset_update_subscriber(res.dataset_id, res.workflow_path)
                except Exception, ex:
                    log.error('Error retrieving associated resource "%s".  Dispatcher may not be subscribed to all dataset updates as expected:  %s' % (str(id), str(ex)))

    
    @defer.inlineCallbacks
    def _create_subscription_subscriber(self, subscriber_type, callback):
        """
        @brief: Creates a SubscriptionChangeEventsSubscriber which listens to notifications of changes to this Dispatcher's
                subscriptions (NEW and DELETE).  When subscription change notifications occur, this subscriber will call back
                and either delete or create a new Dataset Update Subscriber (used for listening to changes to datasets)
        
        @param subscriber_type: The type of subscriber to create.  Either a NewSubscriptionEventSubscriber or
                                DelSubscriptionEventSubscriber
        @param callback: The function to callback to when the subscriber created here receives notification of subscription
                         change
        
        @return: A deferred containing the newly created subscriber object
        """
        if log.getEffectiveLevel() <= logging.INFO:
            log.info('_create_subscription_subscriber(): Creating a Subscription Change Events Subscriber as "%s"' % subscriber_type.__name__)
        
        # Step 1: Generate the subscriber
        log.debug('_create_subscription_subscriber(): Building Subscriber from a SubscriberFactory')
        factory = SubscriberFactory(subscriber_type=subscriber_type, process=self)
        subscriber = yield factory.build(origin=self.dispatcher_id)
        
        # Step 2: Monkey Patch a callback into the subscriber
        log.debug('_create_subscription_subscriber(): Monkey patching ondata callback to subscriber')
        def cb(data):
            log.info('cb(): <<<---@@@ Subscription Change Subscriber received data')
            subscription = DispatcherProcess.unpack_subscription_data(data)
            if log.getEffectiveLevel() <= logging.INFO:
                log.info('cb(): Invoking subscription event callback using dataset_id "%s" and script "%s"' % subscription)
            return callback(*subscription)
        subscriber.ondata = cb
        
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('_create_subscription_subscriber(): Subscription Subscriber bound to topic "%s"' % subscriber.topic(self.dispatcher_id))
        defer.returnValue(subscriber)
        
    
    @staticmethod
    def unpack_subscription_data(data):
        """
        @brief: Unpacks the subscription change event from message content in the given
                dictionary and retrieves the dataset_id and workflow_path fields.
        @param data: An instance of an EventMessage (GPB{2322}) resource containing subscription data
        
        @return: The subscription data as a tuple.
        """
        # Dig through the message wrappers...
        content = data and data.get('content', None)
        sub_evt = content and content.additional_data
        dwf_res = sub_evt and sub_evt.dispatcher_workflow
        
        # Keep digging...
        dataset_id  = dwf_res and str(dwf_res.dataset_id)
        script_path = dwf_res and str(dwf_res.workflow_path)
        return (dataset_id, script_path)

    
    @staticmethod
    def unpack_dataset_update_data(data):
        """
        @brief: Unpacks the Dataset Update Event from message content in the given
                dictionary and retrieves the dataset_id and data_source_id fields.
        @param data: An instance of an EventMessage (GPB{2322}) resource containing subscription data
                
        @return: This dataset's update data returned as a tuple.
        """
        # Dig through the message wrappers...
        content = data and data.get('content', None)
        res_evt = content and content.additional_data
        chg_evt = res_evt and chg_evt.resource
        
        # Keep digging...
        dataset_id     = chg_evt and str(chg_evt.dataset_id)
        data_source_id = chg_evt and str(chg_evt.data_source_id)
        return (dataset_id, data_source_id)
    
    
    @defer.inlineCallbacks
    def create_dataset_update_subscriber(self, dataset_id, script_path):
        """
        @brief: Creates a Dataset Update Subscriber.
                A dataset update subscriber listens for update event notifications which are triggered when
                a dataset has changed.  When this occurs, the subscriber kicks-off the given script via the
                subprocess module
        @param dataset_id: The OOI Resource ID for a dataset resource
        @param script_path: The pathname of a local script to be run upon notification of dataset changes
        
        @return: Nothing - see the following note...
        
        @note:   The subscriber here will be stored in the Dispatcher Process's dictionary of
                 Dataset Update Event Subscribers (self.dues_dict) and is mapped to a key which is a tuple
                 in the form of: (dataset_id, script_path).  This key is later used to remove the subscriber
                 if/when the Delete Subscription Events Subscriber receives a notification to remove this
                 item from the dictionary. 
        """
        if log.getEffectiveLevel() <= logging.INFO:
            log.info('')
            log.info('create_dataset_update_subscriber(): Creating Dataset Update Event Subscriber (DUES) for ID "%s" and script "%s' % (str(dataset_id), str(script_path)))
        
        key = (dataset_id, script_path)
        
        
        # Step 1: If the dictionary has this key dispose of the corresponding subscriber first
        subscriber = self.dues_dict.has_key(key) and self.dues_dict.pop(key)
        if subscriber:
            log.warn('create_dataset_update_subscriber(): Dataset Update Subscriber already exists for key: %s.  An additional subscriber will not be created' % str(key))
            defer.returnValue(None)
        
        # Step 2: Create the new subscriber
        log.debug('create_dataset_update_subscriber(): Creating new Dataset Update Events Subscriber via SubscriberFactory')
        if self.dues_factory is None:
            self.dues_factory = SubscriberFactory(subscriber_type=DatasetSupplementAddedEventSubscriber, process=self)
        subscriber = yield self.dues_factory.build(origin=dataset_id)
        log.debug('create_dataset_update_subscriber(): Bound subscriber to topic: "%s"' % subscriber.topic(dataset_id))
        
        
        # Step 3: Monkey patch a callback into subscriber
        log.debug('create_dataset_update_subscriber(): Monkey patching callback to subscriber')
        subscriber.ondata = lambda data: self.run_script(data, script_path, dataset_id)
        
        # Step 4: Add this subscriber to the dictionary of dataset update event subscribers
        self.dues_dict[key] = subscriber
        log.debug('create_dataset_update_subscriber(): Create subscriber complete!')
        
    
    def delete_dataset_update_subscriber(self, dataset_id, script_path):
        """
        @brief: Removes the dataset update event subscriber from the DUES dict which is keyed off the
                same dataset_id and script_path
        """
        if log.getEffectiveLevel() <= logging.INFO:
            log.info('delete_dataset_update_subscriber(): Unregistering Dataset Update Event Subscriber for ID "%s" and script "%s"...' % (str(dataset_id), str(script_path)))
        key = (dataset_id, script_path)
        
        subscriber = self.dues_dict.has_key(key) and self.dues_dict.pop(key)
        if subscriber:
            log.info('delete_dataset_update_subscriber(): ...Removing old subscriber for key: %s' % str(key))
            self._delete_subscriber_resource(subscriber)
        else:
            log.warn('delete_dataset_update_subscriber(): ...Subscriber does not exist for key: %s.  Dataset Update Subscriber will not be deleted.' % str(key))
        
    
    def _delete_subscriber_resource(self, subscriber):
        """
        @brief: Terminates the given subscriber
        """
        if log.getEffectiveLevel() <= logging.DEBUG:
            log.debug('_delete_subscriber_resource(): Deleting subscriber %s' % str(subscriber))
        subscriber.terminate()
        
    
    def run_script(self, data, script_path, dataset_id):
        """
        @brief: Reads and Runs the given script passing it the given dataset_id
        @note:  (data is currently unused) 
        """
        if log.getEffectiveLevel() <= logging.INFO:
            log.info('run_script(): Running script "%s" for dataset "%s"' % (script_path, dataset_id))
        
        # Use subprocess to run the script
        try:
            proc = subprocess.Popen([script_path, dataset_id])
        except Exception, ex:
            log.error("Could not start workflow for script '%s'.  Cause: %s" % (str(script_path), str(ex)))
            # @todo: Publish failure notification to the email service
            #        -- nothing will be listening but do it anyway
    

    
# ===================================================================================================================


    
class DispatcherProcessClient(ProcessClient):
    """
    This is an example client which calls the DispatcherService.  It's
    intent is to notify the dispatcher of changes to data sources so
    it can make requests to start various data processing/modeling scripts.
    This test client effectually simulates notifications as if by the Scheduler Service.
    """
    
    
    def __init__(self, *args, **kwargs):
        """
        """
        ProcessClient.__init__(self, *args, **kwargs)
        self.rc = ResourceClient(proc=self.proc)
        self.mc = MessageClient(proc=self.proc)
        self.ac = AssociationClient(proc=self.proc)
        self.dispatcher_resource = None
    
    
    @defer.inlineCallbacks
    def test_newsub(self, dispatcher_id, dataset_id='abcd-1234', script='./dispatcher_script'):
        yield self._check_init()

        # Step 1: Create the publisher
        #-----------------------------
        factory = PublisherFactory(publisher_type=NewSubscriptionEventPublisher, process=self.proc)
        publisher = yield factory.build(origin=dispatcher_id)
        log.debug('test_newsub(): Created publisher; bound to topic "%s" for publishing new subscription notifications' % publisher.topic(dispatcher_id))
        
        
        # Step 2: Create the dispatcher workflow resource
        #------------------------------------------------
        dwr = yield self.rc.create_instance(DISPATCHER_WORKFLOW_RESOURCE_TYPE, ResourceName='DWR1', ResourceDescription='Dispatcher Workflow Resource')
        dwr.dataset_id = dataset_id
        dwr.workflow_path = script
        log.debug('test_newsub(): Created the DispatcherWorkflowResource')
        
        yield self.rc.put_instance(dwr)
        log.debug('test_newsub(): Commited the DispatcherWorkflowResource')
        
                
        # Step 3: Associate the workflow with the dispatcher_resource
        #------------------------------------------------------------
        if self.dispatcher_resource is None:
            # lazy-initialize the dispatcher_resource
            try:
                self.dispatcher_resource = yield self.rc.get_instance(dispatcher_id)
            except Exception, ex:
                log.error('Resource object for dispatcher_id "%s" is not present:  %s' % (str(dispatcher_id), str(ex)))
                defer.returnValue(None)
        # Make sure the association doesn't already exist..
        assoc = yield self.ac.find_associations(self.dispatcher_resource, HAS_A_ID, dwr)
        if assoc is not None and len(assoc) > 0:
           log.warn("Association already exists! -- The dispatcher should already own this subscription!")
           defer.returnValue(None) 
        
        # Otherwise create the association
        assoc = yield self.ac.create_association(self.dispatcher_resource, HAS_A_ID, dwr)
        log.debug('test_newsub(): Created "HAS_A" association between the Dispatcher and its Workflow')
        
        yield self.rc.put_instance(assoc)
        log.debug('test_newsub(): Commited "HAS_A" association between the Dispatcher and its Workflow')
        
        
        # Step 4: Publish the new subscription notification
        #--------------------------------------------------
        log.info('test_newsub(): @@@--->>> Publishing New Subscription event on topic "%s"' % publisher.topic(dispatcher_id))
        yield publisher.create_and_publish_event(dispatcher_workflow=dwr.ResourceObject)
        
        log.debug('test_newsub(): Publish test complete!')


    @defer.inlineCallbacks
    def test_delsub(self, dispatcher_id, dataset_id='abcd-1234', script='./dispatcher_script'):
        yield self._check_init()

        # Step 1: Create the publisher
        #-----------------------------
        factory = PublisherFactory(publisher_type=DelSubscriptionEventPublisher, process=self.proc)
        publisher = yield factory.build(routing_key=self.target, origin=dispatcher_id)
        log.debug('test_delsub(): Created publisher; bound to topic "%s" for publishing del subscription notifications' % publisher.topic(dispatcher_id))
        
        
        # Step 3: Disassociate the workflow and the dispatcher_resource
        #--------------------------------------------------------------
        # @todo: Find a more efficient way to do this.. might not be a way for R1!!
        # Grab association ID
        if self.dispatcher_resource is None:
            # lazy-initialize the dispatcher_resource
            try:
                self.dispatcher_resource = yield self.rc.get_instance(dispatcher_id)
            except Exception, ex:
                log.error('Resource object for dispatcher_id "%s" is not present:  %s' % (str(dispatcher_id), str(ex)))
                defer.returnValue(None)
        # @attention: The following line assumes all HAS_A associations with the dispatcher resource
        #             are to DispatcherWorkflowResource objects -- if this changes we need type checking
        associations = yield self.ac.find_associations(self.dispatcher_resource, HAS_A_ID)
        
        # Extract DispatcherWorkflowResource IDs
        dwr_ids = [association.ObjectReference for association in associations]
        
        # Grab the DispatcherWorkflowResources to find which one(s) match
        # -- remote the dwr ID from the list if it does not match
        # @todo: There is another way to copy list..  find out if it is more efficient
        dwr = None
        for dwr_id in list(dwr_ids):
            dwr_temp = yield self.rc.get_instance(dwr_id)
            if hasattr(dwr_temp, 'dataset_id') and hasattr(dwr_temp, 'workflow_path'):
                if dwr_temp.dataset_id == dataset_id and dwr_temp.workflow_path == script:
                    dwr = dwr_temp
                    break
                else:
                    dwr_ids.remove(dwr_id)
        log.debug("test_delsub(): Retrieved all associations for this subscriber's DispatcherWorkflowResource. Count = %i" % len(dwr_ids))
        
        # Set the association to NULL & commit changes
        assert(len(associations) < 2, 'There should only be 0 or 1 HAS_A associations between this DispatcherResource and corresponding DispatcherWorkflowResource')
        for association in associations:
            if association.ObjectReference in dwr_ids:
                dwr_ids.remove(association.ObjectReference) # makes iteration faster
                association.SetNull()
                self.rc.put_instance(association)
        log.debug('test_delsub(): Nullified all associations for this subscriber')
        
        
        # Step 4: Publish the delete subscription notification
        #-----------------------------------------------------
        if dwr is not None:
            log.info('test_newsub(): @@@--->>> Publishing Del Subscription event on topic "%s"' % publisher.topic(dispatcher_id))
            yield publisher.create_and_publish_event(dispatcher_workflow=dwr.ResourceObject)
        else:
            log.warn('test_newsub(): Cannot delete subscription -- no associations exist between the DispatcherWorkflowResource with the given arguments and this DispatcherResource')
        log.debug('test_delsub(): Publish test complete!')
        
        
    @defer.inlineCallbacks
    def test_update_dataset(self, dataset_id='abcd-1234'):
        yield self._check_init()

        # Step 1: Create the publisher
        factory = PublisherFactory(publisher_type=DatasetSupplementAddedEventPublisher, process=self.proc)
        publisher = yield factory.build(routing_key=self.target, origin=dataset_id)
        log.debug('test_update_dataset(): Created publisher; bound to topic "%s" for publishing' % publisher.topic(dataset_id))
        
        # Step 2: Create the dispatcher script resource
        chg_evt = yield self.rc.create_instance(CHANGE_EVENT_MESSAGE, ResourceName='ChangeEvent1', ResourceDescription='Dataset Change Event Message')
        chg_evt.dataset_id = dataset_id
        # chg_evt.data_source_id = 'no-data-source'
        log.debug('test_update_dataset(): Created the ChangeEventMessage')
        
        # Step 3: Send the dispatcher script resource
        log.info('test_update_dataset(): @@@--->>> Publishing Dataset Change Event on topic "%s"' % publisher.topic(dataset_id))
        yield publisher.create_and_publish_event(resource=chg_evt.ResourceObject)
        log.debug('test_update_dataset(): Publish test complete!')
    

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
client.test_newsub('AA23CC4D-9CDD-4086-8D79-B62B596AE3FA')

client.test_update_dataset()
"""
