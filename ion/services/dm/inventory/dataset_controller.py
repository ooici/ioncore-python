#!/usr/bin/env python

"""
@file ion/services/dm/inventory/dataset_controller.py
@author David Stuebe
@brief An example service definition that can be used as template for resource management.
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.services.dm.inventory.ncml_generator import create_ncml, rsync_ncml, rsa_to_dot_ssh
from ion.core import ioninit

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.exception import ApplicationError
from ion.services.coi.resource_registry.resource_client import ResourceClient
from ion.core.object import object_utils
from ion.services.dm.inventory.association_service import PREDICATE_OBJECT_QUERY_TYPE
from ion.services.dm.inventory.association_service import AssociationServiceClient
from ion.services.coi.datastore_bootstrap.ion_preload_config import TYPE_OF_ID, \
    HAS_LIFE_CYCLE_STATE_ID, OWNED_BY_ID, DATASET_RESOURCE_TYPE_ID, ANONYMOUS_USER_ID

CMD_DATASET_RESOURCE_TYPE = object_utils.create_type_identifier(object_id=10001, version=1)
"""
message Dataset {
   enum _MessageTypeIdentifier {
        _ID = 10001;
        _VERSION = 1;
    }
   optional net.ooici.core.link.CASRef root_group = 1;
}
"""

CMD_GROUP_TYPE = object_utils.create_type_identifier(object_id=10020, version=1)


IDREF_TYPE = object_utils.create_type_identifier(object_id=4, version=1)
"""
message IDRef {
    enum _MessageTypeIdentifier {
        _ID = 4;
        _VERSION = 1;
    }
	required string key = 1;
	optional string branch = 3;
	optional bytes commit = 4;
}
"""

FINDDATASETREQUEST_TYPE = object_utils.create_type_identifier(object_id=2401, version=1)
"""
message FindDatasetMessage {
    enum _MessageTypeIdentifier {
		_ID = 2401;
		_VERSION = 1;
	}

    optional bool only_mine = 1 ;
    optional net.ooici.services.coi.LifeCycleState by_life_cycle_state = 2 [default = ACTIVE];
    }
"""

QUERYRESULTS_TYPE = object_utils.create_type_identifier(object_id=22, version=1)
"""
message QueryResult{
    enum _MessageTypeIdentifier {
      _ID = 22;
      _VERSION = 1;
    }
    repeated net.ooici.core.link.CASRef idrefs = 1;
}
"""


PREDICATE_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=25, version=1)
LCS_REFERENCE_TYPE = object_utils.create_type_identifier(object_id=26, version=1)


CONF = ioninit.config(__name__)
NCML_PATH = CONF['ncml_path']
THREDDS_NCML_URL = CONF['thredds_ncml_url']

class DatasetControllerError(ApplicationError):
    """
    An exception class for the Dataset Controller Service
    """


class DatasetController(ServiceProcess):
    """
    The Dataset Controller service

    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='dataset_controller',
                                             version='0.1.0',
                                             dependencies=['scheduler'])

    def slc_init(self):
        # Service life cycle state. Initialize service here. Can use yields.

        # Can be called in __init__ or in slc_init... no yield required
        self.resource_client = ResourceClient(proc=self)

        self.asc = AssociationServiceClient(proc=self)

        # As per DS, pull config from spawn args first and config file(s) second
        self.rsa_key = self.spawn_args.get('rsa_key', CONF.getValue('rsa_key', default=None))
        self.server_url = self.spawn_args.get('thredds_ncml_url',
                                              CONF.getValue('thredds_ncml_url',
                                              default='thredds.oceanobservatories.org:/opt/tomcat/ooici_tds_data'))
        self.update_interval = self.spawn_args.get('update_interval', CONF.getValue('update_interval', default=5.0))
        log.debug('rsa key: %s Update: %f URL: %s' % (self.rsa_key, self.update_interval, self.server_url))

        if self.rsa_key:
            rsa_to_dot_ssh(self.rsa_key)
        log.debug('Creating new message receiver for scheduler')

        log.debug('Scheduling periodic rsync')
            
        log.info('SLC_INIT Dataset Controller')

    #noinspection PyUnusedLocal
    @defer.inlineCallbacks
    def op_create_dataset_resource(self, request, headers, msg):
        """
        @Brief This method creates an empty dataset resource and returns its ID.  
        It assumes that the caller provides an Instrument Info Object
        in a Resource Configuration Request message which should be made into a
        resource.

        @param params request GPB, ?, Is there anything in the request? What?
        @retval response, GPB 12/1, a response containing the dataset resource ID
        """
        log.info('op_create_dataset_resource: ')

        # Check only the type received and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if request.MessageType is not None:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise DatasetControllerError('Expected Null message type, received %s'
                                     % str(request), request.ResponseCodes.BAD_REQUEST)

        # Use the resource client to create a resource!
        resource = yield self.resource_client.create_instance(CMD_DATASET_RESOURCE_TYPE,
                                                              ResourceName='CDM Dataset Resource',
                                                              ResourceDescription='None')

        resource.root_group = resource.CreateObject(CMD_GROUP_TYPE)


        # What state should this be in at this point?
        #resource.ResourceLifeCycleState = resource.DEVELOPED
        yield self.resource_client.put_instance(resource)

        log.info(str(resource))

        response = yield self.message_client.create_instance(MessageContentTypeID = IDREF_TYPE)

        # Create a reference to return to the caller
        # This is one pattern - it exposes the resource to the caller

        # pass the reference
        response.MessageObject = self.resource_client.reference_instance(resource)

        # Set a response code in the message envelope
        response.MessageResponseCode = response.ResponseCodes.OK

        # pfh - create local ncml file as well
        create_ncml(response.key, NCML_PATH)
        # Push to server
        #yield rsync_ncml(NCML_PATH, THREDDS_NCML_URL)

        # The following line shows how to reply to a message
        yield self.reply_ok(msg, response)


    @defer.inlineCallbacks
    def op_find_dataset_resources(self, request, headers, msg):
        """
        @Brief set the lifecycle state of the dataset resource

        @param params request GPB, 2401/1, a request to find datasets
        @retval ListFindResults Type, GPB 22/1, A list of Dataset Resource References that match the request
        """


        log.info('op_find_dataset_resources: ')

        # Check only the type recieved and linked object types. All fields are
        #strongly typed in google protocol buffers!
        if request.MessageType != FINDDATASETREQUEST_TYPE:
            # This will terminate the hello service. As an alternative reply okay with an error message
            raise DatasetControllerError('Expected message type FindDataSetRequest, received %s'
                                     % str(request), request.ResponseCodes.BAD_REQUEST)

        ### Check the type of the configuration request
        query = yield self.message_client.create_instance(PREDICATE_OBJECT_QUERY_TYPE)

        pair = query.pairs.add()

        # Set the predicate search term
        pref = query.CreateObject(PREDICATE_REFERENCE_TYPE)
        pref.key = TYPE_OF_ID

        pair.predicate = pref

        # Set the Object search term

        type_ref = query.CreateObject(IDREF_TYPE)
        type_ref.key = DATASET_RESOURCE_TYPE_ID

        pair.object = type_ref

        ### Check the type of the configuration request
        if request.IsFieldSet('by_life_cycle_state'):

            # Add a life cycle state request
            pair = query.pairs.add()

            # Set the predicate search term
            pref = query.CreateObject(PREDICATE_REFERENCE_TYPE)
            pref.key = HAS_LIFE_CYCLE_STATE_ID

            pair.predicate = pref


            # Set the Object search term
            state_ref = query.CreateObject(LCS_REFERENCE_TYPE)
            state_ref.lcs = request.by_life_cycle_state
            pair.object = state_ref

        if request.IsFieldSet('only_mine') and request.only_mine == True:

            pair = query.pairs.add()

            # Set the predicate search term
            pref = query.CreateObject(PREDICATE_REFERENCE_TYPE)
            pref.key = OWNED_BY_ID

            pair.predicate = pref

            # Set the Object search term

            type_ref = query.CreateObject(IDREF_TYPE)

            # Get the user to associate with this new resource
            user_id = headers.get('user-id', 'ANONYMOUS')
            if user_id ==  'ANONYMOUS':
                user_id = ANONYMOUS_USER_ID

            type_ref.key = user_id

            pair.object = type_ref

        result = yield self.asc.get_subjects(query)

        # The result is the same type
        self.reply_ok(msg, result)



class DatasetControllerClient(ServiceClient):
    """
    Dataset Controller Svc Client
    """

    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "dataset_controller"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def create_dataset_resource(self, msg):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('create_dataset_resource', msg)

        defer.returnValue(content)

    @defer.inlineCallbacks
    def set_dataset_resource_life_cycle(self, msg):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('set_dataset_resource_life_cycle', msg)

        defer.returnValue(content)

    @defer.inlineCallbacks
    def find_dataset_resources(self, msg):
        yield self._check_init()

        (content, headers, msg) = yield self.rpc_send('find_dataset_resources', msg)

        defer.returnValue(content)



# Spawn of the process using the module name
factory = ProcessFactory(DatasetController)


