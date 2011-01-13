#!/usr/bin/env python

"""
@file ion/services/coi/resource_registry/resource_registry.py
@author Michael Meisinger
@author David Stuebe
@brief service for registering resources

To test this with the Java CC!
> bin/start-cc -h amoeba.ucsd.edu -a sysname=eoitest res/scripts/eoi_demo.py
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.python import reflect

from ion.services.coi import datastore

from ion.core.object import gpb_wrapper

from net.ooici.resource import resource_pb2
from net.ooici.core.type import type_pb2

from ion.core.process.process import ProcessFactory, Process
from ion.core.process.service_process import ServiceProcess, ServiceClient
import ion.util.procutils as pu

# For testing - used in the client
from net.ooici.play import addressbook_pb2


from ion.core import ioninit
CONF = ioninit.config(__name__)


class EOIIngestionService(ServiceProcess):
    """
    Place holder to move data between EOI and the datastore
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='eoi_ingest', version='0.1.0', dependencies=[])

    #TypeClassType = gpb_wrapper.set_type_from_obj(type_pb2.GPBType())

    def __init__(self, *args, **kwargs):
        # Service class initializer. Basic config, but no yields allowed.
        
        #assert isinstance(backend, store.IStore)
        #self.backend = backend
        ServiceProcess.__init__(self, *args, **kwargs)

        self.push = self.workbench.push
        self.pull = self.workbench.pull
        self.fetch_linked_objects = self.workbench.fetch_linked_objects
        self.op_fetch_linked_objects = self.workbench.op_fetch_linked_objects
        self.fetch_linked_objects = self.workbench.fetch_linked_objects

        log.info('ResourceRegistryService.__init__()')

    @defer.inlineCallbacks
    def op_ingest(self, content, headers, msg):
        """
        Push this dataset to the datastore
        """
        log.debug('op_ingest recieved content:'+ str(content))
        
        msg_repo = content.Repository
        
        response, exception = yield self.push('datastore', msg_repo.repository_key)
        
        assert response == self.ION_SUCCESS, 'Push to datastore failed!'
        
        yield self.reply(msg, content=msg_repo.repository_key)
        


    @defer.inlineCallbacks
    def op_retrieve(self, content, headers, msg):
        """
        Return the root group of the dataset
        Content is the unique ID for a particular dataset
        """
        log.debug('op_retrieve: recieved content:'+ str(content))
        response, exception = yield self.pull('datastore', str(content))
        
        assert response == self.ION_SUCCESS, 'Push to datastore failed!'
        
        repo = self.workbench.get_repository(content)
        
        head = repo.checkout('master')
        
        yield self.reply(msg, content=head)
        


    
class EOIIngestionClient(ServiceClient):
    """
    Class for the client accessing the resource registry.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "eoi_ingest"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def ingest(self):
        """
        No argument needed - just send a simple object....
        """
        yield self._check_init()
        
        repo, ab = self.proc.workbench.init_repository(addressbook_pb2.AddressLink)
        
        ab.person.add()

        p = repo.create_wrapped_object(addressbook_pb2.Person)
        p.name = 'david'
        p.id = 59
        p.email = 'stringgggg'
        ab.person[0] = p
        
        print 'AdressBook!',ab
        
        (content, headers, msg) = yield self.rpc_send('ingest', ab)
        
        response = headers.get(self.MSG_RESPONSE)
        exception = headers.get(self.MSG_EXCEPTION)
        
        log.info('EOI Ingestion Service; Ingest replied: %s, ID: %s' % (response, str(content)))

        defer.returnValue((response, content))
        
        

    @defer.inlineCallbacks
    def retrieve(self,dataset_id):
        """
        @brief Client method to Register a Resource Instance
        This method is used to generate a new resource instance of type
        Resource Type
        @param resource_type
        """
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('retrieve', dataset_id)
        
        
        log.info('EOI Ingestion Service; Retrieve replied: '+str(content))
        # Return value should be a resource identity
        defer.returnValue(str(content))
        

# Spawn of the process using the module name
factory = ProcessFactory(EOIIngestionService)


