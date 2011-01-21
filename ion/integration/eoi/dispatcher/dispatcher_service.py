#!/usr/bin/env python
"""
Created on Nov 9, 2010

@file:   ion/integration/eoi/dispatcher/dispatcher_service.py
@author: Tim LaRocque
@brief:  Dispatching service for starting remote workflows (exernal processes) for data assimilation/processing upon changes to availability of data
"""

import ion.util.ionlog
import subprocess

from twisted.internet import defer
from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.messaging.ion_reply_codes import ResponseCodes as RC


log = ion.util.ionlog.getLogger(__name__)


#@todo: Rename to WorkflowDispatcher
class DispatcherService(ServiceProcess):
    """
    Dispatching service for starting remote workflow (scripts)
    """

    
    declare = ServiceProcess.service_declare(name='workflow_dispatcher',
                                             version='0.3.0',
                                             dependencies=[]) # no dependecies

    
    
    def __init__(self, *args, **kwargs):
        """
        Initializes the DispatcherService class
        (Yields NOT allowed)
        """
        # Step 1: Delegate initialization to parent
        log.info('Starting initialization...')
#        if 'spawnargs' not in kwargs:
#            kwargs['spawnargs'] = {}
#        kwargs['spawnargs']['proc-name'] = __name__ + ".DispatcherService"
        ServiceProcess.__init__(self, *args, **kwargs)
        # Step 2: Add class attributes
        # NONE
        
    def slc_init(self):
        """
        Initializes the Dispatching Service when spawned
        (Yields ALLOWED)
        """
        pass
    
    @defer.inlineCallbacks
    def op_notify(self, content, headers, msg):
        """
        Receives a notification of change to a dataset and launches the associated workflow.
        Content should be provided as a dictionary with keys {'dataset_id', 'dataset_name', 'workflow'}
        @see _unpack_notification(content)
        """
        # Step 1: Retrieve workflow, dataset ID and name from the message content
        try:
            (datasetId, datasetName, workflow) = self._unpack_notification(content)
        except (TypeError, KeyError) as ex:
            reply = "Invalid notify content: %s" % (str(ex))
            yield self.reply_uncaught_err(msg, content = reply, response_code = reply)
            defer.returnValue(None)
        # Step 2: Build the subprocess arguments to start the workflow
        args = self._prepare_workflow(datasetId, datasetName, workflow)
        # Step 3: Start the workflow with the subprocess module
        try:
#            proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            proc = subprocess.Popen(args)
        except Exception as ex:
            reply = "Could not start workflow: %s" % (str(ex))
            (content, headers, msg) = yield self.reply_uncaught_err(msg, content = reply, response_code = reply)
            defer.returnValue(content)
        returncode = proc.wait()
        if returncode == 0:
            yield self.reply(msg, content="Workflow completed with SUCCESS")
        else:
            msgout = proc.communicate()[0]
            yield self.reply(msg, content="Error on notify.  Retrieved return code: '%s'.  Retrieved message: %s" % (str(returncode), str(msgout)))
    
    def _unpack_notification(self, content):
        """
        Extracts pertinent data from the given content and returns that data as a 3-tuple.
        Content should be provided as a dictionary with keys {'dataset_id', 'dataset_name', 'workflow'}
        @see _unpack_notification(content)
        """
        if (type(content) is not dict):
            raise TypeError("Content must be a dictionary with keys 'workflow' and 'dataset_id'")
        if ('dataset_id' not in content):
            raise KeyError("Content dict must contain an entry for 'dataset_id'")
        if ('dataset_name' not in content):
            raise KeyError("Content dict must contain an entry for 'dataset_name'")
        if ('workflow' not in content):
            raise KeyError("Content dict must contain an entry for 'workflow'")
        return (content['dataset_id'], content['dataset_name'], content['workflow'])
            
            
    def _prepare_workflow(self, datasetId, datasetName, workflow):
        '''
        Generates a list of arguments from the given parameters to start an external workflow.
        ---Currently just drops the given arguments into a list as they are.---
        '''
        #@todo: This unsafe as it permits injection attacks.  Validate inputs here.
        return [workflow, datasetId, datasetName]
        
class DispatcherServiceClient(ServiceClient):
    """
    This is an example client which calls the DispatcherService.  It's
    intent is to notify the dispatcher of changes to data sources so
    it can make requests to start various data processing/modeling scripts.
    This test client effectually simulates notifications as if by the Scheduler Service.
    """
    
    DEFAULT_GET_SCRIPT = 'res/apps/eoi_dispatcher/get_ooi_dataset.sh'
    _next_id = 0
    
    def __init__(self, *args, **kwargs):
        kwargs['targetname'] = 'workflow_dispatcher'
        ServiceClient.__init__(self, *args, **kwargs)
    
    @classmethod
    def next_id(cls):
        cls._next_id += 1
        return str(cls._next_id)
    
    @defer.inlineCallbacks
    def rpc_notify(self, datasetId, datasetName=None, workflow=DEFAULT_GET_SCRIPT):
        '''
        Dispatches a change notification so that the dispatcher can launch the appropriate
        workflow using the given dataset_id, dataset_name, and workflow filepath.
        '''
        yield self._check_init()
        if (datasetName == None):
            datasetName = 'example_dataset_' + DispatcherServiceClient.next_id()
        (content, headers, msg) = yield self.rpc_send('notify', {"dataset_id":datasetId, "dataset_name": datasetName, "workflow":workflow})
        log.info("<<<---@@@ Incoming RPC reply...")
        log.debug("\n\n\n...Content:\t" + str(content))
        log.debug("...Headers\t" + str(headers))
        log.debug("...Message\t" + str(msg) + "\n\n\n")
        
        if (headers[RC.MSG_STATUS] == RC.ION_OK):
            defer.returnValue("Notify invokation completed with status OK.  Result: %s" % (str(content)))
        else:
            defer.returnValue("Notify invokation completed with status %s.  Response code: %s" % (str(headers[RC.MSG_STATUS]), str(headers[RC.MSG_RESPONSE])))

factory = ProcessFactory(DispatcherService)




'''
#---------------------#
# Copy/paste startup:
#---------------------#
#  :Preparation
from ion.demo.eoilca.dispatcher_service import DispatcherService as ds, DispatcherServiceClient as dsc; dclient = dsc(); dservice = ds(); dservice.spawn();

#  :Send change notification for the source indicated by datasetId... (this launches the default script which simply downloads the associated dataset)
dclient.rpc_notify(datasetId=[add id])


'''
