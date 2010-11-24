#!/usr/bin/env python

"""
@file ion/agents/eoiagents/wrapper_agent.py
@author Chris Mueller
@brief EOI JavaWrapperAgent and JavaWrapperAgentClient class definitions
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.agents.resource_agent import ResourceAgent
from ion.core.process.process import Process, ProcessClient

class JavaWrapperAgent(ResourceAgent):
	"""
	Class designed to facilitate tight interaction with ION in leu of a complete Java CC, including:
	Agent registration, process lifecycle, and reactivity to other core ION services
	"""
		
	@defer.inlineCallbacks
	def op_update_request(self, content, headers, msg):
		"""
		scheduler requests an update, provides the resource_id
		depending on state, may or may not call:
		spawn_dataset_agent
		then will always call:
		get_context
		get_update
		"""
		yield self.reply_err(msg, 'Not Implemented')
	
	@defer.inlineCallbacks
	def get_context(self, content, headers, msg):
		"""
		determines information required to provide necessary context to the java dataset agent
		"""
		defer.returnValue(msg, 'Not Implemented')

	@defer.inlineCallbacks
	def spawn_dataset_agent(self, content, headers, msg):
		"""
		Instantiates the java dataset agent including providing appropriate connectivity information so the agent can establish messaging channels
		"""
		defer.returnValue(msg, 'Not Implemented')
	
	@defer.inlineCallbacks
	def terminate_dataset_agent(self, content, headers, msg):
		defer.returnValue(msg, 'Not Implemented')

	@defer.inlineCallbacks
	def get_update(self, datasetId, context):
		"""
		
		"""
		defer.returnValue('Not Implemented')	

	@defer.inlineCallbacks
	def op_get_update(self, content, headers, msg):
		"""
		Perform the update - send rpc message to dataset agent providing context from op_get_context.  Agent response will be the dataset or an error.
		"""
		yield self.reply_err(msg, 'Not Implemented')
		

class JavaWrapperAgentClient(ResourceAgentClient):
	"""
	Client for direct interaction with the WrapperAgent
	"""
	
	@defer.inlineCallbacks
	def update_request(self, datasetId):
		"""
		
		"""
		defer.returnValue('Not Implemented')
	
	
# Spawn of the process using the module name
factory = ProcessFactory(JavaWrapperAgent)
	