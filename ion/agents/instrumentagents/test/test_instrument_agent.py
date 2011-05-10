#!/usr/bin/env python

"""
@file ion/agents/instrumentagents/test/test_instrument_agent.py
@brief Test cases for the InstrumentAgent and InstrumentAgentClient classes.
@author Edward Hunter

"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from ion.test.iontest import IonTestCase

import ion.util.procutils as pu
import uuid
from twisted.trial import unittest

from ion.core.process.process import Process
import ion.agents.instrumentagents.instrument_agent as instrument_agent
from ion.services.dm.distribution.events import InfoLoggingEventSubscriber
from ion.core.exception import ReceivedError
from ion.agents.instrumentagents.instrument_constants import AgentCommand    
from ion.agents.instrumentagents.instrument_constants import AgentParameter
from ion.agents.instrumentagents.instrument_constants import TimeSource
from ion.agents.instrumentagents.instrument_constants import ConnectionMethod
from ion.agents.instrumentagents.instrument_constants import AgentEvent
from ion.agents.instrumentagents.instrument_constants import AgentStatus
from ion.agents.instrumentagents.instrument_constants import AgentCapability
from ion.agents.instrumentagents.instrument_constants import AgentState
from ion.agents.instrumentagents.instrument_constants import MetadataParameter
from ion.agents.instrumentagents.instrument_constants import InstErrorCode

class TestInstrumentAgent(IonTestCase):

    # Increase the timeout so we can handle longer instrument interactions.
    timeout = 120


    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        processes = [
            {'name':'instrument_agent','module':'ion.agents.instrumentagents.instrument_agent','class':'InstrumentAgent'}
        ]

        self.sup = yield self._spawn_processes(processes)
        self.svc_id = yield self.sup.get_child_id('instrument_agent')

        self.ia_client = instrument_agent.InstrumentAgentClient(proc=self.sup,target=self.svc_id)
        
        
    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()
        
        
    @defer.inlineCallbacks
    def test_transactions(self):
        """
        Test creation and destruction of agent transactions, and ability to block
        during an open transaction.
        """

        # Open an explicit transaction.
        reply_1 = yield self.ia_client.start_transaction(0)
        success_1 = reply_1['success']
        transaction_id_1 = reply_1['transaction_id']
        self.assert_(InstErrorCode.is_ok(success_1))
        self.assertNotEqual(transaction_id_1,None)
        self.assertEqual(type(transaction_id_1),str)

        # Try to open another explicit transaction. Should fail. 
        reply_2 = yield self.ia_client.start_transaction(0)
        success_2 = reply_2['success']
        transaction_id_2 = reply_2['transaction_id']
        #self.assertNotEqual(success_2[0],'OK')
        self.assert_(InstErrorCode.is_error(success_2))
        self.assertEqual(transaction_id_2,None)
                
        # End transaction.
        reply_3 = yield self.ia_client.end_transaction(transaction_id_1)
        success_3 = reply_3['success']
        self.assert_(InstErrorCode.is_ok(success_3))
        
        # Open another explicit transaction. This tests the previous transaction
        # was properly ended.
        reply_4 = yield self.ia_client.start_transaction(0)
        success_4 = reply_4['success']
        transaction_id_4 = reply_4['transaction_id']

        self.assert_(InstErrorCode.is_ok(success_4))
        self.assertNotEqual(transaction_id_4,None)
        self.assertEqual(type(transaction_id_4),str)

        # End second open transaction.
        reply_5 = yield self.ia_client.end_transaction(transaction_id_4)
        success_5 = reply_5['success']
        self.assert_(InstErrorCode.is_ok(success_5))



    @defer.inlineCallbacks
    def test_transaction_expire_timeouts(self):
        """
        Test that transactions expire after the appropriate time.
        """
        raise unittest.SkipTest("Long running test.")
        
        
        # Set the expire timeout to a high value.
        params = {
            AgentParameter.DEFAULT_EXP_TIMEOUT:300
        }
        reply = yield self.ia_client.set_observatory(params,'create')
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok(success))

        
        # Start a transaction.        
        reply = yield self.ia_client.start_transaction(0)
        success = reply['success']
        transaction_id = reply['transaction_id']
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(type(transaction_id),str)
        self.assertEqual(len(transaction_id),36)
        
        # Sleep the agent to simulate some activity.
        params = [AgentCommand.SLEEP,5]
        reply = yield self.ia_client.execute_observatory(params,transaction_id)
        success = reply['success']
        self.assert_(InstErrorCode.is_ok(success))

        # Sleep the agent to simulate some activity.
        # The transaction should still be valid.
        reply = yield self.ia_client.execute_observatory(params,transaction_id)
        success = reply['success']
        self.assert_(InstErrorCode.is_ok(success))
        
        
        # End the transaction
        reply = yield self.ia_client.end_transaction(transaction_id)
        success = reply['success']
        self.assert_(InstErrorCode.is_ok(success))
        
        
        # Set the expire timeout to a low value.
        params = {
            AgentParameter.DEFAULT_EXP_TIMEOUT:3
        }
        reply = yield self.ia_client.set_observatory(params,'create')
        success = reply['success']
        result = reply['result']
        self.assert_(InstErrorCode.is_ok(success))
                
        # Start a transaction.        
        reply = yield self.ia_client.start_transaction(0)
        success = reply['success']
        transaction_id = reply['transaction_id']
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(type(transaction_id),str)
        self.assertEqual(len(transaction_id),36)

        # Sleep the agent to simulate some activity.
        # This should complete normally but the transaction should timeout
        # while it is running.
        params = [AgentCommand.SLEEP,5]
        reply = yield self.ia_client.execute_observatory(params,transaction_id)
        success = reply['success']
        self.assert_(InstErrorCode.is_ok(success))

        # Sleep the agent to simulate some activity.
        # This should fail as the transaction has timed out.
        reply = yield self.ia_client.execute_observatory(params,transaction_id)
        success = reply['success']
        self.assert_(InstErrorCode.is_error(success))

        # End the transaction.
        # This should fail as the resource is now free and can't be unlocked.
        reply = yield self.ia_client.end_transaction(transaction_id)
        success = reply['success']
        self.assert_(InstErrorCode.is_error(success))


        # Start a transaction with explicit expire timeout.    
        reply = yield self.ia_client.start_transaction(0,250)
        success = reply['success']
        transaction_id = reply['transaction_id']
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(type(transaction_id),str)
        self.assertEqual(len(transaction_id),36)
        
        # Sleep the agent to simulate some activity.
        params = [AgentCommand.SLEEP,5]
        reply = yield self.ia_client.execute_observatory(params,transaction_id)
        success = reply['success']
        self.assert_(InstErrorCode.is_ok(success))

        # Sleep the agent to simulate some activity.
        # The transaction should still be valid.
        reply = yield self.ia_client.execute_observatory(params,transaction_id)
        success = reply['success']
        self.assert_(InstErrorCode.is_ok(success))
        
        
        # End the transaction
        reply = yield self.ia_client.end_transaction(transaction_id)
        success = reply['success']
        self.assert_(InstErrorCode.is_ok(success))
        
                        
        # Start a transaction with a low explicit expire timeout.        
        reply = yield self.ia_client.start_transaction(0,3)
        success = reply['success']
        transaction_id = reply['transaction_id']
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(type(transaction_id),str)
        self.assertEqual(len(transaction_id),36)

        # Sleep the agent to simulate some activity.
        # This should complete normally but the transaction should timeout
        # while it is running.
        params = [AgentCommand.SLEEP,5]
        reply = yield self.ia_client.execute_observatory(params,transaction_id)
        success = reply['success']
        self.assert_(InstErrorCode.is_ok(success))

        # Sleep the agent to simulate some activity.
        # This should fail as the transaction has timed out.
        reply = yield self.ia_client.execute_observatory(params,transaction_id)
        success = reply['success']
        self.assert_(InstErrorCode.is_error(success))

        # End the transaction.
        # This should fail as the resource is now free and can't be unlocked.
        reply = yield self.ia_client.end_transaction(transaction_id)
        success = reply['success']
        self.assert_(InstErrorCode.is_error(success))

           
    @defer.inlineCallbacks
    def test_transaction_acquire_timeouts(self):
        """
        Test lifetime of an asynchronous transaction request.
        """
        raise unittest.SkipTest("Long running test.")
        
        
        # Start a transaction.        
        reply = yield self.ia_client.start_transaction(0)
        success = reply['success']
        transaction_id = reply['transaction_id']
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(type(transaction_id),str)
        self.assertEqual(len(transaction_id),36)

        # End the transaction
        reply = yield self.ia_client.end_transaction(transaction_id)
        success = reply['success']
        self.assert_(InstErrorCode.is_ok(success))

        # Tell the agent to start an unused transaction and expire it
        # in a few seconds.
        reply = yield self.ia_client.start_transaction(0,3)
        success = reply['success']
        transaction_id = reply['transaction_id']
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(type(transaction_id),str)
        self.assertEqual(len(transaction_id),36)

        # Acquire another transaction and wait on the release.
        reply = yield self.ia_client.start_transaction(10)
        success = reply['success']
        transaction_id = reply['transaction_id']
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(type(transaction_id),str)
        self.assertEqual(len(transaction_id),36)

        # End the transaction newly acquired transaction.
        reply = yield self.ia_client.end_transaction(transaction_id)
        success = reply['success']
        self.assert_(InstErrorCode.is_ok(success))
        
        # Start a transaction.        
        reply = yield self.ia_client.start_transaction(0)
        success = reply['success']
        transaction_id = reply['transaction_id']
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(type(transaction_id),str)
        self.assertEqual(len(transaction_id),36)

        # Request another transaction with a short acquisition timeout.
        reply = yield self.ia_client.start_transaction(3)
        success = reply['success']
        transaction_id_attempt = reply['transaction_id']
        self.assert_(InstErrorCode.is_error(success))
        self.assertEqual(transaction_id_attempt,None)

        # End the first transaction.
        reply = yield self.ia_client.end_transaction(transaction_id)
        success = reply['success']
        self.assert_(InstErrorCode.is_ok(success))
        
 
    @defer.inlineCallbacks
    def test_get_set_observatory(self):
        """
        Test observatory get and set operations.
        """
        
        #raise unittest.SkipTest("Temp skip.")
        
        # Get current configuration without a transacton. Verify all parameters were
        # attempted. Verify no transaction is issued.
        params_1 = AgentParameter.list()
        reply_1 = yield self.ia_client.get_observatory(params_1,'none')
        success_1 = reply_1['success']
        result_1 = reply_1['result']
        transaction_id_1 = reply_1['transaction_id']
        # It may not be possible to retirieve all parameters during development.
        self.assertEqual(result_1.keys().sort(),params_1.sort())
        self.assertEqual(transaction_id_1,None)
    
        # Get current configuration using 'all' syntax. Verify all parameters were
        # attempted. Verify the results same as before. Verify no transaction is issued.
        params_2 = ['all']
        reply_2 = yield self.ia_client.get_observatory(params_2,'none')
        success_2 = reply_2['success']
        result_2 = reply_2['result']
        transaction_id_2 = reply_2['transaction_id']
        # It may not be possible to retrieve all parameters during development. So do not assert success.
        self.assertEqual(result_2.keys().sort(),params_1.sort())
        self.assertEqual(result_1,result_2)
        self.assertEqual(transaction_id_2,None)

        # Get current configuration with implicit transaction. Verify all parameters
        # are attempted. Verify results same as before. Verify transaction ID issued.
        params_3 = ['all']
        reply_3 = yield self.ia_client.get_observatory(params_3,'create')
        success_3 = reply_3['success']
        result_3 = reply_3['result']
        transaction_id_3 = reply_3['transaction_id']
        # It may not be possible to retrieve all parameters during development. So do not assert success.
        self.assertEqual(result_3.keys().sort(),params_1.sort())
        self.assertEqual(result_1,result_3)
        self.assertEqual(type(transaction_id_3),str)
        self.assertEqual(len(transaction_id_3),36)
        
        # Try to get parameters with previous implicit transaction ID, now expired.
        # This should fail.
        params_4 = ['all']
        reply_4 = yield self.ia_client.get_observatory(params_4,transaction_id_3)        
        success_4 = reply_4['success']
        result_4 = reply_4['result']
        transaction_id_4 = reply_4['transaction_id']
        self.assert_(InstErrorCode.is_error(success_4))
        self.assertEqual(result_4,None)
        self.assertEqual(transaction_id_4,None)
        
        
        # Try to set the configuration without opening a transaction. This should fail.
        # Use parameters here that will always be available, even in development.
        params_5 = {}
        params_5[AgentParameter.TIME_SOURCE] = TimeSource.LOCAL_OSCILLATOR
        params_5[AgentParameter.CONNECTION_METHOD] = ConnectionMethod.CABLED_OBSERVATORY
        params_5[AgentParameter.MAX_EXP_TIMEOUT] = 600
        params_5[AgentParameter.DEFAULT_EXP_TIMEOUT] = 60 
        reply_5 = yield self.ia_client.set_observatory(params_5,'none')
        success_5 = reply_5['success']
        result_5 = reply_5['result']
        transaction_id_5 = reply_5['transaction_id']
        self.assert_(InstErrorCode.is_error(success_5))
        self.assertEqual(result_5,None)
        self.assertEqual(transaction_id_5,None)
       
        # Set the configuration using an implicit transaction. 
        params_6 = params_5
        reply_6 = yield self.ia_client.set_observatory(params_6,'create')
        success_6 = reply_6['success']
        result_6 = reply_6['result']
        transaction_id_6 = reply_6['transaction_id']
        self.assert_(InstErrorCode.is_ok(success_6))
        self.assertEqual(result_6.keys().sort(),params_6.keys().sort())
        self.assertEqual(type(transaction_id_6),str)
        self.assertEqual(len(transaction_id_6),36)
        
        # Get the new configuration back using the invalid transaction ID from
        # the previous implicit transaction.
        # This should fail.
        params_7 = params_5.keys()
        reply_7 = yield self.ia_client.get_observatory(params_7,transaction_id_6)
        success_7 = reply_7['success']
        result_7 = reply_7['result']
        transaction_id_7 = reply_7['transaction_id']
        self.assert_(InstErrorCode.is_error(success_7))        
        self.assertEqual(result_7,None)
        self.assertEqual(transaction_id_7,None)

        
        # Open an explicit transaction. 
        reply_8 = yield self.ia_client.start_transaction(0)
        success_8 = reply_8['success']
        transaction_id_8 = reply_8['transaction_id']
        self.assert_(InstErrorCode.is_ok(success_8))
        self.assertEqual(type(transaction_id_8),str)
        self.assertEqual(len(transaction_id_8),36)
        
        # Get the configuration using the current transaction. Verify it is the one we just set.
        params_9 = params_5.keys()
        reply_9 = yield self.ia_client.get_observatory(params_9,transaction_id_8)
        success_9 = reply_9['success']
        result_9 = reply_9['result']
        transaction_id_9 = reply_9['transaction_id']
        self.assert_(InstErrorCode.is_ok(success_9))
        for (key,val) in result_9.iteritems():
            result_9[key] = val[1]
        self.assertEqual(result_9,params_5)
        self.assertEqual(type(transaction_id_9),str)
        self.assertEqual(len(transaction_id_9),36)
        self.assertEqual(transaction_id_9,transaction_id_8)
        
        # Restore the original configuration using a bad transaction ID.
        # This should fail.
        bad_transaction_id = str(uuid.uuid4())
        params_10 = result_1
        reply_10 = yield self.ia_client.set_observatory(params_10,bad_transaction_id)
        success_10 = reply_10['success']
        result_10 = reply_10['result']
        transaction_id_10 = reply_10['transaction_id']
        self.assert_(InstErrorCode.is_error(success_10))        
        self.assertEqual(result_10,None)
        self.assertEqual(transaction_id_10,None)
        
        # Restore the original configuration of timeout values using the correct transaction ID.
        # Verify the result keys match the input params. Verify the transaction ID.
        params_11 = {}
        params_11[AgentParameter.TIME_SOURCE] = result_1[AgentParameter.TIME_SOURCE][1] 
        params_11[AgentParameter.CONNECTION_METHOD] = result_1[AgentParameter.CONNECTION_METHOD][1] 
        params_11[AgentParameter.MAX_EXP_TIMEOUT] = result_1[AgentParameter.MAX_EXP_TIMEOUT][1] 
        params_11[AgentParameter.DEFAULT_EXP_TIMEOUT] = result_1[AgentParameter.DEFAULT_EXP_TIMEOUT][1]
        reply_11 = yield self.ia_client.set_observatory(params_11,transaction_id_8)
        success_11 = reply_11['success']
        result_11 = reply_11['result']
        transaction_id_11 = reply_11['transaction_id']
        self.assert_(InstErrorCode.is_ok(success_11))
        self.assertEqual(result_11.keys().sort(),params_5.keys().sort())
        self.assertEqual(type(transaction_id_11),str)
        self.assertEqual(len(transaction_id_11),36)
        self.assertEqual(transaction_id_11,transaction_id_8)
        
        # Get full configuration. Verify it matches the original configuration. Verify the
        # transaction ID.
        params_12 = ['all']
        reply_12 = yield self.ia_client.get_observatory(params_12,transaction_id_8)
        success_12 = reply_12['success']
        result_12 = reply_12['result']
        transaction_id_12 = reply_12['transaction_id']
        # Not all parameters can be retreived during development. So don't assert success.
        self.assertEqual(result_12,result_1)
        self.assertEqual(type(transaction_id_12),str)
        self.assertEqual(len(transaction_id_12),36)
        self.assertEqual(transaction_id_12,transaction_id_8)
                
        # Try to open another transaction. This should fail.
        reply_13 = yield self.ia_client.start_transaction(0)
        success_13 = reply_13['success']
        transaction_id_13 = reply_13['transaction_id']
        self.assert_(InstErrorCode.is_error(success_13))        
        self.assertEqual(transaction_id_13,None)
        self.assertEqual(transaction_id_13,None)
        
        # Try to set a parameter to a bad value. This should fail for the invalid values only.
        params_14 = params_5
        params_14[AgentParameter.CONNECTION_METHOD] = 'I am an invalid connection method string.'
        params_14[AgentParameter.DEFAULT_EXP_TIMEOUT] = -99
        reply_14 = yield self.ia_client.set_observatory(params_14,transaction_id_8)
        success_14 = reply_14['success']
        result_14 = reply_14['result']
        transaction_id_14 = reply_14['transaction_id']
        self.assert_(InstErrorCode.is_error(success_14))        
        self.assert_(InstErrorCode.is_ok(result_14[AgentParameter.TIME_SOURCE]))
        self.assert_(InstErrorCode.is_ok(result_14[AgentParameter.MAX_EXP_TIMEOUT]))
        self.assert_(InstErrorCode.is_error(result_14[AgentParameter.CONNECTION_METHOD]))
        self.assert_(InstErrorCode.is_error(result_14[AgentParameter.DEFAULT_EXP_TIMEOUT]))
        self.assertEqual(type(transaction_id_11),str)
        self.assertEqual(len(transaction_id_11),36)
        self.assertEqual(transaction_id_14,transaction_id_8)
        
        # Try to set an unknown parameter. This should fail for unknown parameters only.
        params_15 = {
            'I_AM_AN_UNKNOWN_PARAMETER':'With a strange string value.',
            AgentParameter.DEFAULT_EXP_TIMEOUT: 15}
        reply_15 = yield self.ia_client.set_observatory(params_15,transaction_id_8)
        success_15 = reply_15['success']
        result_15 = reply_15['result']
        transaction_id_15 = reply_15['transaction_id']
        self.assert_(InstErrorCode.is_error(success_15))
        self.assert_(InstErrorCode.is_error(result_15['I_AM_AN_UNKNOWN_PARAMETER']))
        self.assert_(InstErrorCode.is_ok(result_15[AgentParameter.DEFAULT_EXP_TIMEOUT]))
        self.assertEqual(type(transaction_id_15),str)
        self.assertEqual(len(transaction_id_15),36)
        self.assertEqual(transaction_id_15,transaction_id_8)
                
        # End the current transaction.
        reply_16 = yield self.ia_client.end_transaction(transaction_id_8)
        success_16 = reply_16['success']
        self.assert_(InstErrorCode.is_ok(success_16))
        
        # Get the full configuration again. Verify it matches the original and none of
        # the bad sets made it through.
        params_17 = ['all']
        reply_17 = yield self.ia_client.get_observatory(params_17,'none')
        success_17 = reply_17['success']
        result_17 = reply_17['result']
        transaction_id_17 = reply_17['transaction_id']
        # Not all parameters can be retreived during development. So don't assert success.
        for (key,val) in result_17.iteritems():
            if key == AgentParameter.TIME_SOURCE:
                self.assert_(InstErrorCode.is_ok(val[0]))
                self.assertEqual(val[1],params_14[AgentParameter.TIME_SOURCE])
            elif key == AgentParameter.MAX_EXP_TIMEOUT:
                self.assert_(InstErrorCode.is_ok(val[0]))
                self.assertEqual(val[1],params_14[AgentParameter.MAX_EXP_TIMEOUT])
            elif key == AgentParameter.DEFAULT_EXP_TIMEOUT:
                self.assert_(InstErrorCode.is_ok(val[0]))
                self.assertEqual(val[1],params_15[AgentParameter.DEFAULT_EXP_TIMEOUT])
            else:
                self.assertEqual(val,result_1[key])
        self.assert_(InstErrorCode.is_error(result_15['I_AM_AN_UNKNOWN_PARAMETER']))
        self.assert_(InstErrorCode.is_ok(result_15[AgentParameter.DEFAULT_EXP_TIMEOUT]))
        self.assertEqual(type(transaction_id_15),str)
        self.assertEqual(len(transaction_id_15),36)
        self.assertEqual(transaction_id_15,transaction_id_8)
        
        
    @defer.inlineCallbacks
    def test_get_obsevatory_metadata(self):
        """
        Test observatory get metadata and related variables.
        """

        #raise unittest.SkipTest("Temp skip.")
        
        # Get current metadata without a transacton using 'all' syntax.
        params = [('all','all')]
        reply = yield self.ia_client.get_observatory_metadata(params,'none')
        success = reply['success']
        result = reply['result']
        tid = reply['transaction_id']
        self.assert_(InstErrorCode.is_equal(success,InstErrorCode.NOT_IMPLEMENTED))
        self.assertEqual(tid,None)
        
                   
    @defer.inlineCallbacks
    def test_get_obsevatory_status(self):
        """
        Test observatory get status and related variables.
        """
        
        #raise unittest.SkipTest("Temp skip.")
        
        # Get all observatory status vals using 'all' syntax.
        params_1 = ['all']
        reply_1 = yield self.ia_client.get_observatory_status(params_1,'none')
        success_1 = reply_1['success']
        result_1 = reply_1['result']
        transaction_id_1 = reply_1['transaction_id']
    
        self.assert_(InstErrorCode.is_ok(success_1))
        self.assertEqual(transaction_id_1,None)
        self.assertEqual(type(result_1),dict)
        self.assertEqual(result_1.keys().sort(),AgentStatus.list().sort())

        self.assertEqual(AgentState.has(result_1[AgentStatus.AGENT_STATE][1]),True)
        self.assertEqual(type(result_1[AgentStatus.CONNECTION_STATE][1]),str)
        self.assertEqual(isinstance(result_1[AgentStatus.ALARMS][1],(list,tuple)),True)
        self.assertEqual(all(map(lambda x: (x in instrument_agent.status_alarms) or (x==None),result_1[AgentStatus.ALARMS][1])),True)
        self.assertIsInstance(result_1[AgentStatus.TIME_STATUS][1],dict)
        self.assertIsInstance(result_1[AgentStatus.BUFFER_SIZE][1],int)
        self.assertIsInstance(result_1[AgentStatus.AGENT_VERSION][1],str)
        self.assertIsInstance(result_1[AgentStatus.PENDING_TRANSACTIONS][1],(list,tuple))


        # Get all observatory vals using explicit list.
        params_2 = AgentStatus.list()
        reply_2 = yield self.ia_client.get_observatory_status(params_2,'none')
        success_2 = reply_2['success']
        result_2 = reply_2['result']
        transaction_id_2 = reply_2['transaction_id']
    
        self.assert_(InstErrorCode.is_ok(success_2))
        self.assertEqual(transaction_id_2,None)
        self.assertEqual(type(result_2),dict)
        self.assertEqual(result_2,result_1)

        # Attempt to get some values, including bad ones.
        params_3 = [
            AgentStatus.AGENT_STATE,
            AgentStatus.CONNECTION_STATE,
            'BAD_STATUS_KEY_1',
            'BAD_STATUS_KEY_2'            
        ]
        reply_3 = yield self.ia_client.get_observatory_status(params_3,'none')
        success_3 = reply_3['success']
        result_3 = reply_3['result']
        transaction_id_3 = reply_3['transaction_id']

        self.assert_(InstErrorCode.is_error(success_3))
        self.assertEqual(transaction_id_3,None)
        self.assertEqual(type(result_3),dict)
        self.assertEqual(result_3.keys().sort(),params_3.sort())
        self.assert_(InstErrorCode.is_ok(result_3[AgentStatus.AGENT_STATE][0]))
        self.assert_(InstErrorCode.is_ok(result_3[AgentStatus.CONNECTION_STATE][0]))
        self.assert_(InstErrorCode.is_error(result_3['BAD_STATUS_KEY_1'][0]))
        self.assertEqual(result_3['BAD_STATUS_KEY_1'][1],None)        
        self.assert_(InstErrorCode.is_error(result_3['BAD_STATUS_KEY_2'][0]))
        self.assertEqual(result_3['BAD_STATUS_KEY_2'][1],None)        
        self.assertEqual(AgentState.has(result_1[AgentStatus.AGENT_STATE][1]),True)
        self.assertEqual(type(result_1[AgentStatus.CONNECTION_STATE][1]),str)

        # Start a transaction.
        reply_4 = yield self.ia_client.start_transaction(0)
        success_4 = reply_4['success']
        transaction_id_4 = reply_4['transaction_id']
        self.assert_(InstErrorCode.is_ok(success_4))
        self.assertEqual(type(transaction_id_4),str)
        self.assertEqual(len(transaction_id_4),36)
        
        # Try to get the status parameters with no transaction. This should fail.
        params_5 = ['all']
        reply_5 = yield self.ia_client.get_observatory_status(params_5,'none')
        success_5 = reply_5['success']
        result_5 = reply_5['result']
        transaction_id_5 = reply_5['transaction_id']
    
        self.assert_(InstErrorCode.is_error(success_5))
        self.assertEqual(result_5,None)
        self.assertEqual(transaction_id_5,None)

        # Try to get the status parameters with implicit transaction. This should fail.
        params_6 = ['all']
        reply_6 = yield self.ia_client.get_observatory_status(params_6,'create')
        success_6 = reply_6['success']
        result_6 = reply_6['result']
        transaction_id_6 = reply_6['transaction_id']
    
        self.assert_(InstErrorCode.is_error(success_6))
        self.assertEqual(result_6,None)
        self.assertEqual(transaction_id_6,None)

        # Try to get the status parameters with a bad transaction ID. This should fail.
        bad_tid = str(uuid.uuid4())
        params_7 = ['all']
        reply_7 = yield self.ia_client.get_observatory_status(params_7,bad_tid)
        success_7 = reply_7['success']
        result_7 = reply_7['result']
        transaction_id_7 = reply_7['transaction_id']
    
        self.assert_(InstErrorCode.is_error(success_7))
        self.assertEqual(result_7,None)
        self.assertEqual(transaction_id_7,None)

        # Get status parameters with valid transaction ID.
        params_8 = ['all']
        reply_8 = yield self.ia_client.get_observatory_status(params_8,transaction_id_4)
        success_8 = reply_8['success']
        result_8 = reply_8['result']
        transaction_id_8 = reply_8['transaction_id']
    
        self.assert_(InstErrorCode.is_ok(success_8))
        self.assertEqual(result_8,result_1)
        self.assertEqual(transaction_id_8,transaction_id_4)
        
        # End the transaction.
        reply_9 = yield self.ia_client.end_transaction(transaction_id_4)
        success_9 = reply_9['success']
        self.assert_(InstErrorCode.is_ok(success_9))

        # Make sure the old transaction no longer works.
        params_10 = ['all']
        reply_10 = yield self.ia_client.get_observatory_status(params_10,transaction_id_4)
        success_10 = reply_10['success']
        result_10 = reply_10['result']
        transaction_id_10 = reply_10['transaction_id']

        self.assert_(InstErrorCode.is_error(success_10))
        self.assertEqual(result_10,None)
        self.assertEqual(transaction_id_10,None)

        # An implicit transaction should now work.
        params_11 = ['all']
        reply_11 = yield self.ia_client.get_observatory_status(params_11,'create')
        success_11 = reply_11['success']
        result_11 = reply_11['result']
        transaction_id_11 = reply_11['transaction_id']
    
        self.assert_(InstErrorCode.is_ok(success_11))
        self.assertEqual(result_11,result_1)
        self.assertEqual(type(transaction_id_11),str)
        self.assertEqual(len(transaction_id_11),36)


    
    @defer.inlineCallbacks
    def test_get_capabilities(self):

        #raise unittest.SkipTest("Temp skip.")


        # Get all capabilities with the 'all' syntax.
        params = ['all']
        reply = yield self.ia_client.get_capabilities(params,'none')
        success = reply['success']
        result = reply['result']
        transaction_id = reply['transaction_id']
        result_1 = result
        
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(transaction_id,None)
        self.assertEqual(type(result),dict)
        self.assertEqual(result.keys().sort(),AgentCapability.list().sort())
        self.assertEqual(all(map(lambda x: AgentCommand.has(x),result[AgentCapability.OBSERVATORY_COMMANDS][1])),True)
        self.assertEqual(all(map(lambda x: AgentParameter.has(x),result[AgentCapability.OBSERVATORY_PARAMS][1])),True)
        self.assertEqual(all(map(lambda x: AgentStatus.has(x),result[AgentCapability.OBSERVATORY_STATUSES][1])),True)
        self.assertEqual(all(map(lambda x: MetadataParameter.has(x),result[AgentCapability.METADATA][1])),True)
        self.assertEqual(all(map(lambda x: isinstance(x,(str,None)),result[AgentCapability.DEVICE_COMMANDS][1])),True)
        self.assertEqual(all(map(lambda x: isinstance(x,(str,None)),result[AgentCapability.DEVICE_PARAMS][1])),True)
        self.assertEqual(all(map(lambda x: isinstance(x,(str,None)),result[AgentCapability.DEVICE_STATUSES][1])),True)
        

        # Get all capabilities with an explicit list.
        params = AgentCapability.list()
        reply = yield self.ia_client.get_capabilities(params,'none')
        success = reply['success']
        result = reply['result']
        transaction_id = reply['transaction_id']
        result_2 = result
        
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(transaction_id,None)
        self.assertEqual(type(result),dict)
        self.assertEqual(result_2,result_1)
        
        # Attempt to get some capabilities, including bad ones.
        params = [
            AgentCapability.OBSERVATORY_STATUSES,
            AgentCapability.METADATA,
            AgentCapability.DEVICE_COMMANDS,
            'CAP_BAD_CAPABILITY_1',
            'CAP_BAD_CAPABILITY_2'
        ]
        reply = yield self.ia_client.get_capabilities(params,'none')
        success = reply['success']
        result = reply['result']
        transaction_id = reply['transaction_id']
        result_3 = result
        
        self.assert_(InstErrorCode.is_error(success))
        self.assertEqual(type(result),dict)
        self.assertEqual(transaction_id,None)
        self.assert_(InstErrorCode.is_ok(result[AgentCapability.OBSERVATORY_STATUSES][0]))
        self.assert_(InstErrorCode.is_ok(result[AgentCapability.METADATA][0]))
        self.assert_(InstErrorCode.is_ok(result[AgentCapability.DEVICE_COMMANDS][0]))
        self.assertEqual(all(map(lambda x: AgentStatus.has(x),result[AgentCapability.OBSERVATORY_STATUSES][1])),True)
        self.assertEqual(all(map(lambda x: MetadataParameter.has(x),result[AgentCapability.METADATA][1])),True)
        self.assertEqual(all(map(lambda x: isinstance(x,(str,None)),result[AgentCapability.DEVICE_COMMANDS][1])),True)        
        self.assert_(InstErrorCode.is_error(result['CAP_BAD_CAPABILITY_1'][0]))
        self.assertEqual(result['CAP_BAD_CAPABILITY_1'][1],None)
        self.assert_(InstErrorCode.is_error(result['CAP_BAD_CAPABILITY_2'][0]))
        self.assertEqual(result['CAP_BAD_CAPABILITY_2'][1],None)
                
        # Start a transaction.
        reply = yield self.ia_client.start_transaction(0)
        success = reply['success']
        transaction_id = reply['transaction_id']
        transaction_id_4 = transaction_id
        
        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(type(transaction_id),str)
        self.assertEqual(len(transaction_id),36)

        # Try to get capabilities without a transaction.
        params = ['all']
        reply = yield self.ia_client.get_capabilities(params,'none')
        success = reply['success']
        result = reply['result']
        transaction_id = reply['transaction_id']

        self.assert_(InstErrorCode.is_error(success))
        self.assertEqual(result,None)
        self.assertEqual(transaction_id,None)
        
        
        # Try to get capabilities with implicit transaction.
        params = ['all']
        reply = yield self.ia_client.get_capabilities(params,'create')
        success = reply['success']
        result = reply['result']
        transaction_id = reply['transaction_id']

        self.assert_(InstErrorCode.is_error(success))
        self.assertEqual(result,None)
        self.assertEqual(transaction_id,None)
        
        
        # Try to get capabilities with bad transaction ID.
        bad_tid = str(uuid.uuid4())
        params = ['all']
        reply = yield self.ia_client.get_capabilities(params,bad_tid)
        success = reply['success']
        result = reply['result']
        transaction_id = reply['transaction_id']

        self.assert_(InstErrorCode.is_error(success))
        self.assertEqual(result,None)
        self.assertEqual(transaction_id,None)
        
        
        # Get capabilities with correct transaction ID.
        params = ['all']
        reply = yield self.ia_client.get_capabilities(params,transaction_id_4)
        success = reply['success']
        result = reply['result']
        transaction_id = reply['transaction_id']
        transaction_id_8 = transaction_id
        result_8 = result
        

        self.assert_(InstErrorCode.is_ok(success))
        self.assertEqual(result_8,result_1)
        self.assertEqual(transaction_id_8,transaction_id_4)
        
        # End the transaction.
        reply = yield self.ia_client.end_transaction(transaction_id_4)
        success = reply['success']
        
        self.assert_(InstErrorCode.is_ok(success))
        



 
    @defer.inlineCallbacks
    def test_publish(self):
        
        #raise unittest.SkipTest("Temp skip.")
        
        # Setup a subscriber to an event topic
        class TestEventSubscriber(InfoLoggingEventSubscriber):
            def __init__(self, *args, **kwargs):
                self.msgs = []
                InfoLoggingEventSubscriber.__init__(self, *args, **kwargs)
                
            def ondata(self, data):
                log.debug("TestEventSubscriber received a message with name: %s",
                          data['content'].name)
                self.msgs.append(data)
                
        subproc = Process()
        yield subproc.spawn()
        testsub = TestEventSubscriber(origin=str(self.svc_id),
                                      process=subproc)
        yield testsub.initialize()
        yield testsub.activate()
        
        # Twiddle the IA
        result = yield self.ia_client.start_transaction(5)
        tid = result['transaction_id']
        yield self.ia_client.end_transaction(tid)
        
        # check the event
        yield pu.asleep(1.0)
        self.assertEqual(len(testsub.msgs), 1)
        self.assertEqual(testsub.msgs[0]['content'].name, u"Transaction ended!") 
 
    """
    reply_ = yield self.ia_client.get_observatory(instrument_agent.ci_param_list,'none')
    success_ = reply_['success']
    result_ = reply_['result']
    transaction_id_ = reply_['transaction_id']

    self.assertEqual(success_[0],'OK')
    self.assertEqual(type(transaction_id_),str)
    self.assertEqual(len(transaction_id_),36)

    self.assertEqual(success_[0],'ERROR') 
    self.assertEqual(result_,None)
    self.assertEqual(transaction_id_,None)
    
    print success_
    print result_
    print transaction_id_
    
    """
 
    """
    def test_something(self):
        pass
        
        #raise unittest.SkipTest("InstrumentAgent rewrite in progress.")
    """     
    
    



