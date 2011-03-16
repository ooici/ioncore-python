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


import ion.agents.instrumentagents.instrument_agent as instrument_agent
from ion.core.exception import ReceivedError
import ion.util.procutils as pu
import uuid
from twisted.trial import unittest


    
    

class TestInstrumentAgent(IonTestCase):

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
        
        
        
        pu.asleep(1)
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
        
        self.assertEqual(success_1[0],'OK')
        self.assertNotEqual(transaction_id_1,None)
        self.assertEqual(type(transaction_id_1),str)

        # Try to open another explicit transaction. Should fail. 
        reply_2 = yield self.ia_client.start_transaction(0)
        success_2 = reply_2['success']
        transaction_id_2 = reply_2['transaction_id']
        
        self.assertNotEqual(success_2[0],'OK')
        self.assertEqual(transaction_id_2,None)
                
        # End transaction.
        reply_3 = yield self.ia_client.end_transaction(transaction_id_1)
        success_3 = reply_3['success']
        self.assertEqual(success_3[0],'OK')
        
        # Open another explicit transaction. This tests the previous transaction
        # was properly ended.
        reply_4 = yield self.ia_client.start_transaction(0)
        success_4 = reply_4['success']
        transaction_id_4 = reply_4['transaction_id']

        self.assertEqual(success_4[0],'OK')
        self.assertNotEqual(transaction_id_4,None)
        self.assertEqual(type(transaction_id_4),str)

        # End second open transaction.
        reply_5 = yield self.ia_client.end_transaction(transaction_id_4)
        success_5 = reply_5['success']
        self.assertEqual(success_5[0],'OK')


                  
    @defer.inlineCallbacks
    def test_execute_observatory(self):
        """
        Test observatory command execution, and implicit and explicit transactions.
        """
        
        cmd = ['CI_CMD_STATE_TRANSITION','CI_TRANS_INITIALIZE']
        
        # No transaction. This should fail.
        reply_1 = yield self.ia_client.execute_observatory(cmd,'none')
        success_1 = reply_1['success']
        result_1 = reply_1['result']
        transaction_id_1 = reply_1['transaction_id']
        self.assertEqual(success_1[0],'ERROR')
        self.assertEqual(result_1,None)
        self.assertEqual(transaction_id_1,None)

                
        # Implicit transaction.
        reply_2 = yield self.ia_client.execute_observatory(cmd,'create') 
        success_2 = reply_2['success']
        result_2 = reply_2['result']
        transaction_id_2 = reply_2['transaction_id']
        self.assertEqual(success_2[0],'OK')
        self.assertEqual(type(transaction_id_2),str)
        self.assertEqual(len(transaction_id_2),36)

        
        # Implicit transaction. Tests the previous transaction was properly ended.
        reply_3 = yield self.ia_client.execute_observatory(cmd,'create') 
        success_3 = reply_3['success']
        result_3 = reply_3['result']
        transaction_id_3 = reply_3['transaction_id']
        self.assertEqual(success_2[0],'OK')
        self.assertEqual(type(transaction_id_2),str)
        self.assertEqual(len(transaction_id_2),36)

        
        # Explicit transaction.
        reply_4 = yield self.ia_client.start_transaction(0)
        success_4 = reply_4['success']
        transaction_id_4 = reply_4['transaction_id']
        self.assertEqual(success_4[0],'OK')
        self.assertEqual(type(transaction_id_4),str)
        self.assertEqual(len(transaction_id_4),36)

        reply_5 = yield self.ia_client.execute_observatory(cmd,transaction_id_4) 
        success_5 = reply_5['success']
        result_5 = reply_5['result']
        transaction_id_5 = reply_5['transaction_id']
        self.assertEqual(success_5[0],'OK')
        self.assertEqual(type(transaction_id_5),str)
        self.assertEqual(len(transaction_id_5),36)
        self.assertEqual(transaction_id_4,transaction_id_5)
        
        
        # Attempt to execute another command with a bad transaction ID.
        bad_transaction_id = str(uuid.uuid4())
        reply_6 = yield self.ia_client.execute_observatory(cmd,bad_transaction_id) 
        success_6 = reply_6['success']
        result_6 = reply_6['result']
        transaction_id_6 = reply_6['transaction_id']
        self.assertEqual(success_6[0],'ERROR')
        self.assertEqual(result_6,None)
        self.assertEqual(transaction_id_6,None)
                
        # Attempt to execute another command with the correct transaction ID.
        reply_7 = yield self.ia_client.execute_observatory(cmd,transaction_id_4) 
        success_7 = reply_7['success']
        result_7 = reply_7['result']
        transaction_id_7 = reply_7['transaction_id']
        self.assertEqual(success_7[0],'OK')
        self.assertEqual(type(transaction_id_7),str)
        self.assertEqual(len(transaction_id_7),36)
        self.assertEqual(transaction_id_4,transaction_id_7)
        
        # Execute a state transition with a bad transition argument.
        cmd_bad_argument = ['CI_CMD_STATE_TRANSITION','CI_TRANS_BAD_TRANSITION']
        reply_8 = yield self.ia_client.execute_observatory(cmd_bad_argument,transaction_id_4) 
        success_8 = reply_8['success']
        result_8 = reply_8['result']
        transaction_id_8 = reply_8['transaction_id']
        self.assertEqual(success_8[0],'ERROR')
        self.assertEqual(type(transaction_id_8),str)
        self.assertEqual(len(transaction_id_8),36)
        self.assertEqual(transaction_id_4,transaction_id_8)
               
        # Execute an unknown command.
        cmd_unknown = ['I am an unknown command','With unknown argument']
        reply_9 = yield self.ia_client.execute_observatory(cmd_unknown,transaction_id_4) 
        success_9 = reply_9['success']
        result_9 = reply_9['result']
        transaction_id_9 = reply_9['transaction_id']
        self.assertEqual(success_8[0],'ERROR')
        self.assertEqual(type(transaction_id_8),str)
        self.assertEqual(len(transaction_id_8),36)
        self.assertEqual(transaction_id_4,transaction_id_8)        
        
        # Close the transaction.
        reply_10 = yield self.ia_client.end_transaction(transaction_id_4)
        success_10 = reply_10['success']
        self.assertEqual(success_10[0],'OK')        
        
        # Run a valid command with the old transaction ID.
        reply_11 = yield self.ia_client.execute_observatory(cmd,transaction_id_4) 
        success_11 = reply_11['success']
        result_11 = reply_11['result']
        transaction_id_11 = reply_11['transaction_id']
        self.assertEqual(success_11[0],'ERROR')
        self.assertEqual(result_11,None)
        self.assertEqual(transaction_id_11,None)
        
        # Run an implicit transaction.
        reply_12 = yield self.ia_client.execute_observatory(cmd,'create')
        success_12 = reply_12['success']
        result_12 = reply_12['result']
        transaction_id_12 = reply_12['transaction_id']
        self.assertEqual(success_12[0],'OK')
        self.assertEqual(type(transaction_id_12),str)
        self.assertEqual(len(transaction_id_12),36)
        self.assertNotEqual(transaction_id_4,transaction_id_12)
 
        # Run another implicit transaction. Ensure IDs are unique.
        reply_13 = yield self.ia_client.execute_observatory(cmd,'create')
        success_13 = reply_13['success']
        result_13 = reply_13['result']
        transaction_id_13 = reply_13['transaction_id']
        self.assertEqual(success_13[0],'OK')
        self.assertEqual(type(transaction_id_13),str)
        self.assertEqual(len(transaction_id_13),36)
        self.assertNotEqual(transaction_id_12,transaction_id_13)
 
 
    @defer.inlineCallbacks
    def test_get_set_observatory(self):
        """
        Test observatory get and set operations.
        """
        
        # Get current configuration without a transacton. Verify all parameters were
        # attempted. Verify no transaction is issued.
        params_1 = instrument_agent.ci_param_list
        reply_1 = yield self.ia_client.get_observatory(params_1,'none')
        success_1 = reply_1['success']
        result_1 = reply_1['result']
        transaction_id_1 = reply_1['transaction_id']
        # It may not be possible to retirieve all parameters during development.
        #self.assertEqual(success_1[0],'OK')
        self.assertEqual(result_1.keys().sort(),instrument_agent.ci_param_list.sort())
        self.assertEqual(transaction_id_1,None)
    
        # Get current configuration using 'all' syntax. Verify all parameters were
        # attempted. Verify the results same as before. Verify no transaction is issued.
        params_2 = ['all']
        reply_2 = yield self.ia_client.get_observatory(params_2,'none')
        success_2 = reply_2['success']
        result_2 = reply_2['result']
        transaction_id_2 = reply_2['transaction_id']
        # It may not be possible to retrieve all parameters during development. So do not assert success.
        #self.assertEqual(success_2[0],'OK')
        self.assertEqual(result_2.keys().sort(),instrument_agent.ci_param_list.sort())
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
        #self.assertEqual(success_3[0],'OK')
        self.assertEqual(result_3.keys().sort(),instrument_agent.ci_param_list.sort())
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
        self.assertEqual(success_4[0],'ERROR')
        self.assertEqual(result_4,None)
        self.assertEqual(transaction_id_4,None)
        
        
        # Try to set the configuration without opening a transaction. This should fail.
        # Use parameters here that will always be available, even in development.
        params_5 = {}
        params_5['CI_PARAM_TIME_SOURCE'] = 'TIME_LOCAL_OSCILLATOR'
        params_5['CI_PARAM_CONNECTION_METHOD'] = 'CONNECTION_PART_TIME_RANDOM'
        params_5['CI_PARAM_MAX_TRANSACTION_TIMEOUT'] = 600
        params_5['CI_PARAM_DEFAULT_TRANSACTION_TIMEOUT'] = 60 
        reply_5 = yield self.ia_client.set_observatory(params_5,'none')
        success_5 = reply_5['success']
        result_5 = reply_5['result']
        transaction_id_5 = reply_5['transaction_id']
        self.assertEqual(success_5[0],'ERROR')
        self.assertEqual(result_5,None)
        self.assertEqual(transaction_id_5,None)
       
        # Set the configuration using an implicit transaction. 
        params_6 = params_5
        reply_6 = yield self.ia_client.set_observatory(params_6,'create')
        success_6 = reply_6['success']
        result_6 = reply_6['result']
        transaction_id_6 = reply_6['transaction_id']
        self.assertEqual(success_6[0],'OK')
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
        self.assertEqual(success_7[0],'ERROR')
        self.assertEqual(result_7,None)
        self.assertEqual(transaction_id_7,None)

        
        # Open an explicit transaction. 
        reply_8 = yield self.ia_client.start_transaction(0)
        success_8 = reply_8['success']
        transaction_id_8 = reply_8['transaction_id']
        self.assertEqual(success_8[0],'OK')
        self.assertEqual(type(transaction_id_8),str)
        self.assertEqual(len(transaction_id_8),36)
        
        # Get the configuration using the current transaction. Verify it is the one we just set.
        params_9 = params_5.keys()
        reply_9 = yield self.ia_client.get_observatory(params_9,transaction_id_8)
        success_9 = reply_9['success']
        result_9 = reply_9['result']
        transaction_id_9 = reply_9['transaction_id']
        self.assertEqual(success_9[0],'OK')
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
        self.assertEqual(success_10[0],'ERROR')
        self.assertEqual(result_10,None)
        self.assertEqual(transaction_id_10,None)

        # Restore the original configuration of timeout values using the correct transaction ID.
        # Verify the result keys match the input params. Verify the transaction ID.
        params_11 = {}
        params_11['CI_PARAM_TIME_SOURCE'] = result_1['CI_PARAM_TIME_SOURCE'][1] 
        params_11['CI_PARAM_CONNECTION_METHOD'] = result_1['CI_PARAM_CONNECTION_METHOD'][1] 
        params_11['CI_PARAM_MAX_TRANSACTION_TIMEOUT'] = result_1['CI_PARAM_MAX_TRANSACTION_TIMEOUT'][1] 
        params_11['CI_PARAM_DEFAULT_TRANSACTION_TIMEOUT'] = result_1['CI_PARAM_DEFAULT_TRANSACTION_TIMEOUT'][1]
        reply_11 = yield self.ia_client.set_observatory(params_11,transaction_id_8)
        success_11 = reply_11['success']
        result_11 = reply_11['result']
        transaction_id_11 = reply_11['transaction_id']
        self.assertEqual(success_11[0],'OK')
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
        #self.assertEqual(success_12[0],'OK')
        self.assertEqual(result_12,result_1)
        self.assertEqual(type(transaction_id_12),str)
        self.assertEqual(len(transaction_id_12),36)
        self.assertEqual(transaction_id_12,transaction_id_8)
                
        # Try to open another transaction. This should fail.
        reply_13 = yield self.ia_client.start_transaction(0)
        success_13 = reply_13['success']
        transaction_id_13 = reply_13['transaction_id']
        self.assertEqual(success_13[0],'ERROR')
        self.assertEqual(transaction_id_13,None)
        self.assertEqual(transaction_id_13,None)
        
        # Try to set a parameter to a bad value. This should fail for the invalid values only.
        params_14 = params_5
        params_14['CI_PARAM_CONNECTION_METHOD'] = 'I am an invalid connection method string.'
        params_14['CI_PARAM_DEFAULT_TRANSACTION_TIMEOUT'] = -99
        reply_14 = yield self.ia_client.set_observatory(params_14,transaction_id_8)
        success_14 = reply_14['success']
        result_14 = reply_14['result']
        transaction_id_14 = reply_14['transaction_id']
        self.assertEqual(success_14[0],'ERROR')
        self.assertEqual(result_14['CI_PARAM_TIME_SOURCE'][0],'OK')
        self.assertEqual(result_14['CI_PARAM_MAX_TRANSACTION_TIMEOUT'][0],'OK')
        self.assertEqual(result_14['CI_PARAM_CONNECTION_METHOD'][0],'ERROR')
        self.assertEqual(result_14['CI_PARAM_DEFAULT_TRANSACTION_TIMEOUT'][0],'ERROR')
        self.assertEqual(type(transaction_id_11),str)
        self.assertEqual(len(transaction_id_11),36)
        self.assertEqual(transaction_id_14,transaction_id_8)
        
        # Try to set an unknown parameter. This should fail for unknown parameters only.
        params_15 = {
            'I_AM_AN_UNKNOWN_PARAMETER':'With a strange string value.',
            'CI_PARAM_DEFAULT_TRANSACTION_TIMEOUT': 15}
        reply_15 = yield self.ia_client.set_observatory(params_15,transaction_id_8)
        success_15 = reply_15['success']
        result_15 = reply_15['result']
        transaction_id_15 = reply_15['transaction_id']
        self.assertEqual(success_15[0],'ERROR')
        self.assertEqual(result_15['I_AM_AN_UNKNOWN_PARAMETER'][0],'ERROR')
        self.assertEqual(result_15['CI_PARAM_DEFAULT_TRANSACTION_TIMEOUT'][0],'OK')
        self.assertEqual(type(transaction_id_15),str)
        self.assertEqual(len(transaction_id_15),36)
        self.assertEqual(transaction_id_15,transaction_id_8)
                
        # End the current transaction.
        reply_16 = yield self.ia_client.end_transaction(transaction_id_8)
        success_16 = reply_16['success']
        self.assertEqual(success_16[0],'OK')
        
        # Get the full configuration again. Verify it matches the original and none of
        # the bad sets made it through.
        params_17 = ['all']
        reply_17 = yield self.ia_client.get_observatory(params_17,'none')
        success_17 = reply_17['success']
        result_17 = reply_17['result']
        transaction_id_17 = reply_17['transaction_id']
        # Not all parameters can be retreived during development. So don't assert success.
        #self.assertEqual(success_17[0],'OK')
        for (key,val) in result_17.iteritems():
            if key == 'CI_PARAM_TIME_SOURCE':
                self.assertEqual(val[0][0],'OK')
                self.assertEqual(val[1],params_14['CI_PARAM_TIME_SOURCE'])
            elif key == 'CI_PARAM_MAX_TRANSACTION_TIMEOUT':
                self.assertEqual(val[0][0],'OK')
                self.assertEqual(val[1],params_14['CI_PARAM_MAX_TRANSACTION_TIMEOUT'])
            elif key == 'CI_PARAM_DEFAULT_TRANSACTION_TIMEOUT':
                self.assertEqual(val[0][0],'OK')
                self.assertEqual(val[1],params_15['CI_PARAM_DEFAULT_TRANSACTION_TIMEOUT'])
            else:
                self.assertEqual(val,result_1[key])
        self.assertEqual(result_15['I_AM_AN_UNKNOWN_PARAMETER'][0],'ERROR')
        self.assertEqual(result_15['CI_PARAM_DEFAULT_TRANSACTION_TIMEOUT'][0],'OK')
        self.assertEqual(type(transaction_id_15),str)
        self.assertEqual(len(transaction_id_15),36)
        self.assertEqual(transaction_id_15,transaction_id_8)
        
 
 
 
 
 
 
 
 
 
 
 
 
 
 
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
    @defer.inlineCallbacks
    def test_something(self):
        
        
        raise unittest.SkipTest("InstrumentAgent rewrite in progress.")
    """        
    
    


        