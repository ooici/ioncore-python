#!/usr/bin/env python

"""
@file ion/core/base_process.py
@author Michael Meisinger
@author Stephen Pasco
@brief abstract superclass for all processes
"""

import ion.util.procutils as pu

class BaseProcess(object):
    """
    This is the abstract superclass for all processes.
    """
    
    def op_noop_catch(self, content, headers, msg):
        """The method called if operation is not defined
        """
    
    def receive(self, content, msg):
        """
        content - content can be anything (list, tuple, dictionary, string, int, etc.)

        For this implementation, 'content' will be a dictionary:
        content = {
            "op": "operation name here",
            "content": ('arg1', 'arg2')
        }
        """
        
        pu.log_message(__name__, content, msg)
        
        if "op" in content:
            print 'found \'op\' in content'
            # TODO: Null error check 'cont'
            cont = content['content']
            # TODO: Null error check 'op'
            op = content['op']            
            # dynamically invoke the operation
            
            opdef = getattr(self, 'op_' + op)
            if opdef != None:
                opdef(cont, content, msg)
            else:
                op_noop_catch(cont, content, msg)
        else:
            log.error("Receive() failed. Bad message", content)
            