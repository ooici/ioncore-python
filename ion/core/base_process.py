#!/usr/bin/env python

"""
@file ion/core/base_process.py
@author Stephen Pasco
@package ion.core abstract superclass for all agents
"""

import ion.util.procutils as pu

class BaseProcess(object):
    """
    This is the abstract superclass for all processes.
    """
    
    def receive(self, content, msg):
        
        print 'in receive'
        
        """
        content - content can be anything (list, tuple, dictionary, string, int, etc.)

        For this implementation, 'content' will be a dictionary:
        content = {
            "method": "method name here",
            "args": ('arg1', 'arg2')
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
            getattr(self, 'op_' + op)(cont, content, msg)
            
        log.error("Receive() failed. Bad message", content) 