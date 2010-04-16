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
        """
        """
        print 'in receive'
        pu.log_message(__name__, content, msg)
        if "op" in content:
            print 'in receive op'
            cont = content['content']
            getattr(self,'op_' + content['op'])(cont,content,msg)

        log.error("Receive() failed. Bad message", content) 