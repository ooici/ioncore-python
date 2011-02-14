#!/usr/bin/env python

"""
@file ion/core/object/codec.py
@author David Stuebe
@brief Interceptor for encoding and decoding ION messages
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.intercept.interceptor import EnvelopeInterceptor
import ion.util.procutils as pu

from ion.core.object import gpb_wrapper
from ion.core.object import repository
from net.ooici.core.container import container_pb2
from ion.core.object import object_utils
from ion.core.messaging import message_client
ion_message_type = object_utils.create_type_identifier(object_id=11, version=1)


ION_R1_GPB = 'ION R1 GPB'

class CodecError(Exception):
    """
    An error class for problems that occur in the codec
    """


class ObjectCodecInterceptor(EnvelopeInterceptor):
    """
    Interceptor that assembles the headers in the ION message format.
    """
    def before(self, invocation):
        """
        print '======= Hello from BEFORE Davids Codec!============'
        #print 'args', invocation.args
        print 'path', invocation.path
        #print 'message', invocation.message
        print 'content', invocation.content
        print 'status', invocation.status
        print 'route', invocation.route
        print 'note', invocation.note
        print 'workbench', invocation.workbench
        """
        
        # Only mess with ION_R1_GPB encoded objects...
        if isinstance(invocation.content, dict) and ION_R1_GPB == invocation.content['encoding']:
            raw_content = invocation.content['content']
            unpacked_content = invocation.workbench.unpack_structure(raw_content)
                
            if hasattr(unpacked_content, 'ObjectType') and \
                unpacked_content.ObjectType == ion_message_type:
                # If this content should be returned in a Message Instance
                content = message_client.MessageInstance(unpacked_content.Repository)
            
            else:
                # Continue to allow the return of non Message Instance objects...
                content = unpacked_content
            
            invocation.content['content'] = content
            
            
        
        #print '======= End BEFORE Davids Codec!============'
        
        return invocation

    def after(self, invocation):
        """
        print '======= Hello from AFTER Davids Codec! ================'
        #print 'args', invocation.args
        print 'path', invocation.path
        #print 'message', invocation.message
        print 'content', invocation.content
        print 'status', invocation.status
        print 'route', invocation.route
        print 'note', invocation.note
        print 'workbench', invocation.workbench
        """
        content = invocation.message['content']
          
        if isinstance(content, message_client.MessageInstance):
            invocation.message['content'] = invocation.workbench.pack_structure(content.Message)
            invocation.message['encoding'] = ION_R1_GPB
            
        elif isinstance(content, gpb_wrapper.Wrapper):
            invocation.message['content'] = invocation.workbench.pack_structure(content)
        
            invocation.message['encoding'] = ION_R1_GPB      
        
        elif isinstance(content, repository.Repository):
            invocation.message['content'] = invocation.workbench.pack_repository(content)
                     
            invocation.message['encoding'] = ION_R1_GPB
            
        elif isinstance(content, container_pb2.Structure):
            serialized = content.SerializeToString()
            invocation.message['content'] = serialized
            
            invocation.message['encoding'] = ION_R1_GPB
        
        elif isinstance(content, list) and len(content)>0:
                        
            if isinstance(content[0], repository.Repository):
                # assume it is a list of repository objects to send
                
                
                print 'PELEPELEPELPELELELPELPELPELPPEEL'
                
                invocation.message['content'] = invocation.workbench.pack_repositories(content)
                     
                invocation.message['encoding'] = ION_R1_GPB
                                    
        
        #print '======= End AFTER Davids Codec! ================'

        
        return invocation
