#!/usr/bin/env python

"""
@file ion/core/object/codec.py
@author David Stuebe
@brief Interceptor for encoding and decoding ION messages
"""

from twisted.internet import defer

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core.intercept.interceptor import EnvelopeInterceptor
from google.protobuf.internal import decoder

from ion.core.object import gpb_wrapper
from ion.core.object import repository
from net.ooici.core.container import container_pb2
from ion.core.object import object_utils
from ion.core.messaging import message_client

ION_MESSAGE_TYPE = object_utils.create_type_identifier(object_id=11, version=1)

STRUCTURE_ELEMENT_TYPE = object_utils.create_type_identifier(object_id=1, version=1)
STRUCTURE_TYPE = object_utils.create_type_identifier(object_id=2, version=1)

ION_R1_GPB = 'ION R1 GPB'

class CodecError(Exception):
    """
    An error class for problems that occur in the codec
    """


class ObjectCodecInterceptor(EnvelopeInterceptor):
    """
    Interceptor that decodes the serialized content in a message.
    The object returned is the root of a repository structure. It is not yet added to the workbench and completely
    separate from the process until it finishes the interceptor stack!
    """
    def before(self, invocation):

        # Only mess with ION_R1_GPB encoded objects...
        if isinstance(invocation.content, dict) and ION_R1_GPB == invocation.content['encoding']:
            raw_content = invocation.content['content']
            unpacked_content = unpack_structure(raw_content)
                
            if hasattr(unpacked_content, 'ObjectType') and unpacked_content.ObjectType == ION_MESSAGE_TYPE:
                # If this content should be returned in a Message Instance
                unpacked_content = message_client.MessageInstance(unpacked_content.Repository)

            
            invocation.content['content'] = unpacked_content

        return invocation

    def after(self, invocation):
        """
        Encode a Message Instance to a serialized form.
        Also possible to encode a gpb_wrapper for backward compatibility.
        """

        content = invocation.message['content']
          
        if isinstance(content, (message_client.MessageInstance, gpb_wrapper.Wrapper)):

            # Turn of access to shared process object Cache
            content.Repository.index_hash.has_cache = False

            invocation.message['content'] = pack_structure(content)
        
            invocation.message['encoding'] = ION_R1_GPB

            # Turn it back on.
            content.Repository.index_hash.has_cache = True


        return invocation



def pack_structure(content):
    """
    Pack all children of the content stucture into a message.
    Return the content as a serialized container object.
    """
    log.debug('pack_structure: Packing wrapper:\n'+str(content))


    repo = getattr(content, 'Repository', None)
    if repo is None:
        raise CodecError('Pack Structure received content which does not have a valid Repository')


    if not repo.status == repo.UPTODATE:
        comment='Commiting to send message with wrapper object'
        repo.commit(comment=comment)

    obj_set=set()


    # Get the serialized root object
    root_obj_key = repo.root_object.MyId
    root_obj = repo.index_hash.get(root_obj_key)

    items = set([root_obj])


    # Recurse through the DAG and add the keys to a set - obj_set.
    while len(items) > 0:
        child_items = set()
        for item in items:

            # Add this item to the set we are sending
            if item not in obj_set:
                obj_set.add(item)

                for key in item.ChildLinks:

                    obj = repo.index_hash.get(key,None)
                    if not obj:
                        # Debugging exception - remove later
                        raise CodecError('Hashed CREF not found! Please call David')

                    child_items.add(obj)

        items = child_items

    # Only send the root object once
    obj_set.discard(root_obj)

    container_structure = _pack_container(root_obj, obj_set)
    log.debug('pack_structure: Packing Complete!')

    serialized = container_structure.SerializeToString()
    return serialized

def _pack_container(head, objects):
    """
    Helper for the sender to pack message content into a container in order
    """
    log.debug('_pack_container: Packing container head and object_keys!')
    # An unwrapped GPB Structure message to put stuff into!
    cs = object_utils.get_gpb_class_from_type_id(STRUCTURE_TYPE)()


    cs.head.key = head.key

    cs.head.type.object_id =  head.type.object_id
    cs.head.type.version =  head.type.version

    cs.head.isleaf = head.isleaf
    cs.head.value = head.value

    for item in objects:

        se = cs.items.add()

        # Can not set the pointer directly... must set the components
        se.key = item.key
        se.isleaf = item.isleaf
        se.type.object_id = item.type.object_id
        se.type.version = item.type.version

        # @TODO - How can we measure memory usage here to make sure this is the okay?
        se.value = item.value # Let python's object manager keep track of the pointer to the big things!


    log.debug('_pack_container: Packed container!')
    return cs

def unpack_structure(serialized_container):
    """
    Take a serialized container object and load a repository with its contents
    """
    log.debug('unpack_structure: Unpacking Structure!')
    head, obj_dict = _unpack_container(serialized_container)

    assert len(obj_dict) > 0, 'There should be objects in the container!'

    repo = repository.Repository()

    repo.index_hash.update(obj_dict)

    # Load the object and set it as the workspace root
    root_obj = repo._load_element(head)
    repo.root_object = root_obj

    repo.branch(nickname='master')

    # Create a commit to record the state when the message arrived
    cref = repo.commit(comment='Message for you Sir!')


    # Now load the rest of the linked objects - down to the leaf nodes.
    repo._load_links(root_obj)

    log.debug('unpack_structure: returning root_obj:'+str(root_obj))

    return root_obj



def _unpack_container(serialized_container):
    """
    Helper for the receiver for unpacking message content
    Returns the head object and items as wrapped structure elements
    """

    log.debug('_unpack_container: Unpacking Container')
    # An unwrapped GPB Structure message to put stuff into!
    cs = object_utils.get_gpb_class_from_type_id(STRUCTURE_TYPE)()

    try:
        cs.ParseFromString(serialized_container)
    except decoder._DecodeError, de:
        log.debug('Received invalid content - decode error: "%s"' % str(de))
        raise CodecError('Could not decode message content as a GPB container structure!')

    # Return arguments
    obj_dict={}

    head = gpb_wrapper.StructureElement(cs.head)
    obj_dict[head.key] = head


    for se in cs.items:
        wse = gpb_wrapper.StructureElement(se)

        obj_dict[wse.key] = wse

    log.debug('_unpack_container: returning head:\n' +str(head))
    log.debug('_unpack_container: returning dictionary of objects:\n'+str(obj_dict))

    return head, obj_dict