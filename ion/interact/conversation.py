#!/usr/bin/env python

"""
@file ion/interact/conversation.py
@author Michael Meisinger
@brief classes for using conversations and conversation types (aka protocols,
    interaction patterns)
"""

from twisted.python.reflect import namedAny
from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.exception import ConversationError
from ion.util.state_object import FSMFactory, StateObject, BasicStates
import ion.util.procutils as pu

CONF = ioninit.config(__name__)
CF_basic_conv_types = CONF['basic_conv_types']

# Conversation type id for no conversation use.
CONV_TYPE_NONE = "none"

class IConversationType(Interface):
    """
    Interface for all conversation type instances
    """
    def new_conversation():
        pass

class IConversation(Interface):
    """
    Interface for all conversation instances
    """

class ConversationType(object):
    """
    @brief Represents a conversation type. Also known as protocol, interaction
        pattern, session type. Defines ID and roles of conversation.
        Acts as factory for the Conversation instances of a specific type.
    """
    implements(IConversationType)

    # Role id for the default initator of this conv
    DEFAULT_ROLE_INITIATOR = None

    # Role id for the default counterparty of this conv
    DEFAULT_ROLE_PARTICIPANT = None

    # List of state ids for conversation final states
    FINAL_STATES = ()

    def __init__(self, id):
        """
        @param id    Unique registry identifier of a conversation type
        """
        self.id = id

    def new_conversation(self, **kwargs):
        raise NotImplementedError("Not implemented")


class Conversation(object):
    """
    @brief An instance of a conversation type. Identifies the entities by name
    that bind to roles.
    """
    implements(IConversation)

    def __init__(self, conv_type, conv_id):
        """
        Creates a new conversation instance.

        @param conv_id    Unique registry identifier of a conversation
        @param conv_type  ConversationType instance
        """
        self.conv_id = conv_id
        self.conv_type = conv_type
        self.protocol = conv_type.id
        self.role_bindings = {}
        self.local_role = None
        self.local_process = None
        self.local_fsm = None
        # Holder for a Deferred for blocking (RPC style) send/receive
        self.blocking_deferred = None
        # Marks a timeout in the conversation processing
        self.timeout = None
        self.conv_log = []

    def bind_role_local(self, role_id, process):
        self.bind_role(role_id, process.id)

        self.local_role = role_id
        self.local_process = process

        # Create an instance of the local role ConversationRole/StateObject
        role_spec = self.conv_type.roles[role_id]
        self.local_fsm = role_spec.role_class()
        self.local_fsm.local_process = process

    def bind_role(self, role_id, process_id):
        """
        @brief Binds a process to a role id
        """
        assert not role_id in self.role_bindings, "Cannot bind role %s twice" % role_id

        self.role_bindings[role_id] = process_id

    def get_conv_log_str(self):
        res = "CONV_LOG[type=%s, id=%s, state=%s, @process=%s, #messages=%s:\n" % (
            self.protocol, self.conv_id, self.local_fsm._get_state(), self.local_process.proc_name, len(self.conv_log))
        for msg_rec in self.conv_log:
            (ts, mtype, cstate, mhdrs) = msg_rec
            hstr = "%s -> %s %s:%s:%s; uid=%s, status=%s" % (mhdrs.get('sender',None),
                    mhdrs.get('receiver',None), mhdrs.get('protocol',None),
                    mhdrs.get('performative',None), mhdrs.get('op',None),
                    mhdrs.get('user-id',None), mhdrs.get('status',None))
            mstr = " %d %s: %s >> %s\n" % (ts, mtype, hstr, cstate)
            res = res + mstr
        res = res + "]"
        return res

    def __str__(self):
        return "Conversation(%s)" % self.__dict__

class RoleSpec(object):
    """
    @brief Spec for a conversation role
    """
    def __init__(self, role_id, role_class):
        self.role_id = role_id
        self.role_class = role_class

#class RoleBinding(object):
#    """
#    @brief Binds a process to a role in a conversation instance.
#    """
#    def __init__(self, role_id, process=None, process_id=None):
#        self.role_id = role_id
#        self.process = process
#        if self.process:
#            self.process_id = self.process.id
#        else:
#            self.process_id = process_id

class ConversationRole(StateObject):
    """
    @brief A conversation as seen from one participant (=role binding).
        Encapsulates a FSM that keeps track of the state of the conversation
        of the participant.
    """
    def __init__(self):
        StateObject.__init__(self)
        fsm = self.factory.create_fsm(self)
        self._so_set_fsm(fsm)

    def _so_process(self, event, *args, **kwargs):
        log.debug("Processing Conversation event='%s' in state='%s'" % (event,self._get_state()))
        d = StateObject._so_process(self, event, *args, **kwargs)
        return d

    def error(self, *args, **kwargs):
        log.error("Conversation ERROR: Exception %r %r" % (args, kwargs))

    def unexpected(self, *args, **kwargs):
        log.error("Conversation ERROR: UNEXPECTED MSG")

    def timeout(self, *args, **kwargs):
        log.error("Conversation ERROR: TIMEOUT")

class ConversationTypeSpec(object):
    """
    Represents a conversation type specification. Base class for specific
    specification languages, such as Scribble, MSC etc.
    """

class ConversationTypeFSMFactory(FSMFactory):
    """
    A factory for instantiating conversation type FSMs.
    If there are only two participants to a conversation, the same FSM can be
    used (with different action behavior) for the state of the participant
    conversations.
    """

    def create_fsm(self, target, memory=None):
        fsm = FSMFactory.create_fsm(self, target, memory)
        fsm.post_action = True
        return fsm

    def _create_action_func(self, target, action):
        """
        @retval a function with a closure with the action name
        """
        def action_target(fsm):
            return target(action, fsm)
        return action_target


class ConversationManager(object):
    """
    @brief Manages conversation types within a container
    """

    # @todo CHANGE: Conversation ID counter
    convIdCnt = 0

    def __init__(self):
        # All available conversation types registry
        # Dict conv_type_id -> class name of Conversation subclass
        self.conv_type_registry = dict(**CF_basic_conv_types)

        # Dict of ConversationType instances
        self.conv_types = {}

        for (ctid,ctcls) in self.conv_type_registry.iteritems():
            ct_inst = self.load_conversation_type(ctid, ctcls)
            self.conv_types[ctid] = ct_inst

        log.debug("Loaded and instantiated %s conversation types: %s" % (
                    len(self.conv_types),self.conv_types.keys()))

    def load_conversation_type(self, ct_id, ct_cls_name):
        ct_class = namedAny(ct_cls_name)
        if not IConversationType.implementedBy(ct_class):
            raise ConversationError("ConversationType id=%s classname=%s does not implement IConversationType" % (ct_id, ct_cls_name))

        ct_inst = ct_class(id=ct_id)
        return ct_inst

    def get_conversation_type(self, conv_type_id):
        ct_inst = self.conv_types.get(conv_type_id, None)
        if ct_inst:
            return ct_inst

        # Trying to load again
        ct_class_name = self.conv_type_registry.get(conv_type_id, None)
        if not ct_class_name:
            raise ConversationError("ConversationType %s not registered" % conv_type_id)

        ct_inst = self.load_conversation_type(conv_type_id, ct_class_name)
        self.conv_types[conv_type_id] = ct_inst
        return ct_inst

    def create_conversation_id(self, prefix=''):
        # Returns a new unique conversation id
        self.convIdCnt += 1
        convid = str(prefix) + "#" + str(self.convIdCnt)
        return convid

    def new_conversation(self, conv_type_id, conv_id=None):
        ct_inst = self.get_conversation_type(conv_type_id)
        conv_id = conv_id or self.create_conversation_id()

        conv_inst = ct_inst.new_conversation(conv_type=ct_inst, conv_id=conv_id)
        if not IConversation.providedBy(conv_inst):
            raise ConversationError("Conversation instance %r from ConvType id=%s does not provide IConversation" % (conv_inst, conv_type_id))

        return conv_inst

conv_mgr_instance = ConversationManager()

class ProcessConversationManager(object):
    """
    @brief Oversees a set of conversations, e.g. within a process instance
    """

    def __init__(self, process):
        self.process = process
        self.conversations = {}
        self.conv_mgr = conv_mgr_instance

    def msg_send(self, message):
        """
        @brief Trigger the FSM for a to-be-sent message and delegate all checking
            to the callback action function
        @param message An in-memory standard message object
        """
        conv = self.get_conversation(message['headers']['conv-id'])
        perf = message['performative']
        if conv and conv.local_fsm:
            log.debug("msg_send(): Processing performative '%s'" % perf)
            return conv.local_fsm._so_process(perf, message)
        else:
            log.debug("msg_send(): NO FSM. Ignoring performative '%s'" % perf)

    def msg_received(self, message):
        """
        @brief Trigger the FSM for a received message and delegate all processing
            to the callback action function
        @param message An in-memory standard message object
        """
        #log.debug("msg_received(): %s" % message)
        conv = message['conversation']
        perf = message['performative']
        #log.debug("msg_received(): Processing performative '%s'" % perf)
        return conv.local_fsm._so_process(perf, message)

    def create_conversation_id(self):
        return self.conv_mgr.create_conversation_id(prefix=self.process.id.full)

    def new_conversation(self, conv_type_id, conv_id=None):
        conv_id = conv_id or self.create_conversation_id()
        conv_inst = self.conv_mgr.new_conversation(conv_type_id, conv_id)
        self.conversations[conv_inst.conv_id] = conv_inst
        return conv_inst

    def get_conversation(self, conv_id):
        return self.conversations.get(conv_id, None)

    def get_or_create_conversation(self, conv_id, message, initiator=False):
        """
        @brief Gets cached Conversation instance by conv-id header or creates
            new instance for for type by protocol header.
        @param conv_id the conversation id extracted from a message
        @param message the standard message callback object
        @param initiator True of this message is being sent, False if received
        """
        conv = self.conversations.get(conv_id, None)

        # If not existing, create new Conversation instance based on protocol header
        if not conv:
            conv_type = message['headers'].get('protocol', 'generic')

            log.debug("[%s] NEW local conversation from conv-id=%s: type=%s" % (
                    self.process.proc_name, conv_id, conv_type))
            conv = self.new_conversation(conv_type, conv_id)

            # Bind roles
            sender = message['headers'].get('sender', None)
            if initiator:
                conv.bind_role_local(conv.conv_type.DEFAULT_ROLE_INITIATOR, self.process)
                conv.bind_role(conv.conv_type.DEFAULT_ROLE_PARTICIPANT, sender)
                log.debug("Binding roles initiator(local)=%s, participant=%s" % (self.process.id, sender))
            else:
                conv.bind_role(conv.conv_type.DEFAULT_ROLE_INITIATOR, sender)
                conv.bind_role_local(conv.conv_type.DEFAULT_ROLE_PARTICIPANT, self.process)
                log.debug("Binding roles initiator=%s, participant(local)=%s" % (sender, self.process.id))

        return conv

    def log_conv_message(self, conv, message, msgtype):
        # Tuple of Timestamp (MS), type, message
        if conv is None:
            return
        mhdrs = message.get('headers',{}).copy()
        if 'content' in mhdrs:
            del mhdrs['content']
        msg_rec = (pu.currenttime_ms(), msgtype, conv.local_fsm._get_state(), mhdrs)
        conv.conv_log.append(msg_rec)

    def check_conversation_state(self, conv):
        """
        @brief Check a conversation state after an event (send, receive) for
            final and error state.
        """
        if conv is None:
            return
        conv_id = conv.conv_id
        #log.debug("check_conversation_state(), conv=%s, conv_id=%s, state=%s" % (conv, conv_id, conv.local_fsm._get_state()))
        # Check for final state
        if conv.local_fsm._get_state() in conv.conv_type.FINAL_STATES:
            del self.conversations[conv_id]
            log.info("Conversation FINAL: id=%s. Active conversations: %s" % (
                conv_id, len(self.conversations)))
            log.info("Conversation FINAL log:\n%s" % (conv.get_conv_log_str()))

        # Create a tombstone for later messages and timeouts with this conv_id

        # GC tombstones and conversations
