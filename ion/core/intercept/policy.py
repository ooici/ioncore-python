#!/usr/bin/env python

"""
@file ion/core/intercept/policy.py
@author Michael Meisinger
@brief Policy checking interceptor
"""

from twisted.internet import defer
from zope.interface import implements, Interface

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from ion.core.intercept.interceptor import EnvelopeInterceptor
import ion.util.procutils as pu

from ion.core.process.cprocess import Invocation

import time

from ion.util.config import Config

CONF = ioninit.config(__name__)

def construct_policy_lists(policydb):
    thedict = {}
    try:
        for policy_entry in policydb:
            role, action, resource = policy_entry
            service, opname = action.split('.', 1)
            assert role in ('ANONYMOUS', 'AUTHENTICATED', 'OWNER', 'ADMIN')

            if role == 'ADMIN':
                role_set = set(['ADMIN'])
            elif role == 'OWNER':
                role_set = set(['OWNER', 'ADMIN'])
            elif role == 'AUTHENTICATED':
                role_set = set(['AUTHENTICATED', 'OWNER', 'ADMIN']) 
            else:           
                role_set = set(['ANONYMOUS', 'AUTHENTICATED', 'OWNER', 'ADMIN'])

            service_dict = thedict.setdefault(service, {})
            op_set = service_dict.setdefault(opname, set())
            op_set.update(role_set)

    except Exception, ex:
        log.exception('----- POLICY INIT ERROR -----')
        raise ex
    return thedict

policydb_filename = ioninit.adjust_dir(CONF.getValue('policydecisionpointdb'))
policy_dictionary = construct_policy_lists(Config(policydb_filename).getObject())

def construct_admin_list(adminrolelist):
    thelist = []
    for role_entry in adminrolelist:
        subject = role_entry
        role_dict = {'subject': subject, 'ooid': None}
        thelist.append(role_dict);
    return thelist

adminroledb_filename = ioninit.adjust_dir(CONF.getValue('adminroledb'))
admin_role_list = construct_admin_list(Config(adminroledb_filename).getObject())

def subject_has_admin_role(subject):
    for role_entry in admin_role_list:
        if role_entry['subject'] == subject:
            return True
    return False

def user_has_admin_role(ooid):
    for role_entry in admin_role_list:
        if role_entry['ooid'] == ooid:
            return True
        else:
            return False

def map_ooid_to_subject(subject,ooid):
    for role_entry in admin_role_list:
        if role_entry['subject'] == subject:
            role_entry['ooid'] = ooid
            return

class PolicyInterceptor(EnvelopeInterceptor):
    def before(self, invocation):
        msg = invocation.content
        return self.is_authorized(msg, invocation)

    def after(self, invocation):
        return invocation
        # msg = invocation.message
        # return self.is_authorized(msg, invocation)

    def is_authorized(self, msg, invocation):
        """
        Policy enforcement method which implements the functionality
        conceptualized as the policy decision point (PDP).  This method
        will take the specified user id, convert it into a role.  A search
        will then be performed on the global policy_dictionary to determine if
        the user has the appropriate authority to access the specified
        resource via the specified action. A final check is made to determine
        if the user's authentication has expired.
        The following rules are applied to determine authority:
        - If there are no policy tuple entries for service, or no policy
        tuple entries for the specified role, the action is assumed to be allowed.
        - Else, there is a policy tuple for this service:operation.  A check
        is made to ensure the user role is equal to or greater than the
        required role.
        Role precedence from lower to higher is:
            ANONYMOUS, AUTHORIZED, OWNER, ADMIN
        @param msg: message content from invocation
        @param invocation: invocation object passed on interceptor stack.
        @return: invocation object indicating status of authority check
        """

        # Reject improperly defined messages
        if not 'user-id' in msg:
            log.info("Policy Interceptor: Rejecting improperly defined message missing user-id [%s]." % str(msg))
            invocation.drop(note='Error: no user-id defined in message header!', code=Invocation.CODE_BAD_REQUEST)
            return invocation
        if not 'expiry' in msg:
            log.info("Policy Interceptor: Rejecting improperly defined message missing expiry [%s]." % str(msg))
            invocation.drop(note='Error: no expiry defined in message header!', code=Invocation.CODE_BAD_REQUEST)
            return invocation
        if not 'receiver' in msg:
            log.info("Policy Interceptor: Rejecting improperly defined message missing receiver [%s]." % str(msg))
            invocation.drop(note='Error: no receiver defined in message header!', code=Invocation.CODE_BAD_REQUEST)
            return invocation
        if not 'op'in msg:
            log.info("Policy Interceptor: Rejecting improperly defined message missing op [%s]." % str(msg))
            invocation.drop(note='Error: no op defined in message header!', code=Invocation.CODE_BAD_REQUEST)
            return invocation
        
        user_id = msg['user-id']
        expirystr = msg['expiry']
        
        if not type(expirystr) is str:
            log.info("Policy Interceptor: Rejecting improperly defined message with bad expiry [%s]." % str(expirystr))
            invocation.drop(note='Error: expiry improperly defined in message header!', code=Invocation.CODE_BAD_REQUEST)
            return invocation

        try:
            expiry = int(expirystr)
        except ValueError, ex:
            log.info("Policy Interceptor: Rejecting improperly defined message with bad expiry [%s]." % str(expirystr))
            invocation.drop(note='Error: expiry improperly defined in message header!', code=Invocation.CODE_BAD_REQUEST)
            return invocation
            
        rcvr = msg['receiver']
        service = rcvr.rsplit('.',1)[-1]

        operation = msg['op']

        log.info('Policy Interceptor: Authorization request for service [%s] operation [%s] resource [%s] user_id [%s] expiry [%s]' % (service, operation, '*', user_id, expiry))
        if service in policy_dictionary:
            role = 'ANONYMOUS'
            # TODO figure out mechanism to map user id to role
            if user_id != None and user_id != 'ANONYMOUS':
                if user_has_admin_role(user_id) :
                    log.info('Policy Interceptor: Using ADMIN role.')
                    role = 'ADMIN'
                else:
                    log.info('Policy Interceptor: Using AUTHENTICATED role.')
                    role = 'AUTHENTICATED'
            else:
                log.info('Policy Interceptor: Using ANONYMOUS role.')

            service_list = policy_dictionary[service]
            # TODO figure out how to handle non-wildcard resource ids
            if operation in service_list:
                operation_entry = service_list[operation]
                log.info('Policy Interceptor: Policy tuple [%s]' % str(operation_entry))
                if role in operation_entry:
                    log.info('Policy Interceptor: Authentication matches')
                else:
                    log.info('Policy Interceptor: Authentication failed')
                    invocation.drop(note='Not authorized', code=Invocation.CODE_UNAUTHORIZED)
                    return invocation
        else:
            log.info('Policy Interceptor: service not in policy dictionary.')

        expiry_time = int(expiry)
        if (expiry_time > 0):
            current_time = time.time()
        
            if current_time > expiry_time:
                log.info('Policy Interceptor: Current time [%s] exceeds expiry [%s]. Returning Not Authorized.' % (str(current_time), expiry))
                invocation.drop(note='Authentication expired', code=Invocation.CODE_UNAUTHORIZED)
                return invocation

        log.info('Policy Interceptor: Returning Authorized.')
        return invocation
