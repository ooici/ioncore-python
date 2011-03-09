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

import time

from ion.util.config import Config

CONF = ioninit.config(__name__)

def construct_policy_lists(policydb):
    thedict = {}
    try:
        for policy_entry in policydb:
            role, op, action = policy_entry
            service, opname = op.split('.', 1)
            if service in thedict:
                servicelist = thedict[service]
                servicelist.append(policy_entry)
            else:
                servicelist = [policy_entry]
                thedict[service] = servicelist
                
    except Exception, ex:
        log.exception('----- POLICY INIT ERROR -----')
        raise ex
    return thedict

policydb_filename = ioninit.adjust_dir(CONF.getValue('policydecisionpointdb'))
policy_dictionary = construct_policy_lists(Config(policydb_filename).getObject())

class PolicyInterceptor(EnvelopeInterceptor):
    def before(self, invocation):
        msg = invocation.content
        return self.is_authorized(msg, invocation)

    def after(self, invocation):
        msg = invocation.message
        return self.is_authorized(msg, invocation)

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
            ANONYMOUS, AUTHORIZED, OWNER
        @param msg: message content from invocation
        @param invocation: invocation object passed on interceptor stack.
        @return: invocation object indicating status of authority check
        """

        # Reject improperly defined messages
        if not 'user-id' in msg:
            log.info("Policy Interceptor: Rejecting improperly defined message missing user-id [%s]." % str(msg))
            invocation.drop('Error: no user-id defined in message header!')
            return invocation
        if not 'expiry' in msg:
            log.info("Policy Interceptor: Rejecting improperly defined message missing expiry [%s]." % str(msg))
            invocation.drop('Error: no expiry defined in message header!')
            return invocation
        if not 'receiver' in msg:
            log.info("Policy Interceptor: Rejecting improperly defined message missing receiver [%s]." % str(msg))
            invocation.drop('Error: no receiver defined in message header!')
            return invocation
        if not 'op'in msg:
            log.info("Policy Interceptor: Rejecting improperly defined message missing op [%s]." % str(msg))
            invocation.drop('Error: no op defined in message header!')
            return invocation
        
        user_id = msg['user-id']
        expirystr = msg['expiry']
        
        if not type(expirystr) is str:
            log.info("Policy Interceptor: Rejecting improperly defined message with bad expiry [%s]." % str(expirystr))
            invocation.drop('Error: expiry improperly defined in message header!')
            return invocation

        try:
            expiry = int(expirystr)
        except:
            log.info("Policy Interceptor: Rejecting improperly defined message with bad expiry [%s]." % str(expirystr))
            invocation.drop('Error: expiry improperly defined in message header!')
            return invocation
            
        rcvr = msg['receiver']
        substrs = rcvr.split('.')
        service = substrs[len(substrs) - 1]

        operation = msg['op']
        action = service + '.' + operation

        log.info('Policy Interceptor: Authorization request for service [%s] user_id [%s] action [%s] expiry [%s] resource [%s]' % (service, user_id, action, expiry, '*'))
        if service in policy_dictionary:
            role = 'ANONYMOUS'
            # TODO figure out mechanism to map user id to role
            if user_id != None and user_id != 'ANONYMOUS':
                log.info('Policy Interceptor: Using AUTHENTICATED role.')
                role = 'AUTHENTICATED'
            else:
                log.info('Policy Interceptor: Using ANONYMOUS role.')

            service_list = policy_dictionary[service]
            # TODO figure out how to handle non-wildcard resource ids
            for entry in service_list:
                log.info('Policy Interceptor: Policy tuple [%s]' % str(entry))
                entry_role, entry_action, entry_resource = entry
                if entry_action == action:
                    log.info('Policy Interceptor: Action matches tuple [%s]' % str(entry))
                    # TODO handle OWNER role
                    if entry_role == 'AUTHENTICATED' and role == 'ANONYMOUS':
                        log.info('Policy Interceptor: Role [%s] does not satisfy tuple [%s]. Returning Not Authorized.' % (role, str(entry)))
                        invocation.drop('Not authorized')
                        return invocation
        else:
            log.info('Policy Interceptor: service not in policy dictionary.')

        expiry_time = int(expiry)
        if (expiry_time > 0):
            current_time = time.time()
        
            if current_time > expiry_time:
                log.info('Policy Interceptor: Current time [%s] exceeds expiry [%s]. Returning Not Authorized.' % (str(current_time), expiry))
                invocation.drop('Authentication expired')
                return invocation

        log.info('Policy Interceptor: Returning Authorized.')
        return invocation
