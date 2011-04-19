

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core import ioninit
from ion.core.cc.shell import control
from ion.core.object import object_utils
from ion.core.pack import app_supervisor
from ion.core.process.process import ProcessDesc
from ion.integration.eoi.agent.java_agent_wrapper import JavaAgentWrapperClient

# --- CC Application interface ---


# Functions required
@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):
    '''
    Required function:  Invoked during the startup of this app
    '''
    jaw_proc = [
        {'name':'JavaAgentWrapper',
         'module':'ion.integration.eoi.agent.java_agent_wrapper',
         'class':'JavaAgentWrapper',
        }, ]

    appsup_desc = ProcessDesc(name='app-supervisor-' + app_definition.name,
                              module=app_supervisor.__name__,
                              spawnargs={'spawn-procs':jaw_proc})
    supid = yield appsup_desc.spawn()

    res = (supid.full, [appsup_desc])
    defer.returnValue(res)



    # Step 1: Create the supervisor
    #appsup = yield ioninit.container_instance.create_supervisor()

    # Step 2: Spawn child processes
    #b_proc_list = yield defer.maybeDeferred(_bootstrap_procs)
    #for (desc, args, kwargs) in b_proc_list:
    #    yield appsup.spawn_child(desc, *args, **kwargs)


    # Step 3: Add bootstrap objects to the shell
    #b_obj_list = yield defer.maybeDeferred(_bootstrap_objects)
    #b_obj_list_str = ''
    #for (id, obj) in b_obj_list:
    #    control.add_term_name(id, obj)
    #    b_obj_list_str += "\n'%s' = %s" % (str(id), str(obj))
    #print ''
    #print ''
    #print '================================================================='
    #print 'Added Bootstrap Objects:' + b_obj_list_str
    #print '================================================================='
    #print ''
    #print ''

    #res = (appsup.id, [appsup])
    #defer.returnValue(res)


@defer.inlineCallbacks
def stop(container, state):
    '''
    Required function:  Invoked during the termination of this app
    '''
    supdesc = state[0]
    log.info("Stopping EOIAgent")
    yield supdesc.terminate()


def _bootstrap_procs(*args, **kw):
    '''
    Defines a list of tuples where the first entry is a process description
    object, and the second and third items are args and kwargs which will
    be passed to the supervisor to spawn the process description via
    supervisor.spawn_child()
    '''
    # Step 1: Define boostrap process descriptions and any additional arguments for supervisor.spawn_child()
    java_wrapper_agent_args   = []
    java_wrapper_agent_kwargs = {}
#    java_wrapper_agent_kwargs = {'activate':False}
    java_wrapper_agent_desc  = ProcessDesc(name      = 'JavaWrapperAgent',
                                           module    = 'ion.integration.eoi.agent.java_agent_wrapper',
                                           procclass = 'JavaAgentWrapper',
                                           spawnargs = {})

    # Step 2: Return bootstrap procs as a tuple (process description then args)
    app_procs = [
                  (java_wrapper_agent_desc, java_wrapper_agent_args, java_wrapper_agent_kwargs)
                ]
    return app_procs


@defer.inlineCallbacks
def _bootstrap_objects(*args, **kw):
    '''
    Defines a list of tuples where the first entry is the string identifier 
    of the object to be accessible from the python console, and the second
    element is the object instance known by that identifier.
    '''
    # Step 1: Define bootstrap objects and ids
    client_id = 'eoiclient'
    client_ob = yield JavaAgentWrapperClient()
    
    # Step 2: Return bootstrap objects as a tuple (id then object)
    app_obs = [
                (client_id, client_ob)
              ]
    defer.returnValue(app_obs)
    
    
    
