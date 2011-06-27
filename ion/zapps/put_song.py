#!/usr/bin/env python

"""
@file ion/zapps/put_song.py
@author Matt Rodriguez
@brief simple app that tests the performance of the message stack
"""

from twisted.internet import defer

from ion.core.pack import app_supervisor
from ion.core.process.process import ProcessDesc, Process

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
CONF = ioninit.config(__name__)

from ion.core.object import object_utils
from ion.services.coi.resource_registry.resource_client import ResourceClient



BLOB_TYPE = object_utils.create_type_identifier(object_id=2540, version=1)


    
@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):
    """
    This expects an argument filename to be passed in. The filename
    is the location of the mp3 file.
   
    Here's how I run start the put_song app
    %bin/twistd --logfile=put_song.log --pidfile=put_song.pid -n cc -a sysname=sysname filename=/Users/mateo/Downloads/Wagner.mp3 res/apps/put_song.app 
    """
    services = []

    appsup_desc = ProcessDesc(name='app-supervisor-' + app_definition.name,
                              module=app_supervisor.__name__,
                              spawnargs={'spawn-procs':services})
    supid = yield appsup_desc.spawn()
    res = (supid.full, [appsup_desc])
    
    proc = Process()
    
    yield proc.initialize()
    yield proc.activate()
    rc = ResourceClient(proc)
    song = yield rc.create_instance(BLOB_TYPE, "WagnerSong", "Ride of the Valkyries")    
    
    if not 'filename' in kwargs:
        log.warn("The filename of the mp3 song is not passed into the app correctly!")
        log.warn("Please restart the app!")
        defer.returnValue(res)
            
    filename = kwargs['filename']
    
    try:
        f = open(filename, "rb")
    except IOError, ex:
        log.warn("Problem opening the file of the mp3.")
        log.warn("Please restart the app!")
        defer.returnValue(res)
        
    bytes_buffer = f.read()    
    song.blob = bytes_buffer
    print "putting song into registry"
    yield rc.put_instance(song, "Persisting song resource")
    
    reference = rc.reference_instance(song)
    print "Reference is ", reference
    #This works with the existing loglevels.cfg
    log.warn("Resource ID is: %s" % (reference,))
    
    



    defer.returnValue(res)

    
@defer.inlineCallbacks
def stop(container, state):
    
    supdesc = state[0]
    yield supdesc.terminate()