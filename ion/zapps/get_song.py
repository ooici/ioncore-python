#!/usr/bin/env python

"""
@file ion/zapps/put_song.py
@author Matt Rodriguez
@brief simple app that tests the performance of the message stack
"""

import subprocess
from twisted.internet import defer

from ion.core.pack import app_supervisor
from ion.core.process.process import ProcessDesc, Process

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
CONF = ioninit.config(__name__)

from ion.core.object import object_utils
from ion.services.coi.resource_registry.resource_client import ResourceClient

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

BLOB_TYPE = object_utils.create_type_identifier(object_id=2540, version=1)


    
@defer.inlineCallbacks
def start(container, starttype, app_definition, *args, **kwargs):
    """
    This app retrieves a song from the resource_registry. It expects to receive
    a key argument. The key argument is passed in through kwargs.
    
    %bin/twistd -n cc -a sysname=sysname key=6FD6731B-C7BC-425B-B8A6-3D5254115925 res/apps/get_song.app
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
    key = kwargs['key'] 
    song = yield rc.get_instance(key)
    song_buffer = song.blob
    
    #I wish I could get tempfile to work :(
    filelocation ="/tmp/zz.mp3"
    f = open("/tmp/zz.mp3", "wb")
    f.write(song_buffer)
    f.close()
    subprocess.Popen(["/usr/bin/afplay", filelocation])
    
    defer.returnValue(res)

    
@defer.inlineCallbacks
def stop(container, state):
    
    supdesc = state[0]
    yield supdesc.terminate()