#/usr/bin/env python

"""
@file ion/services/dm/inventory/ncml_generator.py
@author Paul Hubbard
@author Matt Rodriguez
@date 4/29/11
@brief For each dataset in the inventory, create a corresponding NcML file and
sync with remove server. Some tricky code for running a process and noting its
exit with a deferred.
"""

# File template. The filename and 'location' are just the GUID.
# Note the %s for string substitution.
file_template = """<?xml version="1.0" encoding="UTF-8"?>\n<netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2" location="ooici:%s"/>
"""

from os import path, environ, listdir, remove
import fnmatch
import os

from twisted.internet import defer
from ion.util.os_process import OSProcess

import ion.util.ionlog
from ion.core import ioninit

# Globals and config file variables
log = ion.util.ionlog.getLogger(__name__)
CONF = ioninit.config(__name__)
RSYNC_CMD = CONF['rsync']

def create_ncml(id_ref, filepath=""):
    """
    @brief for a given idref, generate an NcML file in the filepath directory
    @param filepath Output directory, defaults to current working directory
    @param id_ref idref object from which we pull GUID
    @retval File contents, as a string, or None if error
    """

    full_filename = path.join(filepath, id_ref + '.ncml')
    log.debug('Generating NcML file %s' % full_filename)
    try:
        fh = open(full_filename, 'w')
        fh.write(file_template % id_ref)
        fh.close()
    except IOError:
        log.exception('Error writing NcML file')
        return None

    return file_template % id_ref

def check_for_ncml_files(local_filepath):
    """
    Check for ncml files on disk.
    
    Returns True if any ncml files in the given directory, False if no files
        or an error is raised.
    """
    log.debug('checking for ncml files in %s' % local_filepath)

    try:
        allfiles = listdir(local_filepath)
        ncml_files = []
        for file in allfiles:
            if fnmatch.fnmatch(file, '*.ncml'):
                ncml_files.append(file)
    except IOError:
        log.exception('Error searching %s for ncml files' % local_filepath)
        return False

    if len(ncml_files) > 0:
        return True

    return False


def clear_ncml_files(local_filepath):
    """
    Clear ncml files on disk.
    """
    log.debug('clearing ncml files in %s' % local_filepath)

    try:
        allfiles = listdir(local_filepath)
        ncml_files = []
        for file in allfiles:
            if fnmatch.fnmatch(file, '*.ncml'):
                fpath = path.join(local_filepath, file)
                log.debug('Removing file: %s' % fpath)
                remove(fpath)
    except IOError:
        log.exception('Error searching %s for ncml files' % local_filepath)
        return False

    if len(ncml_files) > 0:
        return True

    return False



def rsync_ncml(local_filepath, server_url):
    """
    @brief Method to perform a bidirectional sync with a remote server,
    probably via rsync, unison or similar. Should be called after generating all
    local ncml files.
    @param local_filepath Local directory for writing ncml file(s)
    @param server_url rsync URL of the server
    @param ssh_key_filename the filename of the private key
    @retval Deferred that will callback when rsync exits, or errback if rsync fails
    """
    
    args = ['-r', '--perms', '--include', '"*.ncml"',
            '-v', '-h', '--delete', local_filepath + '/', server_url]


    log.debug("rsync command %s " % (RSYNC_CMD,))
    
    rp = OSProcess(binary=RSYNC_CMD, spawnargs=args, env=environ.data)
    log.debug('Command is "%s"'% ' '.join(args))
    return rp.spawn()
    


@defer.inlineCallbacks
def do_complete_rsync(local_ncml_path, server_url):
    """
    Orchestration routine to tie it all together plus cleanup at the end.
    Needs the inlineCallbacks to serialise.
    """
 
  
    ssh_cmd = "".join(("ssh -o StrictHostKeyChecking=no "))
    os.environ["RSYNC_RSH"] =  ssh_cmd
    yield rsync_ncml(local_ncml_path, server_url)
    del os.environ["RSYNC_RSH"]


    # Delete the keys from the file system
    #unlink(skey)
    #unlink(pkey)



