
import os
import shutil

from ion.core import env
from ion.core.env import args


def init_command(name=None, path='.'):
    """
    Create a ion container environment to hold settings and other data
    files.

    --name: name of directory to be created
        Example: ion-admin init --name myenvironment

    --path: optional file system path to put environment directory
        Example: ion-admin init --name myenvironment --path /some/path

    """
    envroot = os.path.join(os.path.abspath(path), name)
    if os.path.exists(envroot):
        raise args.ArgumentError("The path %s already exists." % (envroot,))

    os.mkdir(envroot)
    orig_tree = os.path.join(os.path.abspath(env.__path__[0]), 'res')
    dest_tree = os.path.join(envroot, 'res')

    shutil.copytree(orig_tree, dest_tree)

    print "ion env %s created at %s" % (name, envroot,)

def help_command(**options):
    """
    Ion container admin tool.

    For creating ion container environments (but destroying isn't
    implemented yet, sorry :/)
    """
    print "\n".join(args.available_help(env.commands))

