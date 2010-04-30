from __future__ import with_statement
import os
import sys
from fabric.api import env, run, local, put, cd, hide

env.user = os.environ.get('FAB_USER') or "root"
try:
    env.key_filename=os.environ['FAB_KEY']
except KeyError:
    print "ERROR. Please run => 'export FAB_KEY=/path/to/ec2/private-keypair-file'"
    sys.exit(1)
#env.hosts = [""]

def bootstrap():
    update()
    run("apt-get install -y git-core")
    run("rm -rf /opt/chef/*") #clean up existing chef data on VM.
    run("git clone http://github.com/clemesha-ooi/ooi-cookbooks.git /opt/chef/cookbooks")
    putchefdata()
    runchefsolo()

def update():
    with hide('stdout'):
        run("apt-get -q update")

def putchefdata():
    put("chefconf.rb", "/opt/chef/")
    put("chefroles.json", "/opt/chef/")

def runchefsolo():
    run("chef-solo -l debug -c /opt/chef/chefconf.rb -j /opt/chef/chefroles.json")

