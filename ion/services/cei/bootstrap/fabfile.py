from __future__ import with_statement
import os
import sys
from fabric.api import env, run, local, put, cd, hide
from fabric.decorators import runs_once

def bootstrap(rolesfile=None):
    update()
    put_chef_data(rolesfile=rolesfile)
    run_chef_solo()

def bootstrap_cei(rolesfile=None):
    put_provisioner_secrets()
    bootstrap(rolesfile=rolesfile)

def put_provisioner_secrets():
    nimbus_key = os.environ.get('NIMBUS_KEY')
    nimbus_secret = os.environ.get('NIMBUS_SECRET')
    if not nimbus_key or not nimbus_secret:
        print "ERROR.  Please export NIMBUS_KEY and NIMBUS_SECRET"
        sys.exit(1)

    ec2_key = os.environ.get('AWS_ACCESS_KEY_ID')
    ec2_secret = os.environ.get('AWS_SECRET_ACCESS_KEY')
    if not ec2_key or not ec2_secret:
        print "ERROR.  Please export AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
        sys.exit(1)

    ensure_opt()
    run("sudo sh -c 'echo export NIMBUS_KEY=%s >> /opt/cei_environment'" % nimbus_key)
    run("sudo sh -c 'echo export AWS_ACCESS_KEY_ID=%s >> /opt/cei_environment'" % ec2_key)
    
    with hide('running'):
        run("sudo sh -c 'echo export NIMBUS_SECRET=%s >> /opt/cei_environment'" % nimbus_secret)
        run("sudo sh -c 'echo export AWS_SECRET_ACCESS_KEY=%s >> /opt/cei_environment'" % ec2_secret)

def update():
    with hide('stdout'):
        run("sudo apt-get -q update")

def install_chef():
    run("sudo apt-get install -y ruby-dev libopenssl-ruby rubygems")
    run("sudo gem install chef ohai --no-ri --no-rdoc --source http://gems.opscode.com --source http://gems.rubyforge.org")
    run("sudo ln -s /var/lib/gems/1.8/bin/chef-solo /usr/local/bin/")
    run("sudo ln -s /var/lib/gems/1.8/bin/ohai /usr/local/bin/")

@runs_once
def ensure_opt():
    run("if [ ! -d /opt ]; then sudo mkdir /opt; fi")
    
def put_chef_data(rolesfile=None):
    ensure_opt()
    run("if [ -d /opt/chef ]; then sudo rm -rf /opt/chef; fi")
    run("sudo mkdir /opt/chef && sudo chown ubuntu:ubuntu /opt/chef")
    # checkout the latest cookbooks:
    run("sudo apt-get install -y git-core")
    run("git clone http://github.com/clemesha-ooi/ooi-cookbooks.git /opt/chef/cookbooks")
    # put the role and config files:
    put("chefconf.rb", "/opt/chef/")
    if rolesfile:
        put(rolesfile, "/opt/chef/chefroles.json")
    else:
        put("chefroles.json", "/opt/chef/")
        

def run_chef_solo():
    run("sudo chef-solo -l debug -c /opt/chef/chefconf.rb -j /opt/chef/chefroles.json")

