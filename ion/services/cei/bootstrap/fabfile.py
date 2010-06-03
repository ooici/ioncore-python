from __future__ import with_statement
import os
import sys
from fabric.api import env, run, local, put, cd, hide

env.user = os.environ.get('FAB_USER') or "ubuntu"
try:
    env.key_filename=os.environ['FAB_KEY']
except KeyError:
    print "ERROR. Please run => 'export FAB_KEY=/path/to/ec2/private-keypair-file'"
    sys.exit(1)
#env.hosts = [""]

def bootstrap():
    update()
    install_chef()
    put_chef_data()
    run_chef_solo()

def update():
    with hide('stdout'):
        run("sudo apt-get -q update")

def install_chef():
    run("sudo apt-get install -y ruby-dev libopenssl-ruby rubygems")
    run("sudo gem install chef ohai --no-ri --no-rdoc --source http://gems.opscode.com --source http://gems.rubyforge.org")
    run("sudo ln -s /var/lib/gems/1.8/bin/chef-solo /usr/local/bin/")
    run("sudo ln -s /var/lib/gems/1.8/bin/ohai /usr/local/bin/")

def put_chef_data():
    run("sudo mkdir /opt/chef && sudo chown ubuntu:ubuntu /opt/chef")
    # checkout the latest cookbooks:
    run("sudo apt-get install -y git-core")
    run("git clone http://github.com/clemesha-ooi/ooi-cookbooks.git /opt/chef/cookbooks")
    # put the role and config files:
    put("chefconf.rb", "/opt/chef/")
    put("chefroles.json", "/opt/chef/")

def run_chef_solo():
    run("chef-solo -l debug -c /opt/chef/chefconf.rb -j /opt/chef/chefroles.json")

