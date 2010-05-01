==================================================
LCAARCH - OOI Release 1 LCA architecture prototype
==================================================

April 2010

This project defines the services of the OOI release 1 system with their
architectural dependencies.

This project and relies on selected external packages, such as Magnet, etc.

Get it with
::
    git clone git@amoeba.ucsd.edu:lcaarch.git

Usage
=====

(all subsequent steps assume start from lcaarch/ dir)

Start CC ("Magnet" Python Capability Container) shell with:
::
    twistd -n magnet -h amoeba.ucsd.edu

Start system by executing within the CC shell:
><>
    from ion.core import bootstrap
    bootstrap.start()

Alternatively from shell executing a script:
::
    twistd -n magnet -h amoeba.ucsd.edu res/scripts/bootstrap.py

Run trial test cases (recursively)
::
    trial ion.core
    trial ion.services.coi.test.test_resource_registry
    trial ion


Install the dependencies: Magnet (see Magnet's Readme)
======================================================
Recommendation:
    Create a virtualenv for installing Magnet and its dependencies.

Twisted Python
--------------
::
    easy_install twisted

txAMQP
------
::
    easy_install txamqp

carrot (use txamqp branch)
----------------------
::
    git clone git://amoeba.ucsd.edu/carrot.git
    (cd carrot; git checkout -b txamqp origin/txamqp)
    (cd carrot; python setup.py install)

Install the Magnet package:
---------------------------
Get the latest version of the repository, if you haven't already.
::
    git clone git://amoeba.ucsd.edu/magnet.git # no ooi credential
    # OR
    git clone git@amoeba.ucsd.edu:magnet.git # need ooi credential
    (cd magnet; git checkout -b space origin/space)
    (cd magnet; python setup.py install)


Note:
=====
This project dependes closely on magnet. Whenever you do a "git pull" on
this project, there is a chance that you need to update and install magnet
again (see above). Please review the branch logs for any cues.
