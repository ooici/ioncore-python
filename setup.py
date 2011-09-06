#!/usr/bin/env python

"""
@file setup.py
@author Paul Hubbard
@author Michael Meisinger
@brief setup file for OOI ION Capability Container and Core Modules
@see http://peak.telecommunity.com/DevCenter/setuptools
"""

from itertools import chain, izip, repeat
import os

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

from ion import __version__ as version

# Workaround a bug in "package_data" that ignores directories. Build flattened list of all files.
excludeFiles = set(['res/config/ionlocal.config', 'res/logging/loglevelslocal.cfg'])
resFiles = [file for file in (os.path.join(path, file) for path,file in
            chain(*(izip(repeat(root), files) for root,dirs,files in os.walk('res'))))
            if file not in excludeFiles]

setup( name = 'ioncore',
       version = version,
       description = 'OOI ION Python Capability Container and Core Modules',
       url = 'http://www.oceanobservatories.org/spaces/display/CIDev/LCAARCH+Development+Project',
       download_url = 'http://ooici.net/releases',
       license = 'Apache 2.0',
       author = 'Michael Meisinger',
       author_email = 'mmeisinger@ucsd.edu',
       keywords = ['ooici','ioncore'],

       packages = find_packages() + ['twisted/plugins', 'res'],
       dependency_links = [
           'http://ooici.net/releases'
                          ],
       package_data = {
           'twisted.plugins': ['twisted/plugins/cc.py'],
           'ion': ['core/messaging/*.xml'],
           'res': resFiles
                      },
       test_suite = 'ion',
       install_requires = [
           'Twisted==10.2.0', 
           'txamqp==0.3',
           'simplejson==2.1.2', 
           'msgpack-python==015final',
           'gviz_api.py==1.7.0', 
           'Telephus==0.7-beta3.3', 
           'thrift==0.2.0', # thrift is a dependency of Telephus, which should be included there ideally
           'M2Crypto==0.21.1-pl1', # patched version to work with CentOS
           'ply==3.4',
           'pysnmp==4.1.16a',
           'pyserial==2.5',
           'hoover==0.5.2',
           'setproctitle==1.1.2',
           'ionproto>=0.3.39',
                          ],
       entry_points = {
                        'console_scripts': [
                            'cassandra-setup=ion.core.data.cassandra_schema_script:main',
                            'cassandra-teardown=ion.core.data.cassandra_teardown_script:main',
                            'dbmanhole=ion.ops.dbmanhole:main',
                            ],
                        },
       include_package_data = True,
       classifiers = [
           'Development Status :: 3 - Alpha',
           'Environment :: Console',
           'Intended Audience :: Developers',
           'License :: OSI Approved :: Apache Software License',
           'Operating System :: OS Independent',
           'Programming Language :: Python',
           'Topic :: Scientific/Engineering'
                     ]
     )
