#!/usr/bin/env python

"""
@file setup.py
@author Paul Hubbard
@author Michael Meisinger
@brief setup file for OOI ION Capability Container and Core Modules
@see http://peak.telecommunity.com/DevCenter/setuptools
"""

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

setup( name = 'ioncore',
       version = '0.4.4',
       description = 'OOI ION Python Capability Container and Core Modules',
       url = 'http://www.oceanobservatories.org/spaces/display/CIDev/LCAARCH+Development+Project',
       download_url = 'http://ooici.net/releases',
       license = 'Apache 2.0',
       author = 'Michael Meisinger',
       author_email = 'mmeisinger@ucsd.edu',
       keywords = ['ooici','ioncore'],

       packages = find_packages() + ['twisted/plugins'],
       dependency_links = [
           'http://ooici.net/releases'
                          ],
       package_data = {
           'twisted.plugins' : [
               'twisted/plugins/cc.py'
                               ]
                      },
       test_suite = 'ion',
       install_requires = [
           'Twisted==10.2.0', 
           'carrot==0.10.14-txamqp', 
           'txamqp==0.3',
           'simplejson==2.1.2', 
           'msgpack-python==015final',
           'gviz_api.py==1.7.0', 
           'Telephus==0.7-beta3.3', 
           'thrift==0.2.0', # thrift is a dependency of Telephus, which should be included there ideally
           'M2Crypto==0.21.1-pl1', # patched version to work with CentOS
           'ionproto>=0.3.13'
                          ],
       entry_points = {
                        'console_scripts': [
                            'ion-admin=ion.core.env.ion_admin:main',
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
