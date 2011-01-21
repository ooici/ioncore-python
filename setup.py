#!/usr/bin/env python

"""
@file setup.py
@author Paul Hubbard
@author Michael Meisinger
@brief setup file for OOI ION Capability Container and Core Modules
@see http://peak.telecommunity.com/DevCenter/setuptools
"""

#from ion.core.ionconst import VERSION

setupdict = {
    'name' : 'ioncore',
    'version' : '0.4.0', #VERSION,
    'description' : 'OOI ION Python Capability Container and Core Modules',
    'url': 'http://www.oceanobservatories.org/spaces/display/CIDev/LCAARCH+Development+Project',
    'download_url' : 'http://ooici.net/packages',
    'license' : 'Apache 2.0',
    'author' : 'Michael Meisinger',
    'author_email' : 'mmeisinger@ucsd.edu',
    'keywords': ['ooici','ioncore'],
    'classifiers' : [
    'Development Status :: 3 - Alpha',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Topic :: Scientific/Engineering'],
}

try:
    from setuptools import setup, find_packages
    setupdict['packages'] = find_packages()

    setupdict['dependency_links'] = ['http://ooici.net/packages']
    setupdict['packages'].extend(['twisted/plugins'])
    setupdict['test_suite'] = 'ion'

    setupdict['install_requires'] = ['Twisted==10.2.0', 'carrot==0.10.11-txamqp', 'txamqp==0.3',
                                     'simplejson==2.1.2', 'httplib2==0.6.0','msgpack-python==015final',
                                     'gviz_api.py==1.7.0','nimboss','txrabbitmq==0.4', 'Telephus==0.7-beta3.3', 
                                     'pyrods-irods==2.4.3', 'M2Crypto==0.20.2', 'ionproto==0.2.2', 'protobuf==2.3.0-p1']

    
    setupdict['include_package_data'] = True
    setup(**setupdict)

except ImportError:
    from distutils.core import setup
    setupdict['packages'] = ['ioncore']
    setup(**setupdict)
