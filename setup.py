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
    'version' : '0.3.0', #VERSION,
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
    setupdict['install_requires'] = ['Twisted', 'carrot', 'txamqp', 'pycassa', 'numpy',
                                     'Paste', 'Pydap', 'simplejson', 'httplib2',
                                     'pydap.handlers.netcdf','pydap.handlers.nca',
                                     'pydap.responses.netcdf',
                                     'msgpack-python','gviz_api.py',
                                     'nimboss','txrabbitmq', 'Telephus', 'pyrods-irods>=2.4.1']
    setupdict['include_package_data'] = True
    setup(**setupdict)

except ImportError:
    from distutils.core import setup
    setupdict['packages'] = ['lcaarch']
    setup(**setupdict)
