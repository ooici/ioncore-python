#!/usr/bin/env python

"""
@file setup.py
@author Paul Hubbard
@author Michael Meisinger
@date 5/2/10
@brief setup file for OOI LCA architecture prototype project.
"""

setupdict = {
    'name' : 'lcaarch',
    'version' : '0.2.0',
    'description' : 'OOI LCA architecture prototype',
    'url': 'http://www.oceanobservatories.org/spaces/display/CIDev/LCAARCH+Development+Project',
    'download_url' : 'http://ooici.net/packages',
    'license' : 'Apache 2.0',
    'author' : 'Michael Meisinger',
    'author_email' : 'mmeisinger@ucsd.edu',
    'keywords': ['ooci','lcar1'],
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
    setupdict['test_suite'] = 'lcaarch.test'
    setupdict['install_requires'] = ['Twisted >=10.1', 'magnet', 'pycassa', 'numpy',
                                     'Paste', 'Pydap>=3.0.rc.10', 'simplejson', 'httplib2',
                                     'pydap.handlers.netcdf >=0.4.9',
                                     'pydap.responses.netcdf >= 0.1.3']
    setupdict['include_package_data'] = True
    setup(**setupdict)

except ImportError:
    from distutils.core import setup
    setupdict['packages'] = ['lcaarch']
    setup(**setupdict)
