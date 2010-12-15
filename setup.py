#!/usr/bin/env python

"""
@file setup.py
@author Paul Hubbard
@author Michael Meisinger
@date 5/2/10
@brief setup file for OOI LCA architecture prototype project.
"""

setupdict = {
    'name' : 'ioncore-python',
    'version' : '0.2.0',
    'description' : 'OOI LCA architecture prototype',
    'url': 'https://github.com/ooici/ioncore-python',
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
    setupdict['test_suite'] = 'ion'
    setupdict['install_requires'] = ['Twisted', 'magnet', 'pycassa', 'numpy',
                                     'Paste', 'Pydap', 'simplejson', 'httplib2',
                                     'pydap.handlers.netcdf','pydap.handlers.nca',
                                     'pydap.responses.netcdf',
                                     'msgpack-python','gviz_api.py']
    setupdict['include_package_data'] = True
    setup(**setupdict)

except ImportError:
    from distutils.core import setup
    setupdict['packages'] = ['ioncore-python']
    setup(**setupdict)
