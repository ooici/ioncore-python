#!/usr/bin/env python

"""
@file setup.py
@author Paul Hubbard
@date 4/2/10
@brief setup file for OOI skeleton project. Modify me to suit!
"""

setupdict = {
    'name' : 'lcaarch',
    'version' : '0.0.2a',
    'description' : 'OOI LCA architecture framework',
    'url': 'http://www.oceanobservatories.org/spaces/display/CIDev/',
    'download_url' : 'http://ooici.net/packages',
    'license' : 'Apache 2.0',
    'author' : 'Michael Meisinger',
    'author_email' : 'mmeisinger@ucsd.edu',
    'keywords': ['ooci'],
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
    setupdict['install_requires'] = ['Twisted']
    setupdict['include_package_data'] = True
    setup(**setupdict)

except ImportError:
    from distutils.core import setup
    setupdict['packages'] = ['lcaarch']
    setup(**setupdict)
