#!/usr/bin/env python
"""
@file ion/services/dm/url_manipulation.py
@brief DM routines for manipulating URLs
@author Paul Hubbard
@date 6/4/10
"""

import re
import string
import urlparse
import logging
import os.path

def rewrite_url(dsUrl, newHostname='localhost'):
    """
    @brief Given a DAP URL, presume that we host it locally and rewrite it
    to reflect same. Changes hostname to localhost, removes any port,
    rewrites path to be just root. Used by the cache front end, to change
    canonical URLs into local-only URLs.
    @param dsUrl Original URL to rewrite
    @param newHostname Default is localhost, TCP name of server
    @retval String with rewritten URL.
    @see ooidx.test.test_rewrite_url for the unit tests for this.
    """

    ml = urlparse.urlsplit(dsUrl)
    # Replace the hostname with localhost
    chg_host = ml._replace(netloc=newHostname)
    # Remove all path components
    chg_path = chg_host._replace(path=os.path.basename(chg_host.path))
    return chg_path.geturl()

def generate_filename(dataset_url):
    """
    @brief Given a URL, generate a local filesystem name for same. Used by
    persister for write and cache for purge operations. It's a stinky hack, really.
    @param dataset_url Original URL
    @retval Local filename
    @todo Complete refactor - directories, etc
    """
    basicName = os.path.basename(dataset_url)

    #file name safe characters
    datasetid = ''.join([char for char in basicName if char in
            (string.letters + string.digits + "_-.")])

    """
    Relative paths work on both mac and EC2, duh.
    @bug Hardwired relative path
    @todo Move path to configuration file
    """
    return '../../dap_server/data/' + datasetid

def base_dap_url(src_url):
    """
    Given a DAP URL with optional parameters, return the base URL.
    The DAP URL itself is used as a key for many indices in the OOIDX
    (is a dataset cached, for example) so this is quite important.

    Current implementation uses regular expressions.

    @see test_baseUrl.py
    @param src_url Source URL, with DAP suffixes (DDS, DAS, DODS, etc)
    @retval Base URL, or None if invalid
    @bug Needs to recognize Ferret functions and return URL plus expression.
    """

    # This covers normal DAP URLs, but fails on base URLs such as http://127.0.0.1:8001/etopo120.cdf
    mset = re.search('(http|https)://([^/]+)(.+)(\.d(a|d)s|\.dods|\.asc(ii)*)(\?.+)*',
                    src_url)
    if mset == None:
#        logging.debug('Checking for match with base URL')
        # This regex works on just plain DAP URLs - dds/das/dods optional
        mset = re.search('(http|https)://([^/]+)(.+)(\.dds|\.das|\.dods|\.asc(i)*)*(\?.+)*', src_url)
        if mset == None:
            logging.warning('No URL match in "%s" % src_url')
            return None
        try:
            return mset.group(1) + '://' + mset.group(2) + mset.group(3)
        except:
            logging.exception('DAP URL does not match expected pattern!')
            return None
    try:
        return mset.group(1) + '://' + mset.group(2) + mset.group(3)
    except:
        logging.error('DAP URL does not match expected pattern!')
        return None
