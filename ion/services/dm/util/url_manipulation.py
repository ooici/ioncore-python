#!/usr/bin/env python
"""
@file ion/services/dm/util/url_manipulation.py
@brief DM routines for manipulating URLs
@author Paul Hubbard
@date 6/4/10
"""

import re
import string
import urlparse
import logging
log = logging.getLogger(__name__)
import os.path
from ion.core import ioninit

# Read our configuration from the ion.config file.
config = ioninit.config(__name__)
LOCAL_DIR = config.getValue('local_dir', '../../dap_server/data/')
CACHE_HOSTNAME = config.getValue('cache_hostname', 'localhost')
CACHE_PORTNUM = config.getValue('cache_port', '80')

def rewrite_url(dsUrl, newHostname=CACHE_HOSTNAME):
    """
    @brief Given a DAP URL, presume that we host it locally and rewrite it
    to reflect same. Changes hostname to localhost, removes any port,
    rewrites path to be just root. Used by the cache front end, to change
    canonical URLs into local-only URLs.
    @param dsUrl Original URL to rewrite
    @param newHostname Default is localhost, TCP name of server
    @retval String with rewritten URL.
    @todo add CACHE_PORTNUM
    """

    ml = urlparse.urlsplit(dsUrl)
    # Replace the hostname with localhost
    # Remove all path components
    chg_path = urlparse.urlunsplit((ml[0],newHostname,
                                    ml[2].split('/')[-1],
                                    ml[3], ml[4]))
    return chg_path

def generate_filename(dataset_url, local_dir=LOCAL_DIR):
    """
    @brief Given a URL, generate a local filesystem name for same. Used by
    persister for write and cache for purge operations. It's a stinky hack, really.
    @param dataset_url Original URL
    @param local_dir Local directory to write to
    @retval Local filename
    @todo Complete refactor - directories, etc
    """
    if local_dir == None:
        local_dir = LOCAL_DIR

    assert(local_dir != None)
    assert(len(local_dir) > 0)

    if local_dir[-1:] != '/':
        local_dir = local_dir + '/'

    basicName = os.path.basename(dataset_url)

    #file name safe characters
    datasetid = ''.join([char for char in basicName if char in
            (string.letters + string.digits + "_-.")])

    return local_dir + datasetid

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
        # This regex works on just plain DAP URLs - dds/das/dods optional
        mset = re.search('(http|https)://([^/]+)(.+)(\.dds|\.das|\.dods|\.asc(i)*)*(\?.+)*', src_url)
        if mset == None:
            log.info('No URL match in "%s" % src_url')
            return None
        try:
            return mset.group(1) + '://' + mset.group(2) + mset.group(3)
        except IndexError, ie:
            log.exception('DAP URL does not match expected pattern!')
            raise ie

    try:
        return mset.group(1) + '://' + mset.group(2) + mset.group(3)
    except IndexError, ie:
        log.exception('DAP URL does not match expected pattern!')
        raise ie
