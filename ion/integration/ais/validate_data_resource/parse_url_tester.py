#!/usr/bin/env python

"""
@file ion/integration/ais/validate_data_resource/parse_url_tester.py
@author Ian Katz
@brief Parse a url for its metadata and spit it to the command line
"""

import sys

import urllib


from ply.lex import lex
from ply.yacc import yacc

from data_resource_parser import Lexer, Parser, ParseException

def parseUrl(das_resource_url):
    """
    @brief validate a data resource
    @retval big table
    """

    #prepare to parse!
    lexer = lex(module=Lexer())
    parser = yacc(module=Parser(), write_tables=0, debug=False)
        

    #fetch file
    fullurl = das_resource_url
    webfile = urllib.urlopen(fullurl)
    dasfile = webfile.read()
    webfile.close()
    
    #crunch it!
    return parser.parse(dasfile, lexer=lexer)


def validateUrl(data_resource_url):
    """
    @brief validate a data resource
    @retval helpful output
    """

    try:

        parsed_das = parseUrl(data_resource_url)


    #url doesn't exist
    except IOError:
        print "Couldn't fetch '%s'" % data_resource_url
        return {}
        
    #bad data
    except ParseException:
        print "Content of '%s' didn't parse" % data_resource_url
        return {}

    print "\n\nResults of parsing this URL:\n%s" % data_resource_url
    print "\n  Found these sections:"
    for k in parsed_das.keys():
        print  ("    %s" % k)
        
        for k2, v2 in parsed_das[k].iteritems():
            print "      %s (%s): %s" % (k2, v2['TYPE'], str(v2['VALUE']))
        print 

    print "\n"

    return parsed_das

if __name__ == "__main__":
    validateUrl(sys.argv[1])
