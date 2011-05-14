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

def validate(data_resource_url):
    """
    @brief update a data resource
    @retval big table
    """

    try:

        #prepare to parse!
        lexer = lex(module=Lexer())
        parser = yacc(module=Parser(), write_tables=0, debug=False)

        #fetch file
        fullurl = data_resource_url + ".das"
        webfile = urllib.urlopen(fullurl)
        dasfile = webfile.read()
        webfile.close()

        #crunch it!
        parsed_das = parser.parse(dasfile)


    #url doesn't exist
    except IOError, e1:
        print ("Couldn't fetch '%s'" % fullurl)
        
    #bad data
    except ParseException, e2:
        print ("Content of '%s' didn't parse" % fullurl)

    print "\n\nResults of parsing this URL:\n%s" % fullurl
    print "\n  Found these sections:"
    for k in parsed_das.keys():
        print "    %s" % k
        
        for k2, v2 in parsed_das[k].iteritems():
            print "      %s (%s): %s" % (k2, v2['TYPE'], str(v2['VALUE']))
        print

    print "\n"

if __name__ == "__main__":
    validate(sys.argv[1])
