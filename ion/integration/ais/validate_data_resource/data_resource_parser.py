#!/usr/bin/env python

"""
@file ion/integration/ais/validate_data_resource/data_resource_parser.py
@author Ian Katz
@brief Classes to parse a url for its metadata 
"""



from ply.lex import lex
from ply.yacc import yacc


class ParseException(Exception):
    pass

# lexer and parser from Python Ply
class Lexer(object):
    #regex's for the tokens we expect to read
    t_OPEN        = r"\{"
    t_CLOSE       = r"\}"
    t_SEMI        = r";"
    t_COMMA       = r","
    #t_NAME        = r'[a-zA-Z_][a-zA-Z0-9_]*'
    t_LITERAL     = r'"[^"]*"'
    t_ignore      = ' \t'

    def t_NAME(self, t):
        r'[a-zA-Z_][a-zA-Z0-9_]*'
        if "NaN" == t.value:
            t.type = "NUMBER"
            t.value = float("NaN")
        #print t
        return t

    #numbers that can be negative and/or decimal
    def t_NUMBER(self, t):
        r"NaN|(-?\d+(\.\d+)?(E-?\d+)?)"
        #print t.value, "becomes", float(t.value)
        t.value = float(t.value)
        return t

    # Define a rule so we can track line numbers
    def t_newline(self, t):
        r'\n+'
        t.lexer.lineno += len(t.value)

    tokens = "OPEN CLOSE SEMI COMMA NUMBER NAME LITERAL".split()

    def t_error(self, t):
        raise ParseException(t)


class Parser(object):
    """parsing rules are contained in the comment to each function"""

    starting = "dasfile" # rule to start parsing on... default is first method

    def p_dasfile(self, p):
        "dasfile : NAME OPEN sections CLOSE"
        #  p[0]  : p[1] p[1] p[3]     p[4]    is how you read this

        output = {}
        #list of single-entry dictionaries for sections
        for asection in p[3]:
            for k, v in asection.iteritems():
                output[k] = v

        p[0] = output

    # recursive case for building a list... base case follows below
    def p_sections(self, p):
        "sections : section sections"
        p[2].append(p[1])
        p[0] = p[2]

    def p_sections_term(self, p):
        "sections : section"
        p[0] = [p[1]]


    def p_section(self, p):
        "section : NAME OPEN lineitems CLOSE"
        theitems = {}

        # lineitems is a list of single-entry dictionaries for lines... collapse it
        for aline in p[3]:
            for k, v in aline.iteritems():
                theitems[k] = v

        p[0] = {p[1] : theitems}


    # recursive case for building a list... base case follows below
    def p_lineitems(self, p):
        "lineitems : lineitem lineitems"
        p[2].append(p[1])
        p[0] = p[2]

    def p_lineitems_term(self, p):
        "lineitems : lineitem"
        p[0] = [p[1]]


    def p_lineitem(self, p):
        """lineitem : NAME NAME meat SEMI
                |   NAME NAME NAME SEMI """
        ret = {p[2] : {"TYPE" : p[1], "VALUE" : p[3]}}
        p[0] = ret


    def p_meat(self, p):
        """meat : LITERAL
              |   NUMBER
              |   numberlist"""

        if type(0.0) == type(p[1]):
            p[0] = p[1]       # use the number as-is
        else:
            p[0] = p[1][1:-1] # strip quotes from literal

    def p_numberlist(self, p):
        """numberlist : NUMBER COMMA numberlist"""
        p[3].append(p[1])
        p[0] = p[3]

    def p_numberlist_term(self, p):
        """numberlist : NUMBER"""
        p[0] = [p[1]]

    def p_error(self, p):
        raise Exception(p)

    tokens = Lexer.tokens



