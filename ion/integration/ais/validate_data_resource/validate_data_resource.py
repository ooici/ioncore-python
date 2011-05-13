#!/usr/bin/env python

"""
@file ion/integration/ais/manageDataResource/manageDataResource.py
@author Ian Katz
@brief The worker class that implements data source URL validation
"""



import urllib

from ply.lex import lex
from ply.yacc import yacc


import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer

from ion.core.exception import ReceivedApplicationError, ReceivedContainerError
from ion.core.messaging.message_client import MessageClient
from ion.core.object import object_utils

from ion.integration.eoi.validation.cdm_validation_service import CdmValidationClient

from ion.integration.ais.ais_object_identifiers import AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE, \
                                                       VALIDATE_DATASOURCE_REQ, \
                                                       VALIDATE_DATASOURCE_RSP



class ParseException(Exception):
    pass

# lexer and parser from Python Ply
class Lexer(object):
    #regex's for the tokens we expect to read
    t_OPEN        = r"\{"
    t_CLOSE       = r"\}"
    t_SEMI        = r";"
    t_NAME        = r'[a-zA-Z_][a-zA-Z0-9_]*'
    t_LITERAL     = r'"[^"]*"'
    t_ignore      = ' \t'

    #numbers that can be negative and/or decimal
    def t_NUMBER(self, t):
        r"-?\d+(\.\d+)?"
        t.value = float(t.value)
        return t

    # Define a rule so we can track line numbers
    def t_newline(self, t):
        r'\n+'
        t.lexer.lineno += len(t.value)

    tokens = "OPEN CLOSE SEMI NAME LITERAL NUMBER".split()

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
        "lineitem : NAME NAME meat SEMI"""
        ret = {p[2] : {"TYPE" : p[1], "VALUE" : p[3]}}
        p[0] = ret


    def p_meat(self, p):
        """meat : LITERAL
              |   NUMBER"""

        if type(0.0) == type(p[1]):
            p[0] = p[1]       # use the number as-is
        else:
            p[0] = p[1][1:-1] # strip quotes from literal


    def p_error(self, p):
        raise Exception(p)

    tokens = Lexer.tokens





class ValidateDataResource(object):

    def __init__(self, ais):
        log.debug('Validatedataresource.__init__()')
        self.mc  = ais.mc
        self.vc  = CdmValidationClient(proc=ais)

    def _equalInputTypes(self, ais_req_msg, some_casref, desired_type):
        test_msg = ais_req_msg.CreateObject(desired_type)
        return (type(test_msg) == type(some_casref))


    @defer.inlineCallbacks
    def validate(self, msg_wrapped):
        """
        @brief update a data resource
        @param msg GPB, FIXME/1,
        @GPB{Input,FIXME,1}
        @GPB{Returns,FIXME,1}
        @retval success
        """
        msg = msg_wrapped.message_parameters_reference # checking was taken care of by client
        try:
            # Check only the type received and linked object types. All fields are
            #strongly typed in google protocol buffers!
            if not self._equalInputTypes(msg_wrapped, msg, VALIDATE_DATASOURCE_REQ):
                errtext = "ValidateDataResource.validate(): " + \
                    "Expected ValidateDataResourceReqMsg type, got " + str(msg)
                log.info(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                Response.error_num =  Response.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)

            if not (msg.IsFieldSet("data_resource_url")):

                errtext = "ValidateDataResource.validate(): " + \
                    "required fields not provided (data_resource_url)"
                log.info(errtext)
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)

                Response.error_num =  Response.ResponseCodes.BAD_REQUEST
                Response.error_str =  errtext
                defer.returnValue(Response)


            cdm_result = yield self.vc.validate(msg.data_resource_url)
            
            if not cdm_result.response_type == content.ResponseType.PASS:
                Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)
                
                cr = cdm_result.ResponseType
                why = {cr.NONE: 'No response',
                       cr.PASS: 'Validation Passed!',
                       cr.CDM_FAILURE: 'CDM (time-axis) validation failed',
                       cr.CF_FAILURE: 'CF compliance failed x%d' % content.cf_error_count,
                       cr.ERROR: "'Other' error: %s" % content.err_msg,
                       }[cdm_result.response_type]


                errtext = "ValidateDataResource.validate(): INVALID: %s " % why
                more_out  = " :: cf_output: %s :: cdm_output: %s :: err_msg: %s " % (content.cf_output, content.cdm_output, content.err_msg)

                errtext = errtext + more_out

                log.info(errtext)
                Response.error_num =  Response.ResponseCodes.INTERNAL_SERVER_ERROR
                Response.error_str =  errtext
                defer.returnValue(Response)







            #prepare to parse!
            lexer = lex(module=Lexer())
            parser = yacc(module=Parser(), write_tables=0, debug=False)

            #fetch file
            fullurl = msg.data_resource_url + ".das"
            webfile = urllib.urlopen(fullurl)
            dasfile = webfile.read()
            webfile.close()

            #crunch it!
            parsed_das = parser.parse(dasfile)


        #url doesn't exist
        except IOError, e1:
            my_msg = "ValidateDataResource.validate(): couldn't fetch '%s'" % fullurl
            log.info(my_msg)
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)

            Response.error_num =  Response.ResponseCodes.NOT_FOUND
            Response.error_str =  my_msg

            defer.returnValue(Response)

        #bad data
        except ParseException, e2:
            my_msg = "ValidateDataResource.validate(): content of '%s' didn't parse" % fullurl
            log.info(my_msg)
            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)

            Response.error_num =  Response.ResponseCodes.INTERNAL_SERVER_ERROR
            Response.error_str =  my_msg

            defer.returnValue(Response)

        #something else???
        except ReceivedApplicationError, ex:
            log.info('ValidateDataResource.validate(): Error: %s' %ex)

            Response = yield self.mc.create_instance(AIS_RESPONSE_ERROR_TYPE)

            Response.error_num =  ex.msg_content.MessageResponseCode
            Response.error_str =  "ValidateDataResource.validate(): Error from lower-level service: " + \
                ex.msg_content.MessageResponseBody

            defer.returnValue(Response)

        log.info("SUCCESSFUL SO FAR")
        Response = yield self.mc.create_instance(AIS_RESPONSE_MSG_TYPE)
        Response.result = 200
        Response.message_parameters_reference.add()
        Response.message_parameters_reference[0] = Response.CreateObject(VALIDATE_DATASOURCE_RSP)
        self._populateResult(Response.message_parameters_reference[0].dataResourceSummary, parsed_das)
        Response.message_parameters_reference[0].cdmResponse = cdm_result
        defer.returnValue(Response)



    def _populateResult(self, out_msg, das):
        if das.has_key("NC_GLOBAL"):
            g = das["NC_GLOBAL"]
            
            if g.has_key("title"):        out_msg.title = g["title"]["VALUE"]
            if g.has_key("institution"):  out_msg.institution = g["institution"]["VALUE"]
            if g.has_key("source_data"):  out_msg.source = g["source_data"]["VALUE"]
            if g.has_key("references"):   out_msg.references = g["references"]["VALUE"]
            if g.has_key("comment"):      out_msg.comment  = g["comment"]["VALUE"]

            if g.has_key("start_date") and g.has_key("start_time"):
                tmp = g["start_date"]["VALUE"] + " " + g["start_time"]["VALUE"]
                out_msg.ion_time_coverage_start = tmp

            if g.has_key("stop_date") and g.has_key("stop_time"):
                tmp = g["stop_date"]["VALUE"] + " " + g["stop_time"]["VALUE"]
                out_msg.ion_time_coverage_end = tmp

            if g.has_key("southernmost_latitude"):
                out_msg.ion_geospatial_lat_min = g["southernmost_latitude"]["VALUE"]

            if g.has_key("northernmost_latitude"):
                out_msg.ion_geospatial_lat_max = g["northernmost_latitude"]["VALUE"]

            if g.has_key("westernmost_longitude"):
                out_msg.ion_geospatial_lon_min = g["westernmost_longitude"]["VALUE"]

            if g.has_key("easternmost_longitude"):
                out_msg.ion_geospatial_lon_max = g["easternmost_longitude"]["VALUE"]

