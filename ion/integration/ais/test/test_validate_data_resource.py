#!/usr/bin/env python

"""
@file ion/integration/ais/test/test_validate_data_resource.py
@test ion.integration.app_integration_service
@author Ian Katz
"""

from twisted.trial import unittest

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
import ion.util.procutils as pu

from twisted.internet import defer

from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient
from ion.core.exception import ReceivedApplicationError

from ion.test.iontest import IonTestCase

from ion.integration.ais.app_integration_service import AppIntegrationServiceClient

from ion.core.messaging.message_client import MessageClient

from ion.integration.ais.ais_object_identifiers import AIS_RESPONSE_MSG_TYPE, \
                                                       AIS_REQUEST_MSG_TYPE, \
                                                       AIS_RESPONSE_ERROR_TYPE, \
                                                       VALIDATE_DATASOURCE_REQ, \
                                                       VALIDATE_DATASOURCE_RSP


class AISValidateDataResourceTest(IonTestCase):
   
    """
    Testing Application Integration Service.
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        self.dispatcher_id = None

        services = [
            {
                'name':'app_integration',
                'module':'ion.integration.ais.app_integration_service',
                'class':'AppIntegrationService'
            },
            ]

        log.debug('AppIntegrationTest.setUp(): spawning processes')
        sup = yield self._spawn_processes(services)
        log.debug('AppIntegrationTest.setUp(): spawned processes')

        self.sup = sup

        self.aisc = AppIntegrationServiceClient(proc=sup)

        self.mc    = MessageClient(proc=sup)


    @defer.inlineCallbacks
    def tearDown(self):
        log.info('Tearing Down Test Container')


        yield self._shutdown_processes()
        yield self._stop_container()
        log.info("Successfully tore down test container")





    @defer.inlineCallbacks
    def test_validateDataResource_BadInput(self):
        """
        run through the validate code without proper input
        """

        log.info("Trying to call validateDataResource with the wrong GPB")
        validate_req_msg  = yield self.mc.create_instance(VALIDATE_DATASOURCE_REQ)
        result            = yield self.aisc.validateDataResource(validate_req_msg)
        self.failUnlessEqual(result.MessageType, AIS_RESPONSE_ERROR_TYPE,
                             "validateDataResource accepted a GPB that was known to be the wrong type")

        log.info("Trying to call validateDataResource with an empty GPB")
        ais_req_msg = yield self.mc.create_instance(AIS_REQUEST_MSG_TYPE)
        validate_req_msg = ais_req_msg.CreateObject(VALIDATE_DATASOURCE_REQ)
        ais_req_msg.message_parameters_reference = validate_req_msg
        result_wrapped = yield self.aisc.validateDataResource(ais_req_msg)
        self.failUnlessEqual(result_wrapped.MessageType, AIS_RESPONSE_ERROR_TYPE,
                             "validateDataResource accepted a GPB without a data_resource_url")




    @defer.inlineCallbacks
    def test_validateDataResourcePositive(self):
        """
        @brief try to validate a sample data sources
        """
        r1 = yield self._validateDataResource("http://thredds1.pfeg.noaa.gov/thredds/dodsC/satellite/GR/ssta/1day")
        res = r1.dataResourceSummary
        
        self.failUnlessEqual(res.title, "Analysed foundation sea surface temperature, global")
        self.failUnlessEqual(res.references, "none")
        self.failUnlessEqual(res.source, "TMI-REMSS,AMSRE-REMSS,AQUA-MODIS-OPBG,TERRA-MODIS-OPBG")
        self.failUnlessEqual(res.institution, "Remote Sensing Systems")
        self.failUnlessEqual(res.ion_time_coverage_start, "2011-04-23 00:00:00 UTC")
        self.failUnlessEqual(res.ion_time_coverage_end, "2011-04-27 23:59:59 UTC")
        self.failUnlessEqual(res.ion_geospatial_lat_min, -89.956055)
        self.failUnlessEqual(res.ion_geospatial_lat_max, 89.956055)
        self.failUnlessEqual(res.ion_geospatial_lon_min, -179.95605)
        self.failUnlessEqual(res.ion_geospatial_lon_max, 179.95605)

    @defer.inlineCallbacks
    def _validateDataResource(self, data_source_url):


        log.info("Creating and wrapping validation request")
        ais_req_msg  = yield self.mc.create_instance(AIS_REQUEST_MSG_TYPE)
        validate_req_msg  = ais_req_msg.CreateObject(VALIDATE_DATASOURCE_REQ)
        ais_req_msg.message_parameters_reference = validate_req_msg


        validate_req_msg.data_resource_url = data_source_url


        result_wrapped = yield self.aisc.validateDataResource(ais_req_msg)

        self.failUnlessEqual(result_wrapped.MessageType, AIS_RESPONSE_MSG_TYPE,
                             "validateDataResource had an internal failure")

        self.failUnlessEqual(200, result_wrapped.result, "validateDataResource didn't return 200 OK")
        self.failUnlessEqual(1, len(result_wrapped.message_parameters_reference),
                             "validateDataResource returned a GPB with too many 'message_parameters_reference's")

        result = result_wrapped.message_parameters_reference[0]

        defer.returnValue(result)


