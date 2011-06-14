#!/usr/bin/env python

"""
@file ion/integration/ais/test/test_validate_data_resource.py
@test ion.integration.app_integration_service
@author Ian Katz
"""

#from twisted.trial import unittest

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

#from twisted.internet import defer

from ion.test.iontest import IonTestCase

from ion.integration.ais.validate_data_resource.parse_url_tester import validateUrl


class AISDataResourceParserTest(IonTestCase):
   
    """
    Testing Application Integration Service parser for data resource urls.
    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_parserOnlyPositive(self):

        toparse = "http://geoport.whoi.edu/thredds/dodsC/usgs/data0/rsignell/data/oceansites/OS_NTAS_2010_R_M-1.nc" + ".das"

        log.info("parsing %s", toparse)
        res = validateUrl(toparse)

        self.failUnlessEqual(True, res.has_key("NC_GLOBAL"), "NC global section not found, but we know its there")

        self.failUnlessEqual(True, res["NC_GLOBAL"].has_key("qc_flag_values"))
        self.failUnlessEqual([0,1,2,3,4], res["NC_GLOBAL"]["qc_flag_values"]["VALUE"])

        self.failUnlessEqual(True, res["NC_GLOBAL"].has_key("institution"))
        self.failUnlessEqual("WHOI", res["NC_GLOBAL"]["institution"]["VALUE"])
        
        self.failUnlessEqual(True, res["NC_GLOBAL"].has_key("FILL_FLAG"))
        self.failUnlessEqual(0, res["NC_GLOBAL"]["FILL_FLAG"]["VALUE"])
        

        self.failUnlessEqual(True, res.has_key("DODS_EXTRA"), "DODS_EXTRA section not found")
        
        self.failUnlessEqual(True, res["DODS_EXTRA"].has_key("Unlimited_Dimension"))
        self.failUnlessEqual("time", res["DODS_EXTRA"]["Unlimited_Dimension"]["VALUE"])

