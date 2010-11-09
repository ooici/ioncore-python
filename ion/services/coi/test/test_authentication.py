#!/usr/bin/env python

"""
@file ion/services/coi/test/test_identity_registry.py
@author Roger Unwin
@brief test service for registering users
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)
from twisted.internet import defer
from twisted.trial import unittest

from ion.core.exception import ReceivedError
from ion.test.iontest import IonTestCase
from ion.services.coi.identity_registry import IdentityRegistryClient

import ion.services.coi.authentication

from ion.resources import coi_resource_descriptions

class AuthenticationTest(IonTestCase):
    """
    Testing client classes of User Registration
    """

    @defer.inlineCallbacks
    def setUp(self):
        """
        """
        yield self._start_container()

        services = [{'name':'identity_registry','module':'ion.services.coi.identity_registry','class':'IdentityRegistryService'}]
        supervisor = yield self._spawn_processes(services)

        self.identity_registry_client = IdentityRegistryClient(proc=supervisor)
        
    @defer.inlineCallbacks
    def tearDown(self):
        yield self.identity_registry_client.clear_identity_registry()
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_sign(self):
        """
        """


        user = coi_resource_descriptions.IdentityResource.create_new_resource()

        # initialize the user
        user.common_name = "Roger Unwin A13"
        user.country = "US"
        user.trust_provider = "ProtectNetwork"
        user.domain_component = "cilogon"
        user.certificate =  """-----BEGIN CERTIFICATE-----
MIIEMzCCAxugAwIBAgICBFIwDQYJKoZIhvcNAQEFBQAwajETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRsw
GQYDVQQDExJDSUxvZ29uIEJhc2ljIENBIDEwHhcNMTAxMTAxMTk1OTM1WhcNMTAxMTA1MjAwNDM1
WjBvMRMwEQYKCZImiZPyLGQBGRMDb3JnMRcwFQYKCZImiZPyLGQBGRMHY2lsb2dvbjELMAkGA1UE
BhMCVVMxFzAVBgNVBAoTDlByb3RlY3ROZXR3b3JrMRkwFwYDVQQDExBSb2dlciBVbndpbiBBMjU0
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAoD6nYLZ2+hBa3LtOn3P/RdOdfxghDD3o
Fa+0bwDyrdk0UTcajcjCT/A49T1zdmIPG1eRfD8I+9iE4YJbYVgu8NKmnVoBwyu29bL36dGpLIkT
FVKVuYQ98FXA0oOTK1dGmLqY4WuN+Od8T910qRVwMpb5doOtogvgyZr57sJxuXyNuRrRS/VbptOl
ltyf0x+nT+RxEL66zEK1g4sL7RIHPDKTAXdpaZ/uWDGK+XBxA01jV3gIKrJecwIoMb+YSSsGXOpp
YjTZuHtzay9ihjZAh5+Q+scLbmNJ/uMYdGFzEAwJ9m/+dlibeoZxKHpzFo6eFYxjXbvgF/89sqjs
fsxrvQIDAQABo4HdMIHaMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgSwMBMGA1UdJQQMMAoG
CCsGAQUFBwMCMBgGA1UdIAQRMA8wDQYLKwYBBAGCkTYBAgEwagYDVR0fBGMwYTAuoCygKoYoaHR0
cDovL2NybC5jaWxvZ29uLm9yZy9jaWxvZ29uLWJhc2ljLmNybDAvoC2gK4YpaHR0cDovL2NybC5k
b2Vncmlkcy5vcmcvY2lsb2dvbi1iYXNpYy5jcmwwHwYDVR0RBBgwFoEUaXRzYWdyZWVuMUB5YWhv
by5jb20wDQYJKoZIhvcNAQEFBQADggEBAIuoPx1EYLD+zkK42oOIzOn0e4RSPdqOwFx2fe5C+Xhm
IlGdlzhhuHE2lOUPYpPZ7p3lKtkacgh0WziuvthRuT+0vQeDv5T+CLsgb+a1vu4/3uMNmoDmpG6N
+f8VZEEezGleqEXGacFDvBF7VxrILVFS1B5pVnbd5PO4onrv4wX1D2nclDlyZrC0+932GGvBt0jb
smBUmBTomsb3dSaRNpZoylhUhf/RNHyUGZNxSadCryYyRvTEJyyTSxTD3JQf6wkPrjxJ0ipdaqLk
/Bu9Pue3g32Yl0suOTxaCfuG03nfbTlHQ6TgSH2O6AaL9Dsf48qfJl+DAoOdCr+Mu54nrZI=
-----END CERTIFICATE-----"""
        user.rsa_private_key = """-----BEGIN RSA PRIVATE KEY-----
MIIEpQIBAAKCAQEAoD6nYLZ2+hBa3LtOn3P/RdOdfxghDD3oFa+0bwDyrdk0UTcajcjCT/A49T1z
dmIPG1eRfD8I+9iE4YJbYVgu8NKmnVoBwyu29bL36dGpLIkTFVKVuYQ98FXA0oOTK1dGmLqY4WuN
+Od8T910qRVwMpb5doOtogvgyZr57sJxuXyNuRrRS/VbptOlltyf0x+nT+RxEL66zEK1g4sL7RIH
PDKTAXdpaZ/uWDGK+XBxA01jV3gIKrJecwIoMb+YSSsGXOppYjTZuHtzay9ihjZAh5+Q+scLbmNJ
/uMYdGFzEAwJ9m/+dlibeoZxKHpzFo6eFYxjXbvgF/89sqjsfsxrvQIDAQABAoIBAQCGCIH16gkQ
Vte5Y057Hwo5PKyy3trdo3ZZlVLlujRCZ7hT6jRivbaSKItrzY+jSJf8Nb2x4APCq7NR8LhAbwMs
WfYVDXEF762kS6MDx2Oqpaj5n88ukkdAnGmha36QtPqOyx0PB+iDdhRLtR9cQltLZW6Zy8BTF35Z
AzDknW2ESDALbte25aMb+eCUUidjmUEyMqHgyfhqQsZvYdA/rfNz1jbeS3UdohSatTAwiJW2c/xd
vVuHp43UKNPw31MDEXTSFvQZhT433DDpv9yCmVuVHPIcuPRxpcmHZDnZ04EVz4XN6mW6bX895zfE
a8WbtJqtqqRd/wEWTpweaQ8DY4B9AoGBANRJqwbWuz3q6maimQtusHcoscpmLgFbeKB2keFGzPhk
4xVwvdvkA/wp7EOpT0LsKoczfGm78vyE2QG+K4R1eg4LNf7mAi69NM6D+4KEW/xmeKP5955Lf7VO
TUaYCCriAUETRL9YuipZQ1vJHSQI6/jE+FVOfGJ1VN6te+QB5vKfAoGBAME9o93D6C3zSzBh7/7v
0I0MXnhXfDjE8+d9FQoVK1gIF1HxwiALncaMlErNiCGCTU/URA4nrFXvKDYHEVpd4HvkPTFHcWvJ
FauHv0aci4qrPnURTzHCDcA4A58ktJPtWWquLmQkFM1UXCY85ZFVYrEcMUhQ3ZnKNpRBIS7kfsAj
AoGBAKxbq6p/ycK56tquBYiMtGXq+n8UeyHK/KN74XGApIbAkscjpGLWPI4OE6/T1XDGgrkHCmpm
mSCBVBfgKUEAiLrCS3LLmNYN9MP/0MLlaDIDmMu59lvlfKjeDEvWwDrhCJenZ1fcWDpuAwyQu0I4
pC507hOFB+SA0wmA3WgAS1yNAoGBALA+xP8Vl+SY+qHFIXwWQ9LxThRaTm0EjSQ7u/23MxIWRxaw
9gn+LkeRngrfjGJrkpHVmsCCRLcX6kfkiFowNvcoQvt4GqVhAIeyxqzjSI4QA2YIhH9watQ/Amaa
tqwYlS4scRlaozJm16j0b7ju9JVujjBTuNl0SfVLtbUsJ8KbAoGASGStMLkpb1QjE9uicj0L2Koz
UcUFKZl8kWqYPTAfXBSL8IomEyBoOwR79pIAw2jWfA1YKMXZb3zXNoCMdhx/t+rTUFe0nPjgUAzh
tUYra2rz3KCjWXk76rQ24JL+tGSeHH2VRIGN5QBlm8/UUiGD0nZxUmEkP6VfWMFm/uESYWA=
-----END RSA PRIVATE KEY-----"""
        user.expiration_date = "Tue Jun 29 23:32:16 PDT 2010"




        user = yield self.identity_registry_client.register_user(user)
        ooi_id = user.reference()
        
        sign_message(self, 'this is a test', ooi_id)
        
        self.assertEqual(verify_message(self, 'this is a test', ooi_id), True)
