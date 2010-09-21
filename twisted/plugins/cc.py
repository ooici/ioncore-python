
from twisted.application.service import ServiceMaker

CC = ServiceMaker(
        "ION CapabilityContainer",
        "ion.core.cc.service",
        "ION Capability Container",
        'cc')
