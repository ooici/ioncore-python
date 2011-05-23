

import ion.agents.instrumentagents.helper_NMEA0183 as NMEA


# Completely valid GPGGA string
testNMEA = '$GPGGA,051950.00,3532.2080,N,12348.0348,W,1,09,07.9,0005.9,M,0042.9,M,0.0,0000*52'
parseNMEA = NMEA.NMEAString(testNMEA)
print '\nTest valid GPGGA MNEA string: %s ' % testNMEA
print parseNMEA.GetNMEAData()

# Completely valid dummy string
testNMEA = '$XXXXX,0'
parseNMEA = NMEA.NMEAString(testNMEA)
print '\nTest valid XXXXX (dummy) MNEA string: %s ' % testNMEA
print parseNMEA.GetNMEAData()

# Invalid GPGGA string checksum
testNMEA = '$GPGGA,051950.00,3532.2080,N,12348.0348,W,1,09,07.9,0005.9,M,0042.9,M,0.0,0000*F2'
parseNMEA = NMEA.NMEAString(testNMEA)
print '\nTest GPGGA with bad checksum: %s ' % testNMEA
print parseNMEA.GetNMEAData()

