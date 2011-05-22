

import ion.agents.instrumentagents.helper_NMEA0183 as NMEA


testNMEA = '$GPGGA,051950.00,3532.2080,N,12348.0348,W,1,09,07.9,0005.9,M,0042.9,M,0.0,0000*52'

parseNMEA = NMEA.NMEAString(testNMEA)

print parseNMEA.GetNMEAData()
