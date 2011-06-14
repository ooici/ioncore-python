# Simple NMEA test
# Simple verification of NMEA parsing routines and simulators

import ion.util.procutils as pu
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import ion.agents.instrumentagents.helper_NMEA0183 as NMEA

# Completely valid GPGGA string

log.info ('Verify NMEA parsing with known good GPGGA sentence:')
testNMEA = '$GPGGA,051950.00,3532.2080,N,12348.0348,W,1,09,07.9,0005.9,M,0042.9,M,0.0,0000*52'
parseNMEA = NMEA.NMEAString(testNMEA)
print '\nTest valid GPGGA MNEA string: %s ' % testNMEA
print parseNMEA.GetNMEAData()

# Completely valid dummy string
log.info ('Verify NMEA parsing with defined dummy setence:')
testNMEA = '$XXXXX,0'
parseNMEA = NMEA.NMEAString(testNMEA)
print '\nTest valid XXXXX (dummy) MNEA string: %s ' % testNMEA
print parseNMEA.GetNMEAData()

# Invalid GPGGA string checksum
log.info ('Verify correct NMEA behavior when passed a bad NMEA checksum:')
testNMEA = '$GPGGA,051950.00,3532.2080,N,12348.0348,W,1,09,07.9,0005.9,M,0042.9,M,0.0,0000*F2'
parseNMEA = NMEA.NMEAString(testNMEA)
print '\nTest GPGGA with bad checksum: %s ' % testNMEA
print parseNMEA.GetNMEAData()

# SELECT ONE SIMULATOR
#   - Comment out the simulator not being used

#from ion.agents.instrumentagents.simulators.sim_NMEA0183_liveSFBay \
#   import NMEA0183SimliveSFBay as sim
#log.info ('Using SF BAY LIVE AIS DATA simulator')

from ion.agents.instrumentagents.simulators.sim_NMEA0183_preplanned \
    import NMEA0183SimPrePlanned as sim
log.info ('Using PREPLANNED ROUTE GPS Simulator')

log.info ('----- Launching simulator:  ' + sim.WHICHSIM)
_sim = sim()
if _sim.IsSimOK():
    log.info ('----- Simulator launched.')
    