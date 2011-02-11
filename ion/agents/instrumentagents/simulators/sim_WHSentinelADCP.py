"""
Teledyne Worckhorse Sentinel ADCP Instrument Simulator - Provides basic
simulation of a Workhorse Sentinel ADCP instrument.  This is not part of the
product description, but rather is being used to help develop and test
instrument agents and drivers. This program can be fleshed out as needed.

@file ion/agents/instrumentagents/simulators/sim_WHSentinelADCP.py
@author Bill Bollenbacher
@date 10-19-10
"""

import logging
import ion.util.ionlog
import random
import math
import time
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import protocol
from twisted.internet import reactor
from twisted.internet import task
from twisted.internet import defer

from ion.agents.instrumentagents.simulators.Simulator_constants import portNumbers
from ion.agents.instrumentagents.simulators.Simulator_constants import NO_PORT_NUMBER_FOUND
from ion.agents.instrumentagents.simulators.Simulator_constants import NUMBER_OF_PORTS_AVAILABLE

INSTRUMENT_ID = "456"

class CmdPort(protocol.Protocol):
    
    def __init__(self):
        log.debug("CmdPort __init__")
        
    def connectionMade(self):
        """
        @brief A client has made a connection:
        """
        log.debug("CmdPort connectionMade")
        self.factory.connections.append(self)

    def dataReceived(self, data):
        if log.getEffectiveLevel() == logging.DEBUG:
            DataAsHex = ""
            for i in range(len(data)):
                if len(DataAsHex) > 0:
                    DataAsHex += ","
                DataAsHex += "{0:X}".format(ord(data[i]))
            log.debug("CmdPort dataReceived [%s] [%s]" % (data, DataAsHex))
        else:
            log.info("CmdPort dataReceived [%s]" % data)
        if data == "\x21\x00":
            log.debug("responding to 21")
            self.transport.write("\x21OK")
        if data == "\x22\x00":
            log.debug("responding to 22")
            self.transport.write("\x22OK")
            self.factory.parent.InstrumentRef.BreakReceived()
            
    def connectionLost(self, reason):
        #log.debug("CmdPort connectionLost - %s" % reason)
        self.factory.connections.remove(self)
        
class Instrument(protocol.Protocol):
    """
    The instrument protocol class. Simulate a Teledyne Worckhorse Sentinel ADCP
    by receiving commands over a TCP connection and responding as much as
    possible as would a Worckhorse Sentinel ADCP.  Instantiate two timers
    for handling autonomous mode (where the instrument sends data at intervals
    until stopped), and test commands (where the instrument sends 100 samples
    periodically).  In autonomous mode, the timer is started and stopped based
    on commands from the client.  In the case of test commands, the timer is
    started when the command is received, and then a counter is incremented
    for each sample, and when the counter reaches 100, the timer is stopped.
    """

    # The following are class static variables (USED AS CONSTANTS)
    prompt = '>'
    out_of_bounds_error = ' ERR 001:  PARAMETER OUT OF BOUNDS\r\n' + prompt
    numeral_expected_error = ' ERR 002:  NUMERAL EXPECTED\r\n' + prompt
    binary_expected_error = ' ERR 004:  BINARY NUMERAL EXPECTED\r\n' + prompt
    extra_parameters_error = ' ERR 005:  EXTRA PARAMETERS ENCOUNTERED\r\n' + prompt
    unrecognized_cmd_error = ' ERR 010:  UNRECOGNIZED COMMAND\r\n' + prompt
    bad_parameters_error = ' ERR :  Bad command parameters\r\n' + prompt
    set_to_factory = '[Parameters set to FACTORY defaults]\r\n' + prompt
    set_to_user = '[Parameters set to USER defaults]\r\n' + prompt
    Wakeup = '[BREAK Wakeup A]\r\n' + \
             'WorkHorse Broadband ADCP Version 16.30\r\n' + \
             'Teledyne RD Instruments (c) 1996-2007\r\n' + \
             'All Rights Reserved.' + prompt
    simple_commands = {
        'ck' : '[Parameters saved as USER defaults]\r\n' + prompt,
        'cz' : 'Powering Down\r\n' + prompt,
        'experton' : 'Expert Mode is ON\r\n' + prompt,
        'expertoff' : 'Expert Mode is OFF\r\n' + prompt,
        'ol' : '                               FEATURES\r\n' +
               '---------------------------------------------------------------------\r\n' +
               'Feature                                                     Installed\r\n' +
               '---------------------------------------------------------------------\r\n' +
               'Bottom Track                                                    Yes\r\n' +
               'Water Profile                                                   Yes\r\n' +
               'High Resolution Water Modes                                      No\r\n' +
               'Lowered ADCP                                                     No\r\n' +
               'Wave Gauge Acquisition                                           No\r\n' +
               'Shallow Bottom Mode                                              No\r\n' +
               'High Rate Pinging                                                No\n\r\n' +
               'See your technical manual or contact RDI for information on how to\r\n' +
               'install additional capability in your WorkHorse.\n\r\n' + prompt,
        '' : prompt,
    }
    testCommands = {
        'pa' : prompt,
        'pc1' : prompt,
        'pc2' : prompt,
        'ps0' : prompt,
        'ps3' : prompt,
        'tpr' : prompt,
    }

    def __init__(self):
        log.debug("instrument __init__")
        self.lc_testSampler = task.LoopingCall(self.testSampler)
        self.lc_autoSampler = task.LoopingCall(self.autoSampler)

        self.numTestSamples = 0  # variable to hold number of test samples taken
        self.maxTestSamples = 10 # maximumm number of test stamples to take
        self.testInterval = 1    # interval in seconds between samples in test commands (TT, etc.)
        self.autoInterval = 6    # interval in seconds between samples in autonomous mode
        self.testRunning = 'false'
        self.autoRunning = 'false'
        self.sample_cnt = 0

    def BreakReceived(self):
        self.transport.write(self.Wakeup)
        if self.autoRunning == 'true':
            log.info("Stopping auto samples")
            self.stopAutoSamples()
            self.autoRunning = 'false'
  
    def connectionMade(self):
        """
        @brief A client has made a connection: call factory to pass
        this instance, because the factory has the timer (LoopingCall).
        @param none
        @retval none
        """
        self.factory.connections.append(self)
        self.factory.parent.InstrumentRef = self


    def dataReceived(self, data):
        """
        @brief Data as been recieved from client. Determine what command was
        sent and attempt to respond as would an Worckhorse Sentinel ADCP.
        @param Data from client.
        @retval none
        """
        DataAsHex = ""
        for i in range(len(data)):
            if len(DataAsHex) > 0:
                DataAsHex += ","
            DataAsHex += "{0:X}".format(ord(data[i]))
        log.info("dataReceived() [%s] [%s]" % (data, DataAsHex))
        
        """
        If anything is received, and the Worckhorse Sentinel ADCP is in autonomous
        mode stop sending samples at the configured interval.
        """
        if self.autoRunning == 'true':
            log.info("Stopping auto samples")
            self.stopAutoSamples()
            self.autoRunning = 'false'
            
        # don't know how line was terminated, so remove whatever it was first
        data = data.strip("\r")  # strip off any CRs
        data = data.strip("\n")  # strip off any LFs
        if len(data) != 0:
            # echo the string like the ADCP does
            self.transport.write(data + "\r\n")

        # Strip off the extraneous whitespace, and convert to lower case.
        data = data.strip()
        data = data.lower()

        if len(data) == 0:
            """
            @note If zero length data received, probably just operator mashing
            on return key: just return the prompt (just as Worckhorse Sentinel
            ADCP would).
            """
            log.debug("no command seen, so sending just a prompt")
            self.transport.write(self.prompt)

        else:
            """
            @brief Partition the data into two parts (command, value)
            """
            command = data.strip('0123456789-')
            value = data.strip(command)
            log.debug("received command: [%s], value: [%s], length of value is %d" %(command, value, len(value)))

            if command in self.testCommands:
                """
                @note If a "testing" command is received, and the instrument is
                not already running a test, start sending samples
                at the configured test interval until the configured maximum
                number of test samples have been sent.
                """
                log.debug("Received test command: %s" %command)
                return   # test simulation not yet implemented for this instrument
                if self.testRunning == 'false':
                    log.debug("Starting test samples")
                    self.startTestSamples()
                    self.testRunning = 'true'
                    
            elif command == "cb":
                if not value.isdigit():
                    self.transport.write(self.bad_parameters_error)
                elif len(value) != 3:
                    self.transport.write(self.bad_parameters_error)
                else:
                    for i in range(3):
                        if i == 0:
                            if not self.checkValueRange(0, 8, int(value[i]), self.bad_parameters_error):
                                return
                        elif i == 1:
                            if not self.checkValueRange(1, 5, int(value[i]), self.bad_parameters_error):
                                return
                        elif not self.checkValueRange(1, 2, int(value[i]), self.bad_parameters_error):
                            return
                    self.transport.write(self.prompt)
                    
            elif command == "cf":
                if not value.isdigit():
                    self.transport.write(self.bad_parameters_error)
                elif len(value) != 5:
                    self.transport.write(self.bad_parameters_error)
                else:
                    for i in range(5):
                        if i == 2:
                            if not self.checkValueRange(0, 2, int(value[i]), self.bad_parameters_error):
                                return
                        elif not self.checkValueRange(0, 1, int(value[i]), self.bad_parameters_error):
                            return
                    self.transport.write(self.prompt)
                    
            elif command == "cr":
                if not value.isdigit():
                    self.transport.write(self.numeral_expected_error)
                elif value == "1":
                    self.transport.write(self.set_to_factory)
                elif value == "2":
                    self.transport.write(self.set_to_user)
                else:
                    self.transport.write(self.out_of_bounds_error)
                    
            elif command == "cs":    # start command
                """
                @note If start command is received, and the Worckhorse Sentinel ADCP is
                not already running a test or sending samples, start sending samples at the configured interval.
                """
                if len(value) != 0:
                    self.transport.write(self.extra_parameters_error)
                    return
                if self.testRunning == 'false' and self.autoRunning == 'false':
                    self.autoRunning = 'true'
                    # The factory handles the sending at intervals.
                    self.startAutoSamples()
                else:
                    # Currently we don't simulate auto/polled: we just handle the
                    # start/stop as if we're in auto mode (mode defaults to auto
                    # and doesn't change). But, in the future we might want to
                    # simulate the modes: wouldn't be hard.
                    self.transport.write(self.commands[command])
                    
            elif command == "ea":
                if value[0] == '-':
                    temp = value
                    value = temp.lstrip('-')
                if not value.isdigit():
                    self.transport.write(self.numeral_expected_error)
                elif len(value) > 5:
                    self.transport.write(self.extra_parameters_error)
                elif self.checkValueRange(0, 18000, int(value), self.out_of_bounds_error):
                    self.transport.write(self.prompt)
                    
            elif command == "ed":
                if not value.isdigit():
                    self.transport.write(self.numeral_expected_error)
                elif self.checkValueRange(0, 65535, int(value), self.out_of_bounds_error):
                    self.transport.write(self.prompt)
                    
            elif command == "es":
                if not value.isdigit():
                    self.transport.write(self.numeral_expected_error)
                elif self.checkValueRange(0, 40, int(value), self.out_of_bounds_error):
                    self.transport.write(self.prompt)
                    
            elif command == "ex":
                if not value.isdigit():
                    self.transport.write(self.binary_expected_error)
                elif len(value) > 5:
                    self.transport.write(self.extra_parameters_error)
                elif len(value) < 5:
                    self.transport.write(self.binary_expected_error)
                else:
                    for i in range(5):
                        if not self.checkValueRange(0, 1, int(value[i]), self.out_of_bounds_error):
                            return
                    self.transport.write(self.prompt)
                    
            elif command == "ez":
                if not value.isdigit():
                    self.transport.write(self.numeral_expected_error)
                elif len(value) > 7:
                    self.transport.write(self.extra_parameters_error)
                elif len(value) < 7:
                    self.transport.write(self.numeral_expected_error)
                else:
                    for i in range(7):
                        if i == 2:
                            if value[i] == '2':
                                self.transport.write(self.out_of_bounds_error)
                                return
                            if not self.checkValueRange(0, 3, int(value[i]), self.out_of_bounds_error):
                                return
                        elif not self.checkValueRange(0, 1, int(value[i]), self.out_of_bounds_error):
                            return
                    self.transport.write(self.prompt)
                    
            elif command == "te":
                temp = value
                value = temp.lstrip(':')
                if not value.isdigit():
                    self.transport.write(self.numeral_expected_error)
                elif len(value) < 8:
                    self.transport.write(self.numeral_expected_error)
                elif len(value) > 8:
                    self.transport.write(self.extra_parameters_error)
                else:
                    for i in range(4):
                        temp = value[i*2:(i*2)+2]
                        if i == 0:                       
                            if not self.checkValueRange(0, 23, int(temp), self.out_of_bounds_error):
                                return
                        elif i == 1 or i == 2:                       
                            if not self.checkValueRange(0, 59, int(temp), self.out_of_bounds_error):
                                return
                        else:                       
                            if not self.checkValueRange(0, 99, int(temp), self.out_of_bounds_error):
                                return
                    self.transport.write(self.prompt)
                    
            elif command == "tp":
                temp = value
                value = temp.lstrip(':')
                if not value.isdigit():
                    self.transport.write(self.numeral_expected_error)
                elif len(value) < 6:
                    self.transport.write(self.numeral_expected_error)
                elif len(value) > 6:
                    self.transport.write(self.extra_parameters_error)
                else:
                    for i in range(3):
                        temp = value[i*2:(i*2)+2]
                        if i == 0 or i == 1:                       
                            if not self.checkValueRange(0, 59, int(temp), self.out_of_bounds_error):
                                return
                        else:                       
                            if not self.checkValueRange(0, 99, int(temp), self.out_of_bounds_error):
                                return
                    self.transport.write(self.prompt)
                    
            elif command == "wb":
                if not value.isdigit():
                    self.transport.write(self.numeral_expected_error)
                elif value != "1" and value != "2":
                    self.transport.write(self.out_of_bounds_error)
                else:
                    self.transport.write(self.prompt)
                    
            elif command == "wn":
                if not value.isdigit():
                    self.transport.write(self.numeral_expected_error)
                elif self.checkValueRange(1, 255, int(value), self.out_of_bounds_error):
                    self.transport.write(self.prompt)
                    
            elif command == "wp":
                if not value.isdigit():
                    self.transport.write(self.numeral_expected_error)
                elif self.checkValueRange(1, 16384, int(value), self.out_of_bounds_error):
                    self.transport.write(self.prompt)
                    
            elif command == "ws":
                if not value.isdigit():
                    self.transport.write(self.numeral_expected_error)
                elif len(value) > 4:
                    self.transport.write(self.extra_parameters_error)
                elif self.checkValueRange(0, 9999, int(value), self.out_of_bounds_error):
                    self.transport.write(self.prompt)
                    
            elif command in self.simple_commands:
                # Any command that falls to this point gets handled with the general
                # command response that is in the commands dictionary.
                if len(value) != 0:
                    self.transport.write(self.extra_parameters_error)
                else:
                    self.transport.write(self.simple_commands[command])
                    
            else:
                log.debug("Invalid command received: %s" % (command))
                self.transport.write(self.unrecognized_cmd_error)

    def connectionLost(self, reason):
        log.debug("Simmulator connection now closed")
        self.factory.connections.remove(self)
        
    def checkValueRange(self, low, high, value, error):
        log.debug("low=%d, high=%d, value=%d" %(low, high, value))
        if value < low or value > high:
            self.transport.write(error)
            return 0
        return 1

    def testSampler(self):
        # Increment the number of samples, then "take a sample" by responding
        # with canned sample data.  If the number of samples reaches max,
        # stop the timer, reset the number of samples to 0, and send the
        # to the client.
        self.numTestSamples += 1
        self.transport.write(self.get_next_sample())
        if self.numTestSamples == self.maxTestSamples:
            log.debug("Stopping test samples")
            self.numTestSamples = 0
            self.testRunning = 'false'
            self.lc_testSampler.stop()
            self.transport.write(self.prompt)


    def autoSampler(self):
        # Send a sample to the client.  This happens until the client sends a
        # stop command.
        log.info("autoSampler")
        self.transport.write(self.get_next_sample())

    def get_next_sample(self):
        self.sample_cnt += 1
        cnt = self.sample_cnt
        value1 = 10.0 + 5.0 * math.sin(float(cnt) / 5.0)
        value2 = 7.00012 * random.random()
        value3 = 3.139 + random.random()
        value4 = 1.0103 + random.random()
        valstr = "%1.4f,  %1.5f,   %1.3f,   %1.3f\r\n" % (value1,value2,value3,value4)
        return valstr
        """
        return '7F7FF002000612004D008E008001FA0174020000101ECA410035041E02009001' + \
               'B00001400900D0070005001F000000007D1D6E02BA0101053200310064000000' + \
               'C71A5B090000FF00D105000014800008000A0A0B031C0D15000000F6050000DB' + \
               '359FFF890023008108000005000000503F56FFFF56819F000000008419000000' + \
               '000000000000140A0A0B031C0D15000100800080008000800080008000800080' + \
               '0080008000800080008000800080008000800080008000800080008000800080' + \
               '0080008000800080008000800080008000800080008000800080008000800080' + \
               '0080008000800080008000800080008000800080008000800080008000800080' + \
               '0080008000800080008000800080008000800080008000800080008000800080' + \
               '0080008000800080008000800080008000800080008000800080008000800080' + \
               '0080008000800080008000800080008000800080008000800080008000800080' + \
               '0080008000800080008000800080008000800080008000800080008000800080' + \
               '0002050705710405070806060707080709050207090808040508070706070808' + \
               '0A07050A070A0608070C07090505060308050703040C070B08040405060A0A04' + \
               '070806070409080906060506050709040608080B05090A080B080D0B100B0805' + \
               '07080A030B0C03080808060B06080806050B0406090606080406000326202734' + \
               '2721262526222725272226252622262527212625262226252721272526222625' + \
               '2622262626222626262226252622272626212625262226252622262527222625' + \
               '2622272526212625262227262621272527222624262125252722262626222625' + \
               '2622262527222725272226252622262527222625000400006400000064000000' + \
               '6400000064000000640000006400000064000000640000006400000064000000' + \
               '6400000064000000640000006400000064000000640000006400000064000000' + \
               '6400000064000000640000006400000064000000640000006400000064000000' + \
               '6400000064000000640000006400C837C277\r\n'
        """
        
    def startTestSamples(self):
        # start the test sample timer
        self.lc_testSampler.start(self.testInterval)

    def startAutoSamples(self):
        # start the autonomous sample timer
        log.info("Starting auto samples")
        self.lc_autoSampler.start(self.autoInterval)

    def stopAutoSamples(self):
        # stop the autonomous sample timer
        self.lc_autoSampler.stop()
        
    def test(self, p):
        log.debug("in test")



class Simulator(object):

    all_simulators = []
    InstrumentRef = None

    def __init__(self, instrument_id=INSTRUMENT_ID, port=None):
        self.state = "NEW"
        self.factory = protocol.Factory()
        self.factory.protocol = Instrument
        self.factory.connections = []
        self.factory.parent = self
        log.debug("self.factory.parent = %s" % self.factory.parent)
        self.Cmdfactory = protocol.Factory()
        self.Cmdfactory.protocol = CmdPort
        self.Cmdfactory.connections = []
        self.Cmdfactory.parent = self
        self.instrument_id = instrument_id
        parsed = __file__.rpartition('/')
        parsed = parsed[2].partition('.')
        SimulatorName = parsed[0]
        if not port:
            if not SimulatorName in portNumbers:
                port = NO_PORT_NUMBER_FOUND
            else:
                port = portNumbers[SimulatorName]
        self.DataPort = port
        Simulator.all_simulators.append(self)

    def start(self):
        """
        @brief Instantiate the reactor to listen on a TCP port, passing a
        Factory as an argument.  When a connection is made, and new
        instance of an instrument is constructed.  Currently, the factory only
        supports one client (i.e., there is not an array of clients, and so
        if an new client connects while another is connected, the client variable
        in the factory will be overwritten).
        """
        if self.DataPort == NO_PORT_NUMBER_FOUND:
            log.error("Failed to start Workhorse Sentinel ADCP simulator, no default port number")
            return [NO_PORT_NUMBER_FOUND, NO_PORT_NUMBER_FOUND]
        assert (self.state == "NEW" or self.state == "STOPPED")
        StartingPortNumber = self.DataPort
        Listening = False
        while not Listening:
            try:
                self.listenport = reactor.listenTCP(self.DataPort, self.factory)
                Listening = True
            except:
                self.DataPort = self.DataPort + 1
                if self.DataPort == StartingPortNumber + NUMBER_OF_PORTS_AVAILABLE:
                    log.error("Failed to start Workhorse Sentinel ADCP simulator, no ports available")
                    return [NO_PORT_NUMBER_FOUND, NO_PORT_NUMBER_FOUND]
        Listening = False
        self.CmdPort = self.DataPort + 1
        while not Listening:
            try:
                self.Cmdlistenport = reactor.listenTCP(self.CmdPort, self.Cmdfactory)
                Listening = True
            except:
                self.CmdPort = self.CmdPort + 1
                if self.CmdPort == StartingPortNumber + NUMBER_OF_PORTS_AVAILABLE:
                    log.error("Failed to start Workhorse Sentinel ADCP simulator, no ports available")
                    return [NO_PORT_NUMBER_FOUND, NO_PORT_NUMBER_FOUND]
        self.state = "STARTED"
        log.info("Started Workhorse Sentinel ADCP simulator for ID %s on ports %d %d" \
                 % (self.instrument_id, self.DataPort, self.CmdPort))
        return [self.DataPort, self.CmdPort]


    @defer.inlineCallbacks
    def stop(self):
        assert (self.state == "STARTED")
        for conn in self.factory.connections:
            yield conn.transport.loseConnection()
        yield self.listenport.stopListening()
        yield self.Cmdlistenport.stopListening()
        log.info("Stopped Workhorse Sentinel ADCP simulator on port %d" % (self.DataPort))
        self.state = "STOPPED"

    @classmethod
    @defer.inlineCallbacks
    def stop_all_simulators(cls):
        for sim in Simulator.all_simulators:
            if sim.state == "STARTED":
                yield sim.stop()
