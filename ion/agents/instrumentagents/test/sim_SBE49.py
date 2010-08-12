#!/usr/bin/env python

"""
Seabird SBE49 Instrument Simulator - Provides basic simulation of a 
Seabird SBE-49 instrument.  This is not part of the product description,
but rather is being used to help develop instrument agents and drivers.
This program can be fleshed out as needed.  

@file sim_SBE49.py
@author Dave Everett 
@date 6/8/10
"""

import sys
from twisted.internet import protocol
from twisted.internet import reactor
from twisted.internet import task

INSTRUMENT_ID = "123"
SIM_PORT = 9000

class Instrument(protocol.Protocol):
    """
    The instrument protocol class. Simulate a Seabird SBE49 by receiving 
    commands over a TCP connection and responding as much as possible
    as would a Seabird SBD49.
    """

    numTestSamples = 0  # variable to hold number of test samples taken
    maxTestSamples = 10 # maximumm number of test stamples to take
    testInterval = 1    # interval between samples in test commands (TT, etc.)
    autoInterval = 5    # interval between samples in autonomous mode
    testRunning = 'false'
    autoRunning = 'false'
    prompt = 'S>'
    mode = 'auto'
    pumpInfo = 'SBE 49 FastCAT V 1.3a SERIAL NO. 0055'
    sampleData = '21.9028,  1.00012,    1.139,   1.0103\n'
    commands = {
        'ds' : pumpInfo + '\n' + 
          'number of scans to average = 1\n' +
          'pressure sensor = strain gauge, range = 1000.0\n' +
          'minimum cond freq = 3000, pump delay = 30 sec\n' +
          'start sampling on power up = no\n' +
          'output format = converted decimal\n' +
          'output salinity = yes, output sound velocity = yes\n' +
          'temperature advance = 0.0625 seconds\n' +
          'celltm alpha = 0.03\n' +
          'celltm tau = 7.0\n' +
          'real-time temperature and conductivity correction enabled for '
          'converted data\n' +
          prompt,
        'setdefaults' : prompt, 
        'baud' : prompt, 
        'outputformat' : prompt, 
        'outputsv' : prompt, 
        'outputformat' : prompt, 
        'autorun' : prompt, 
        'navg' : prompt, 
        'mincondfreq' : prompt, 
        'pumpdelay' : prompt, 
        'processrealtime' : prompt, 
        'tadvance' : prompt, 
        'alpha' : prompt, 
        'tau' : prompt, 
        'start' : prompt, 
        'stop' : prompt,
        'pumpon' : prompt, 
        'pumpoff' : prompt, 
        'ts' : '20.9028,  0.00012,    0.139,   0.0103\n' + prompt, 
        'dcal' : pumpInfo + '\n' +
          'temperature: 26-apr-01\n' + 
          '    TA0 = -3.178124e-06\n' + 
          '    TA1 = 2.751603e-04\n' + 
          '    TA2 = -2.215606e-06\n' + 
          '    TA3 = 1.549719e-07\n' + 
          '    TOFFSET = 0.000000e+00\n' + 
          'conductivity: 26-apr-01\n' + 
          '    G = -9.855242e-01\n' + 
          '    H = 1.458421e-01\n' + 
          '    I = -3.290801e-04\n' + 
          '    J = 4.784952e-05\n' + 
          '    CPCOR = -9.570000e-08\n' + 
          '    CTCOR = 3.250000e-06\n' + 
          '    CSLOPE = 1.000000e+00\n' + 
          'Pressure S/N = 023674, range = 1000 psia:  25-apr-01\n' + 
          '    PA0 = -6.893561e-01\n' + 
          '    PA1 = 1.567975e-02\n' + 
          '    PA2 = -6.637727e-10\n' + 
          '    PTCA0 = 5.246558e+05\n' + 
          '    PTCA1 = -4.886082e+00\n' + 
          '    PTCA2 = 1.257885e-01\n' + 
          '    PTCB0 = 2.489275e+01\n' + 
          '    PTCB1 = -8.500000e-04\n' + 
          '    PTCB2 = 0.000000e+00\n' + 
          '    PTEMPA0 = -6.634546e+01\n' + 
          '    PTEMPA1 = 5.093069e+01\n' + 
          '    PTEMPA2 = 1.886260e-01\n' + 
          '    POFFSET = 0.000000e+00\n' + 
          prompt,
        '' : prompt, 
    }
    testCommands = {
        'tt' : prompt,
        'tc' : prompt, 
        'tp' : prompt, 
        'ttr' : prompt, 
        'tcr' : prompt, 
        'tpr' : prompt, 
    }
   
    def connectionMade(self):
        """
        @brief A client has made a connection: call factory to pass
        this instance, because the factory has the timer (LoopingCall). 
        @param none 
        @retval none 
        """
        self.factory.clientConnectionMade(self)

        # Print prompt
        self.transport.write(self.prompt)
 
    def dataReceived(self, data):
        """
        @brief Data as been recieved from client. Determine what command was 
        sent and attempt to respond as would an SBE49.
        @param Data from client.
        @retval none 
        """

        # Strip off the newlines and other extraneous whitespace, and convert 
        # to lower case.
        data = data.strip()
        data = data.lower()

        if len(data) == 0:
            """
            @note If zero length data received, probably just operator mashing 
            on return key: just return the prompt (just as SBE59 would).
            """
            self.transport.write("S>")
        elif data in self.testCommands:
            """
            @note If a "testing" command is received, and the instrument is 
            not already running a test, start sending samples 
            at the configured test interval until the configured maximum 
            number of test samples have been sent. 
            """
            if self.testRunning == 'false':
                print "Starting test samples"
                self.factory.startTestSamples() 
                self.testRunning = 'true'
        elif data == "start":
            """
            @note If start command is received, and the SBE49 is in autonomous 
            mode, and the instrument is not already running a test, start 
            sending samples at the configured interval. 
            """
            if self.mode == 'auto' and self.autoRunning == 'false':
                print "Starting auto samples"
                self.autoRunning = 'true'
                # The factory handles the sending at intervals.
                self.factory.startAutoSamples()
            else:
                # Currently we don't simulate auto/polled: we just handle the 
                # start/stop as if we're in auto mode (mode defaults to auto 
                # and doesn't change). But, in the future we might want to 
                # simulate the modes: wouldn't be hard.
                self.transport.write(self.commands[data])
        elif data == "stop":
            """
            @note If stop command is received, and the SBE49 is in autonomous 
            mode,  and the instrument is running a test, stop sending samples 
            at the configured interval. 
            """
            if self.mode == 'auto' and self.autoRunning == 'true':
                print "Stopping auto samples"
                self.factory.stopAutoSamples()
                self.autoRunning = 'false'

            # Print prompt whether we stopped or not
            self.transport.write(self.prompt)
        elif data in self.commands:
            # Any command that falls to this point gets handled with the general
            # command response that is in the commands dictionary. 
            print "command received: %s" % (data)
            self.transport.write(self.commands[data])
        else:
            print "Invalid command received: %s" % (data)
            self.transport.write("?CMD")

class InstrumentFactory(protocol.Factory):
    """
    Assign the Instrument class to protocol, and then instantiate two timers 
    for handling autonomous mode (where the instrument sends data at intervals
    until stopped), and test commands (where the instrument sends 100 samples 
    periodically).  In autonomous mode, the timer is started and stopped based 
    on commands from the client.  In the case of test commands, the timer is
    started when the command is received, and then a counter is incremented 
    for each sample, and when the counter reaches 100, the timer is stopped.
    """

    protocol = Instrument
    def __init__(self):
        self.lc_testSampler = task.LoopingCall(self.testSampler)
        self.lc_autoSampler = task.LoopingCall(self.autoSampler)

    def clientConnectionMade(self, client):
        # A client has connected: save the client instance. This currently
        # only supports one  client
        self.client = client 
    
    def startTestSamples(self):
        # start the test sample timer 
        self.lc_testSampler.start(self.client.testInterval)

    def startAutoSamples(self):
        # start the autonomous sample timer 
        self.lc_autoSampler.start(self.client.autoInterval)

    def stopAutoSamples(self):
        # stop the autonomous sample timer
        self.lc_autoSampler.stop()

    def testSampler(self):
        # Increment the number of samples, then "take a sample" by responding 
        # with canned sample data.  If the number of samples reaches max,
        # stop the timer, reset the number of samples to 0, and send the 
        # to the client.
        self.client.numTestSamples += 1
        self.client.transport.write(self.client.sampleData)
        if self.client.numTestSamples == self.client.maxTestSamples:
            print "Stopping test samples"
            self.client.numTestSamples = 0
            self.client.testRunning = 'false' 
            self.lc_testSampler.stop()
            self.client.transport.write(self.client.prompt)
        
    def autoSampler(self):
        # Send a sample to the client.  This happens until the client sends a 
        # stop command.
        self.client.transport.write(self.client.sampleData)
        
def main():
    """
    @brief Instantiate the reactor to listen on TCP port 9000, passing the 
    InstrumentFactory as an argument.  When a connection is made, and new
    instance of an instrument is constructed.  Currently, the factory only
    supports one client (i.e., there is not an array of clients, and so
    if an new client connects while another is connected, the client variable
    in the factory will be overwritten). 
    """
    global INSTRUMENT_ID
    global SIM_PORT
    if len(sys.argv) >= 2:
        INSTRUMENT_ID = sys.argv[1]
    if len(sys.argv) >= 3:
        SIM_PORT = sys.argv[2]
    print "Starting SBE49 simulator for ID %s on port %d" % (INSTRUMENT_ID,SIM_PORT)
    f = InstrumentFactory()
    reactor.listenTCP(SIM_PORT, f)
    reactor.run()

if __name__ == '__main__':
    main()
