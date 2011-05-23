#!/usr/bin/env python
"""
@file ion/agents/instrumentagents/simulators/sim_NMEA0183.py
@brief Launches the external NMEA0183 GPS simulator against a local virtual serial port.
@author Alon Yaari
"""


import os
import time
import errno
import signal
import subprocess


SERPORTMASTER = './serPortMaster'
SERPORTSLAVE = './serPortSlave'
AISGPSapp1 = ['./simGPS0183app']
AISGPSapp2 = ['../ion/agents/instrumentagents/simulators/simGPS0183app']
SOCATapp1 = './socat'
SOCATapp2 = '../ion/agents/instrumentagents/simulators/socat'
SOCATmaster = 'pty,link=' + SERPORTMASTER + ',raw,echo=0'
SOCATslave = 'pty,link=' + SERPORTSLAVE + ',raw,echo=0'
SERPORTMODE = 'w+'
NULLPORTMODE = 'w'


class NMEA0183Simulator:
    """
    Launch NMEA0183 GPS simulator on local machine through a virtual serial port.
    """

    #    Simulator connects to SERPORTMASTER
    #    driver_ NMEA0183 connects to SERPORTSLAVE


    def __init__(self):
        """
        Creates virtual serial ports then Launches the NEMA0183 GPS simulator
        @param None
        @retval True if successfully launched, False if not
        """

        self._simProcess = None
        self._allGood = False

        # Open a null stream to pipe unwanted console messages to nowhere
        self._nullDescriptor = open (os.devnull, NULLPORTMODE)

        # Create the virtual serial ports
        try:
            print '          ***** Creating virtual serial port. Running %s' % SOCATapp1
            self._vsp = subprocess.Popen([SOCATapp1, SOCATmaster, SOCATslave],
                                        stdout=self._nullDescriptor.fileno(),
                                        stderr=self._nullDescriptor.fileno())
        except OSError:
            try:
                print '          ***** Creating virtual serial port. Running %s' % SOCATapp2
                self._vsp = subprocess.Popen([SOCATapp2, SOCATmaster, SOCATslave],
                                        stdout=self._nullDescriptor.fileno(),
                                        stderr=self._nullDescriptor.fileno())
            except OSError:
                print '\n          ***** Current directory: %s' % os.getcwd()
                return

        print '          ***** Successfully created virtual serial ports.'
        time.sleep(1)

        # Opens serial port in write and read mode
        print '          ***** Opening %s for simulator to connect with.' % SERPORTMASTER
        try:
            self._serDescriptor = open(SERPORTMASTER, SERPORTMODE)
        except Exception, e:
            print e
            return
        print '          ***** Successfuly opened  %s.' % SERPORTMASTER
        time.sleep(1)

        # Launches subprocess with output to the serial port
        if self._serDescriptor.closed:
            return
        try:
            print '          ***** Launching simulator %s' % AISGPSapp1
            self._simProcess = subprocess.Popen(AISGPSapp1,
                                         stdout=self._serDescriptor.fileno(),
                                         stderr=self._nullDescriptor.fileno())
        except OSError:
            try:
                print '          ***** Launching simulator %s' % AISGPSapp2
                self._simProcess = subprocess.Popen(AISGPSapp2,
                                         stdout=self._serDescriptor.fileno(),
                                         stderr=self._nullDescriptor.fileno())
            except Exception, e:
                print '\n          ***** Current directory: %s' % os.getcwd()
                print e

        print '          ***** Successfully launched simulator.'
        self._allGood = True

    def StopSimulator(self):
        """
        Stops and disconnects the simulator.
        @param None
        @retval None
        """

        print '          ***** Stopping the simulator...'
        # If the process isn't running any more, nothing to stop
        if not self.IsSimulatorRunning():
            return

        # Force the simulator and socat apps  to stop
        # (Python 2.6 and later would let us send a control-C to stop it)
        print '          ***** Killing the simulator and serial port apps.'
        os.kill (self._simProcess.pid, signal.SIG_IGN)
        os.kill (self._vsp.pid, signal.SIG_IGN)
        print '          ***** Successfully stopped the simulator.'

    def IsSimulatorRunning(self):
        """
        Checks if the simulator app is still running.
        @param None
        @retval True if running, False if not running
        """

        if not self._allGood:
            return False

        pollStatus = self._simProcess.poll()

        # poll() return None if the app is running smoothly
        if not pollStatus:
            return True

        # poll() returns an integer value if the app has stopped
        return False



