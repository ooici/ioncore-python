#!/usr/bin/env python
"""
@file ion/agents/instrumentagents/simulators/sim_NMEA0183_liveSFBay
.py
@brief Launches a simulated GPS that outputs NMEA0183, sourced from live ship positions in the San Francisco Bay.
@author Alon Yaari
"""


import os
import signal
import subprocess
from sim_NMEA0183 import *

AISGPSapp = ['./simGPS0183app']


class NMEA0183SimliveSFBay (NMEA0183SimBase):
    """
    Launch NMEA0183 GPS simulator on local machine through a virtual serial port.
    """

    WHICHSIM = 'NMEA0183 GPS San Francisco Bay AIS-based Simulator'

    def SimGPSSetup(self):
        """
        Launch external SFBay AIS simulator app
        @param None
        @retval None
        """

        self._workingSim = False
        
        # Launches AIS-based GPS sim subprocess with output to the serial port
        if self._serDesc.closed:
            return
        try:
            log.info ('Launching simulator %s' % AISGPSapp)
            log.info ('Wait approximately 15 seconds...')
            self._simProcess = subprocess.Popen (AISGPSapp,
                                         stdout = self._serDesc.fileno(),
                                         stderr = nullDesc.fileno())
        except OSError, e:
            log.info ('Current directory: %s' % os.getcwd())
            log.info (e)

        yield TwistedSleep (10)
        log.info ('Successfully launched SFBay AIS-based GPS simulator.')
        self._workingSim = True

    def SimShutdown (self):
        """
        Stops and disconnects the simulator.
        @param None
        @retval None
        """

        log.info ('Stopping the simulator...')

        # If the process isn't running for whatever reason, nothing to stop
        if self.IsSimulatorRunning():

            # Force the simulator app to stop
            # (Python 2.6 and later would let us send a control-C to stop it)
            os.kill (self._simProcess.pid, signal.SIG_IGN)
            os.kill (self._vsp.pid, signal.SIG_IGN)
            TwistedSleep (2)
        log.info ('Simulator no longer running.')

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

    def WhichSimulator (self):
        """
        Override this method to return a string with the simulator's name
        """
        return "NMEA0183 GPS San Francisco Bay AIS sourced simulator"



