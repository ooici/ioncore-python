"""
Instrument base class HSM
"""

from miros import Hsm


class InstrumentHsm(Hsm):

    def top(self, caller):
        log.debug("!!!!!!!!!!!!!!! In top state")
        if caller.tEvt['sType'] == "init":
            # display event
            log.debug("idle-%s;" %(caller.tEvt['sType']))
            # transition to state idle
            caller.stateStart(self.idle)
            # returning a 0 indicates event was handled
            return 0
        elif caller.tEvt['sType'] == "entry":
            # display event, do nothing 
            log.debug("idle-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.debug("idle-%s;" %(caller.tEvt['sType']))
            self.tEvt['nFoo'] = 0
            return 0
        return caller.tEvt['sType']

    def idle(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In idle state")
        if caller.tEvt['sType'] == "init":
            # display event
            log.debug("idle-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "entry":
            # display event, do nothing 
            log.debug("idle-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.debug("idle-%s;" %(caller.tEvt['sType']))
            self.tEvt['nFoo'] = 0
            return 0
        elif caller.tEvt['sType'] == "configured":
            log.debug("idle-%s;" %(caller.tEvt['sType']))
            log.info("!!!!!! transitioning to stateConfigured! idle-%s;" %(caller.tEvt['sType']))
            self.hsm.stateTran(self.stateConfigured)
            return 0
        return caller.tEvt['sType']

    def stateConfigured(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In stateConfigured state")
        if caller.tEvt['sType'] == "init":
            log.info("stateConfigured-%s;" %(caller.tEvt['sType']))
            caller.stateStart(self.stateDisconnected)
            return 0
        elif caller.tEvt['sType'] == "entry":
            log.info("stateConfigured-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.info("stateConfigured-%s;" %(caller.tEvt['sType']))
            return 0
        return caller.tEvt['sType']

    def stateDisconnecting(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In stateDisconnecting state")
        if caller.tEvt['sType'] == "init":
            log.info("stateDisconnecting-%s;" %(caller.tEvt['sType']))
            log.debug("disconnecting from instrument")
            self.proto.transport.loseConnection()
            return 0
        elif caller.tEvt['sType'] == "entry":
            log.info("stateDisconnecting-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.info("stateDisconnecting-%s;" %(caller.tEvt['sType']))
            return 0
            return 0
        elif caller.tEvt['sType'] == "eventDisconnectComplete":
            log.info("stateDisconnecting-%s;" %(caller.tEvt['sType']))
            #
            # Transition to the stateConnected state
            #
            caller.stateTran(self.stateDisconnected)
            return 0
        return caller.tEvt['sType']

    def stateDisconnected(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In stateDisconnected state")
        if caller.tEvt['sType'] == "init":
            log.info("stateDisconnected-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "entry":
            log.info("stateDisconnected-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.info("stateDisconnected-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "eventCommandReceived":
            log.info("stateDisconnected-%s;" %(caller.tEvt['sType']))
            #
            # Transition to the stateConnecting state
            #
            caller.stateTran(self.stateConnecting)
            return 0
        return caller.tEvt['sType']

    def stateConnecting(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In stateConnecting state")
        if caller.tEvt['sType'] == "init":
            log.info("stateConnecting-%s;" %(caller.tEvt['sType']))
            self.getConnected()
            return 0
        elif caller.tEvt['sType'] == "entry":
            log.info("stateConnecting-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.info("stateConnecting-%s;" %(caller.tEvt['sType']))
            return 0
            return 0
        elif caller.tEvt['sType'] == "eventConnectionComplete":
            log.info("stateConnecting-%s;" %(caller.tEvt['sType']))
            #
            # Transition to the stateConnected state
            #
            caller.stateTran(self.stateConnected)
            return 0
        return caller.tEvt['sType']

    def stateConnected(self, caller):
        log.debug("!!!!!!!!!!!!!!!  In stateConnected state")
        if caller.tEvt['sType'] == "init":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            """
            @todo Need a queue of commands from which to pull commands
            """
            # if command pending
            if self.command:
               self.instrument.transport.write(self.command)
            return 0
        elif caller.tEvt['sType'] == "entry":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "exit":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            return 0
        elif caller.tEvt['sType'] == "eventCommandReceived":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            # if command pending
            if self.command:
               self.instrument.transport.write(self.command)
            return 0
        elif caller.tEvt['sType'] == "eventDisconnectReceived":
            log.info("stateConnected-%s;" %(caller.tEvt['sType']))
            #
            # Transition to the stateDisconnecting state
            #
            caller.stateTran(self.stateDisconnecting)
            return 0
        return caller.tEvt['sType']

