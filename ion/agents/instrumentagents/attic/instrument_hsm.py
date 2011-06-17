"""
Instrument base class HSM
"""

from miros import Hsm
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)


class InstrumentHsm(Hsm):

    hsmEvents = [
        'eventCommandReceived',
        'eventConfigured',
        'eventConnectionComplete',
        'eventConnectionFailed',
        'eventDataReceived',
        'eventDisconnectComplete',
        'eventDisconnectReceived',
        'eventPromptReceived',
        'eventResponseTimeout',
        'eventWakeupTimeout',
        'eventInstrumentAsleep'
    ]

    def sendEvent(self, event):
        if event in self.hsmEvents:
            self.onEvent(event)
        else:
            log.critical(" INVALID EVENT: %s" %event)
            raise RuntimeError(" InstrumentHsm:  INVALID EVENT: %s" %event)

            
