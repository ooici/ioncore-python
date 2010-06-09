#!/usr/bin/env python

#from twisted.internet.protocol import Protocol, Factory
from twisted.internet import protocol
from twisted.internet import reactor
from twisted.internet import task
#from twisted.internet.task import LoopingCall

class Instrument(protocol.Protocol):

    numPolledSamples = 0
    prompt = 'S>'
    mode = 'auto'
    pumpInfo = 'SBE 49 FastCAT V 1.3a SERIAL NO. 0055'
    sampleData = 'This is sample data: not sure what to put here yet.\n'
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
          'real-time temperature and conductivity correction enabled for converted data\n' +
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
        'tt' : '20.8355\n' + 
          '20.8359\n' +
          '20.8360\n' +
          '20.8360\n' +
          prompt,
        'tc' : prompt, 
        'tp' : prompt, 
        'ttr' : prompt, 
        'tcr' : prompt, 
        'tpr' : prompt, 
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
   
    def connectionMade(self):
        self.factory.clientConnectionMade(self)
 
    def dataReceived(self, data):
        """
        test data for membership in command 
        """
        data = data.strip()
        data = data.lower()
        print "received: %s" % (data)
        if len(data) == 0:
            self.transport.write("S>")
        elif data == "ts":
            # Not worrying about mode right now
            self.factory.takePolledSample()
        elif data == "start":
            if self.mode == 'auto':
                # Need way to have a timer 
                print "Starting samples"
                self.factory.startSamples()
                #self.transport.write(self.sampleData + self.prompt)
            else:
                self.transport.write(self.commands[data])
        elif data == "stop":
            if self.mode == 'auto':
                print "Stopping samples"
                self.factory.stopSamples()
            # Print prompt
            self.transport.write(self.prompt)
        elif data in self.commands:
            self.transport.write(self.commands[data])
        else:
            print "Invalid command received"
            self.transport.write("?CMD")

        """
        if data == "pumpOn":
            print "Got something"
            self.transport.write(self.commands["pumpOn"])
        elif data == "pumpOff":
            self.transport.write(self.commands["pumpOff"])
        elif data == "DS":
            self.transport.write(self.commands["DS"])
        """

class InstrumentFactory(protocol.Factory):
    protocol = Instrument
    def __init__(self):
        self.lc_polledSampler = task.LoopingCall(self.polledSampler)
        self.lc = task.LoopingCall(self.takeSample)

    def clientConnectionMade(self, client):
        self.client = client 

    def takePolledSample(self):
        self.lc_polledSampler.start(1)

    def startSamples(self):
        self.lc.start(5)

    def stopSamples(self):
        self.lc.stop()

    def polledSampler(self):
        self.client.numPolledSamples += 1
        self.client.transport.write(self.client.sampleData)
        if self.client.numPolledSamples == 10:
            self.client.numPolledSamples = 0
            self.lc_polledSampler.stop()
            self.client.transport.write(self.client.prompt)
        
    def takeSample(self):
        self.client.transport.write(self.client.sampleData)
        
def main():
    f = InstrumentFactory()
    #f.protocol = Instrument 
    reactor.listenTCP(9000, f)
    reactor.run()

if __name__ == '__main__':
    main()
