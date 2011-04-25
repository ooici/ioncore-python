#!/usr/bin/env python

"""
@file ion/interact/lazy_eye.py
@author Paul Hubbard
@date 4/25/11
@brief LazyEye is a RESTful interface on top of ion.interact.int_observer; a way to command
and control the generation and viewing of message sequence charts.
@note "RESTful observer = lazy eye" - get it? Sure ya do.
"""

from twisted.internet import defer, reactor
from twisted.internet import protocol
from twisted.web import resource
from twisted.web.server import Site

import ion.util.ionlog
from ion.core.process.process import ProcessFactory, ProcessClient
from ion.interact.int_observer import InteractionObserver

# Globals
log = ion.util.ionlog.getLogger(__name__)
# @todo move these into ion.config
BINARY_NAME = 'mscgen'
WEB_PORT = 2012
page_header = """
<html>
<header>
<title>ion-python MSC creator</title>
</header>
<body>
<img src="http://ooici.net/global.logo.jpeg" alt="ion logo">
<p />
"""
page_footer = """
</body>
</html>
"""
startbox = """
<form action="/go/" method="get" <input name="filename" value="msc.txt" size="64" type="text"/>
</form>
"""
stopbutton = """
<form action="/stop/" method="get" <input value="Stop" type="Submit" /></form>
"""


class MscProcessProtocol(protocol.ProcessProtocol):
    """
    Wrapper around the mscgen application, saves output
    """
    def __init__(self, callback, msg):
        protocol.ProcessProtocol.__init__(self)
        self.output = []
        self.running = False
        self.cb = callback
        self.msg = msg

    def connectionMade(self):
        log.debug('mscgen started ok')
        self.running = True

    def processExited(self, reason):
        log.debug('mscgen exited, %s' % str(reason))
        self.cb(self.msg)
        self.running = False

    def outReceived(self, data):
        """
        Called when mscgen prints to the screen
        """
        log.debug('got "%s"' % data)
        self.output.append(data)

class LazyEye(InteractionObserver):
    """
    @brief LazyEye is a RESTful interface on top of ion.interact.int_observer; a way to command
    and control the generation and viewing of message sequence charts.
    @note "RESTful observer = lazy eye" - get it? Sure ya do.
    """

    #noinspection PyUnusedLocal
    def op_start(self, request, headers, msg):
        log.debug('Got a start request')

        if self.running:
            if request.filename == self.filename:
                log.debug('Duplicate start message received, ignoring')
                return
            else:
                log.error('Start received with different filename! Ignoring.')
                return

        self.running = True
        self.filename = request.filename
        self.imagename = request.filename + '.png'
        d = self.plc_init()
        d.addCallback(self.plc_activate())

        self.reply_ok(msg)

    #noinspection PyUnusedLocal
    @defer.inlineCallbacks
    def op_stop(self, request, headers, msg):
        log.debug('Stop request received')

        if not self.running:
            log.error('Stop receieved but not started, ignoring')
            return

        log.debug('Stopping receiver')
        yield self.msg_receiver.deactivate()
        yield self.msg_receiver.terminate()

        log.debug('writing datafile %s' % self.filename)
        f = open(self.filename, 'w')
        f.write(self.writeout_msc())

        self.mpp = MscProcessProtocol(self._mscgen_callback, msg)
        log.debug('Spawing mscgen to render the graph...')
        yield reactor.spawnProcess(self.mpp, BINARY_NAME,
                                              '-i', self.filename,
                                              '-o', self.imagename,
                                              '-T', 'png')
        log.debug('mscgen started')

    #noinspection PyUnusedLocal
    def op_get_image_name(self, request, headers, msg):
        log.debug('image name requested, returning %s' % self.imagename)
        self.reply_ok(msg, self.imagename)

    def _mscgen_callback(self, msg):
        """
        Send reply to caller when mscgen is finished. Callback hook.
        """
        self.reply_ok(msg)

class LazyEyeClient(ProcessClient):
    """
    Minimal process client, start/stop.
    """
    @defer.inlineCallbacks
    def start(self, filename='msc.txt'):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('start', filename)
        defer.returnValue(content)

    @defer.inlineCallbacks
    def stop(self):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('stop')
        defer.returnValue(content)

    @defer.inlineCallbacks
    def get_image_name(self):
        yield self._check_init()
        (content, headers, msg) = yield self.rpc_send('get_image_name')
        defer.returnValue(content)

class NavPage(resource.Resource):
    """
    Root web page for the user interface
    """
    def render_GET(self, request):
        request.write(page_header)
        request.write(startbox)
        request.write(stopbutton)
        request.write(page_footer)
        return ''

class StopPage(resource.Resource):
    """
    Stop the capture, display results
    """
    def __init__(self):
        resource.Resource.__init__(self)
        self.isLeaf = True

    @defer.inlineCallbacks
    def render_GET(self, request):
        request.write('Stopping capture and rendering PNG...')
        
        lec = LazyEyeClient()
        # Stop method also does the render before returning, maybe slow
        dp = yield lec.stop()

        # Lookup image name
        img_file = yield lec.get_image_name()
        request.write('<img src="%s" alt="msc">' % img_file)

        # DDT
        request.write('<p>MSC:<p><pre>')
        request.write(dp)
        request.write('</pre>')

        defer.returnValue('')

class GoPage(resource.Resource):
    """
    Bar.
    """
    def __init__(self, filename):
        resource.Resource.__init__(self)
        log.debug('go page created with %s ' % filename)
        self.filename = filename
        self.isLeaf = True

    #noinspection PyUnusedLocal
    def render_GET(self, request):
        lec  = LazyEyeClient()
        d = lec.start(filename=self.filename)
        return d

class RootPage(resource.Resource):
    """
    If child is required, generate it on the fly
    @todo init method holding LazyEye instance
    """
    def getChild(self, pathstr, request):
        log.debug('got request for "%s" (%s)' % (pathstr, request))

        if pathstr == 'go':
            return GoPage(request.args['filename'][0])
        elif pathstr == 'stop':
            return StopPage()
        elif pathstr == '':
            return NavPage()
        else:
            return resource.Resource.getChild(self, pathstr, request)


# Spawn off the process using the module name
factory = ProcessFactory(LazyEye)

def main():
    root = RootPage()
    factory = Site(root)
    reactor.listenTCP(WEB_PORT, factory)
    log.info('http://localhost:%d/' % WEB_PORT)

if __name__ == '__main__':
    main()
    reactor.run()