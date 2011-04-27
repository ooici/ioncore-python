#!/usr/bin/env python

"""
@file ion/interact/lazy_eye_web.py
@author Paul Hubbard
@date 4/25/11
@brief LazyEye is a RESTful interface on top of ion.interact.int_observer; a way to command
and control the generation and viewing of message sequence charts. This module is the web
interface only. The observer is in lazy_eye.py
@note "RESTful observer = lazy eye" - get it? Sure ya do.
"""

from twisted.internet import defer, reactor
from twisted.web import resource
from twisted.web.server import Site

import ion.util.ionlog
from ion.core.process.process import ProcessFactory, Process

from ion.interact.lazy_eye import LazyEyeClient
# Globals
log = ion.util.ionlog.getLogger(__name__)
# @todo move these into ion.config
WEB_PORT = 2012

# Web page elements, could also be moved to configuration file.
page_header = """
<html>
<header>
<title>ion-python MSC creator</title>
</header>
<body>
<img src="http://ooici.net/global.logo.jpeg" alt="OOI-CI logo">
<h3>Where am I?</h3>
This page controls the ION Python message capture and diagramming tool. Pick a filename,
press Enter, then hit stop when your code is done, and it presents a graphical message
sequence chart via <a href="http://www.mcternan.me.uk/mscgen/">mscgen</a>.
<p />
"""
page_footer = """
</body>
</html>
"""
startbox = """
<form action="/go/" method="get">
 Filename: <input name="filename" value="msc.txt" size="64" type="text"/>
</form>
"""
stopbutton = """
<form action="/stop/" method="get" <input value="Stop" type="Submit" /></form>
"""

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
    def __init__(self, lec):
        resource.Resource.__init__(self)
        self.isLeaf = True
        self.lec = lec

    @defer.inlineCallbacks
    def render_GET(self, request):
        request.write('Stopping capture and rendering PNG...')
        
        # Stop method also does the render before returning, maybe slow
        dp = yield self.lec.stop()

        # Lookup image name
        img_file = yield self.lec.get_image_name()
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
    def __init__(self, lec, filename):
        resource.Resource.__init__(self)
        log.debug('Going, filename=%s ' % filename)
        self.filename = filename
        self.lec = lec
        self.isLeaf = True

    #noinspection PyUnusedLocal
    def render_GET(self, request):
        request.write('Starting capture...<a href="/stop/">Click here to stop</a>')
        d = self.lec.start(filename=self.filename)
        # Returning a deferred, the web framework will yield on same
        return d

class RootPage(resource.Resource):
    """
    Return the static page, the go page or the stop page (results). REST in Twisted is
        kind of peculiar.
    """
    def __init__(self, lec):
        resource.Resource.__init__(self)
        self.lec = lec

    def getChild(self, pathstr, request):
        log.debug('got request for "%s"' % request)

        if pathstr == 'go':
            return GoPage(self.lec, request.args['filename'][0])
        elif pathstr == 'stop':
            return StopPage(self.lec)
        elif pathstr == '':
            return NavPage()
        else:
            return resource.Resource.getChild(self, pathstr, request)

class LazyEyeMonitor(Process):
    """
    Provides the webservice to start/top/display message sequence charts
    via Michaels message capture and formatting code.

    Code cribbed from ion.services.dm.distribution.notify_web_monitor
    """
    def plc_init(self):
        Process.plc_init(self)

        import pdb
        pdb.set_trace()
        
        log.debug('starting client init')
        self.lec = LazyEyeClient(proc=self)

        #self.lec = None
        log.debug('client init completed')
        self.rootpage = RootPage(self.lec)
        self.site = Site(self.rootpage)
        reactor.listenTCP(WEB_PORT, self.site)
        log.debug('Listening on http://localhost:%d/' % WEB_PORT)


# Spawn off the process using the module name
factory = ProcessFactory(LazyEyeMonitor)
