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
from twisted.web import resource, static
from twisted.web.server import Site, NOT_DONE_YET

from ion.core import ioninit
import ion.util.ionlog
from ion.core.process.process import ProcessFactory, Process
from ion.interact.lazy_eye import LazyEyeClient

# Globals
log = ion.util.ionlog.getLogger(__name__)
CONF = ioninit.config(__name__)

WEB_PORT = CONF['WEB_PORT']
IMAGE_PORT = CONF['IMAGE_PORT']
HOSTNAME = CONF['hostname']

# Web page elements, could also be moved to configuration file.
page_header = """
<html>
<header>
<title>ion-python MSC creator</title>
</header>
<body>
<a href="/"><img src="http://ooici.net/global.logo.jpeg" alt="OOI-CI logo"></a>
"""
mainpage_text = """
<h3>Instructions</h3>
This page controls the ION Python message capture and diagramming tool. Pick a
<a href="http://en.wikipedia.org/wiki/Advanced_Message_Queuing_Protocol#Exchange_types_and_the_effect_of_bindings">binding pattern</a>
, press 'Start collection', then hit <a href="/stop">stop</a> when your code is done,
and it presents a graphical message sequence chart via
<a href="http://www.mcternan.me.uk/mscgen/">mscgen</a>.
<p>
<a href="https://github.com/ooici/ioncore-python/tree/develop/ion/interact">Source code is here.</a>
<p>
Clicking on the logo will return you to this page.
<p>
"""
doitagain = """
<form action="/" method="get">
<input type="submit" value="Do it again" />
</form>
"""
page_footer = """
</body>
</html>
"""
binding_form = """
<form action="/go" method="get">
Binding pattern: <input name="binding" value="#" size="32" type="text" />
<input type="submit" value="Start collection" />
</form>
"""

class AsyncResource(resource.Resource):
    """
    Code from ion.services.dm.distribution.notify_web_monitor
    @author Dave Foster
    """
    isLeaf = True

    @defer.inlineCallbacks
    def _do_action(self, request):
        """
        inlineCallbacks decorated action handler called from render_GET.
        Override this in your derived class.
        """
        raise NotImplementedError("You must override _do_action in your derived class")

    def render_GET(self, request):
        """
        Common handler for get requests. Calls into _do_action which you must override.
        """
        def finish_req(res, request):
            request.write(res)
            request.finish()

        def_action = self._do_action(request)
        def_action.addCallback(finish_req, request)

        return NOT_DONE_YET

class NavPage(resource.Resource):
    """
    Root web page for the user interface
    """
    def render_GET(self, request):
        request.write(page_header)
        request.write(mainpage_text)
        request.write(binding_form)
        request.write(page_footer)
        return ''

class StopPage(AsyncResource):
    """
    Stop the capture, display results
    """
    def __init__(self, lec):
        AsyncResource.__init__(self)
        self.lec = lec

    @defer.inlineCallbacks
    def _do_action(self, request):
        request.write(page_header)
        request.write('<p>Stopping capture and rendering PNG...')

        # Stop method also does the render before returning, maybe slow
        yield self.lec.stop()

        rc = yield self.lec.get_results()
        request.write('<br>Results:')
        for cur_key, cur_val in rc.iteritems():
            request.write('  <br>%s: %s' % (cur_key, cur_val))

        # Lookup image name
        img_file = rc['imagename']
        request.write('<h3>MSC</h3><img src="http://%s:%d/%s" alt="msc">' %
                      (HOSTNAME, IMAGE_PORT, img_file))

        # DDT
        # dp = yield self.lec.stop()
        #request.write('<h4>MSC debug output:</h4><pre>')
        #request.write(dp)
        #request.write('</pre>')
        request.write(doitagain)
        request.write(page_footer)

        defer.returnValue('')
        
class GoPage(AsyncResource):
    """
    Bar.
    """
    def __init__(self, lec, binding_key='#'):
        AsyncResource.__init__(self)
        self.lec = lec
        self.binding_key = binding_key

    @defer.inlineCallbacks
    def _do_action(self, request):
        request.write(page_header)
        request.write('<p>Starting capture... <a href="/stop/">Click here to stop</a>')
        yield self.lec.start(binding_key=self.binding_key)
        request.write(page_footer)
        defer.returnValue('')

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
            return GoPage(self.lec, binding_key=request.args['binding'][0])
        elif pathstr == 'stop':
            return StopPage(self.lec)
        elif pathstr == '':
            return NavPage()

        return resource.Resource.getChild(self, pathstr, request)

class LazyEyeMonitor(Process):
    """
    Provides the webservice to start/top/display message sequence charts
    via Michael's message capture and formatting code.

    Code cribbed from ion.services.dm.distribution.notify_web_monitor
    """
    def plc_init(self):
        Process.plc_init(self)

        self.lec = LazyEyeClient(proc=self, target='lazyeye')
        self.rootpage = RootPage(self.lec)
        self.site = Site(self.rootpage)
        reactor.listenTCP(WEB_PORT, self.site)
        log.info('Listening on http://%s:%d/' % (HOSTNAME, WEB_PORT))

        # For simplicity, serve mscgen output from current directory using the
        # supplied server, on a different port.
        self.imgs = static.File('.')
        self.img_site = Site(self.imgs)
        reactor.listenTCP(IMAGE_PORT, self.img_site
        )

# Spawn off the process using the module name
factory = ProcessFactory(LazyEyeMonitor)
