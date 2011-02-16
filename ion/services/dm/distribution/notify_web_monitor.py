#!/usr/bin/env python

"""
@file ion/services/dm/distribution/notify_web_monitor.py
@author Dave Foster <dfoster@asascience.com>
"""

from ion.core.messaging.receiver import Receiver
from twisted.internet import defer, reactor
from twisted.web import server, resource, static
from pkg_resources import resource_filename
import os, time

from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient
from ion.core.process.process import Process, ProcessFactory
from ion.services.dm.distribution.notification import LoggingReceiver

import string
try:
    import json
except:
    import simplejson as json

class LoggingWebResource(resource.Resource):

    class DataRequest(resource.Resource):
        isLeaf = True
        #def __init__(self, msgslice, lastindex):
        def __init__(self, msgs, request):
            resource.Resource.__init__(self)

            self._msgs = msgs
            self._request = request
            #self._msgslice = msgslice
            #self._lastindex = lastindex

        def render_GET(self, request):
            #msgs = ""
            #for m in self._msgslice:
            #    msgs += "<li>%s</li>" % str(m['content'].additional_data.message)

            #data = { 'html': msgs,
            #         'lastindex': self._lastindex }

            print str(request)
            print str(self._request)
            try:
                since = float("".join(self._request))
            except Exception:
                since = 0.0

            data = []
            for recv, msgs in self._msgs.items():
                thisdata = { 'name': recv.name,
                             'logs': [] }
                for msg in msgs:
                    #data[recv.name].append(str(msg['content'].additional_data.message)) #msgs
                    if float(msg['content'].additional_data.createdtime) > since:
                        thisdata['logs'].append(str(msg['content'].additional_data.message))

                data.append(thisdata)

            response = { 'data': data,
                         'lasttime': time.time() }
            return json.dumps(response);

    class GenericRequest(resource.Resource):
        isLeaf = True
        def __init__(self, obj):
            resource.Resource.__init__(self)
            self._obj = obj

        def render_GET(self, request):
            return json.dumps(self._obj)

    page_template = """
        """
    def __init__(self, parentproc):
        self._msgs = {}
        self._receivers = []
        resource.Resource.__init__(self)
        self._parentproc = parentproc

        #self._mainpage = static.File(resource_filename(__file__, "data/notify_web_monitor.html"))
        self._mainpage = static.File(os.path.join(os.path.dirname(__file__), "data", "notify_web_monitor.html"))

    def getChild(self, name, request):

        if name == "data":
            #try:
            #    since = int(''.join(request.postpath))
            #except ValueError:
            #    since = 0

            #return self.DataRequest(self._msgs[since:], len(self._msgs))
            return self.DataRequest(self._msgs, request.postpath)

        elif name == "ctl":
            action = request.postpath.pop(0)

            if action == "_log":
                print "hi making a log monitor"

                lr = LoggingReceiver("anonhandler%d" % len(self._receivers), process=self._parentproc, loglevel="DEBUG")
                def handle_log(payload, msg):
                    if not self._msgs.has_key(lr):
                        self._msgs[lr] = []

                    self._msgs[lr].append(payload)

                lr.add_handler(handle_log)
                self._receivers.append(lr)
                lr.attach()

                return self.GenericRequest({'status':'ok'})

        return self

    def render_GET(self, request):
        #template = string.Template(self.page_template)
        #return template.substitute()
        # TODO: testing only, load every time
        self._mainpage = static.File(os.path.join(os.path.dirname(__file__), "data", "notify_web_monitor.html"))
        return self._mainpage.render_GET(request)

class NotificationWebMonitorService(Process):
    """
    Provides a webservice that contains > 1 notification related receivers.
    Messages received by contained receivers will be returned on request.
    """

    def plc_init(self):
        Process.plc_init(self)

        self._web = LoggingWebResource(self)
        self._site = server.Site(self._web)
        reactor.listenTCP(9999, self._site)
        print "Listening on http://localhost:9999"

    def handler(self):
        self._web._msgs = self._msgs

factory = ProcessFactory(NotificationWebMonitorService)
