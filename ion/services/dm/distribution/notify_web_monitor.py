#!/usr/bin/env python

"""
@file ion/services/dm/distribution/notify_web_monitor.py
@author Dave Foster <dfoster@asascience.com>
"""

from ion.core.messaging.receiver import Receiver
from twisted.internet import defer, reactor
from twisted.web import server, resource, static
from twisted.web.server import NOT_DONE_YET
from pkg_resources import resource_filename
import os, time

from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient
from ion.core.process.process import Process, ProcessFactory
#from ion.services.dm.distribution.notification import LoggingReceiver

from ion.services.dm.distribution.eventmonitor import EventMonitorServiceClient

EVENTMONITOR_GETDATA_MESSAGE_TYPE       = object_utils.create_type_identifier(object_id=2338, version=1)
EVENTMONITOR_SUBSCRIBE_MESSAGE_TYPE     = object_utils.create_type_identifier(object_id=2335, version=1)
EVENTMONITOR_UNSUBSCRIBE_MESSAGE_TYPE   = object_utils.create_type_identifier(object_id=2337, version=1)

import string
try:
    import json
except:
    import simplejson as json

class EventMonitorWebResource(resource.Resource):

    class AsyncResource(resource.Resource):
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

    class DataRequest(AsyncResource):
        isLeaf = True

        def __init__(self, mc, ec, session_id, timestamp, subscription_ids=None):
            """
            @param  mc          The MessageClient.
            @param  ec          The EventMonitorService client.
            @param  session_id  The user's session id. Passed to the request to the EventMonitorService.
            @param  timestamp   The timestamp to request messages after.
            @param  subscription_ids    Unused. Limit data request to these subscription ids.
            """
            resource.Resource.__init__(self)

            self._mc = mc
            self._ec = ec
            self._session_id = session_id
            self._timestamp = timestamp
            self._subscription_ids = subscription_ids or []

        @defer.inlineCallbacks
        def _do_action(self, request):
            try:
                timestamp = float("".join(self._timestamp))
            except Exception:
                timestamp = 0.0

            msg = yield self._mc.create_instance(EVENTMONITOR_GETDATA_MESSAGE_TYPE)
            msg.session_id  = self._session_id
            msg.timestamp   = str(timestamp)
            # @TODO: subids

            msgdata = yield self._ec.getdata(msg)
            data = []

            for sub in msgdata.data:
                subdata = { 'subscription_id' : sub.subscription_id,
                            'subscription_desc' : sub.subscription_desc,
                            'events' : [] }

                for event in sub.events:
                    # @TODO: totally needs to be generic here
                    evlist = []
                    for propname, propval in event._Properties.iteritems():
                        if propname == "additional_data":
                            continue
                        evlist.append({'class':'',
                                       'content':str(getattr(event, propname)),
                                       'id': propname})

                    for propname, propval in event.additional_data._Properties.iteritems():
                        evlist.append({'class':'',
                                       'content':str(getattr(event.additional_data, propname)),
                                       'id': propname})

                    subdata['events'].append(evlist)

                data.append(subdata)

            # build json response
            response = { 'data': data,
                        'lasttime': time.time() }

            defer.returnValue(json.dumps(response))

    class ControlRequest(AsyncResource):
        isLeaf = True
        def __init__(self, mc, ec, session_id):
            resource.Resource.__init__(self)

            self._mc = mc
            self._ec = ec
            self._session_id = session_id

        @defer.inlineCallbacks
        def _do_action(self, request):

            command = request.postpath.pop(0)

            if command == "sub":

                event_id = request.postpath.pop(0)
                origin = None
                if len(request.postpath) > 0:
                    origin = request.postpath.pop(0)

                msg = yield self._mc.create_instance(EVENTMONITOR_SUBSCRIBE_MESSAGE_TYPE)

                msg.session_id = self._session_id
                msg.event_id = int(event_id)
                if origin:
                    msg.origin = origin

                resp = yield self._ec.subscribe(msg)

                response = {'status':'ok',
                            'subscription_id': resp.subscription_id }

                defer.returnValue(json.dumps(response))

            elif command == "unsub":

                sub_id = None
                if len(request.postpath) > 0:
                    sub_id = request.postpath.pop(0)

                msg = yield self._mc.create_instance(EVENTMONITOR_UNSUBSCRIBE_MESSAGE_TYPE)
                msg.session_id = self._session_id
                if sub_id:
                    msg.subscription_id = sub_id

                yield self._ec.unsubscribe(msg)
                response = {'status':'ok'}
                defer.returnValue(json.dumps(response))

            print "UNKNOWN CTL REQUEST"
            response = {'status':'bad'}
            defer.returnValue(json.dumps(response))

    class GenericRequest(resource.Resource):
        isLeaf = True
        def __init__(self, obj):
            resource.Resource.__init__(self)
            self._obj = obj

        def render_GET(self, request):
            return json.dumps(self._obj)

    page_template = """
        """
    def __init__(self, mc, ec):
        resource.Resource.__init__(self)

        self._mc = mc
        self._ec = ec

        self._mainpage = static.File(os.path.join(os.path.dirname(__file__), "data", "notify_web_monitor.html"))

    def getChild(self, name, request):

        if name == "data":
            return self.DataRequest(self._mc, self._ec, request.getSession().uid, request.postpath)

        elif name == "ctl":
            return self.ControlRequest(self._mc, self._ec, request.getSession().uid)

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

        self._mc = MessageClient(proc=self)
        self._ec = EventMonitorServiceClient(proc=self)

        self._web = EventMonitorWebResource(self._mc, self._ec)
        self._site = server.Site(self._web)
        reactor.listenTCP(9999, self._site)
        print "Listening on http://localhost:9999"

    def handler(self):
        self._web._msgs = self._msgs

factory = ProcessFactory(NotificationWebMonitorService)
