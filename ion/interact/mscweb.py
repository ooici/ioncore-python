#!/usr/bin/env python

"""
@file ion/interact/mscweb.py
@author Dave Foster <dfoster@asascience.com>
"""

from twisted.internet import defer, reactor
from twisted.web import server, resource, static
from twisted.web.server import NOT_DONE_YET
import os, time

from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient
from ion.core.process.process import Process, ProcessFactory
#from ion.services.dm.distribution.notification import LoggingReceiver
from ion.interact.int_observer import InteractionObserver
from zope.interface import Interface, Attribute, implements
from twisted.python.components import registerAdapter
from twisted.web.server import Session
from pkg_resources import resource_stream

from ion.services.dm.distribution.eventmonitor import EventMonitorServiceClient

import string
try:
    import json
except:
    import simplejson as json

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

# session mixin for storing last requested time so client doesn't have to do it (but could?)
# adapted from http://jcalderone.livejournal.com/53680.html
class ILastData(Interface):
    last_timestamp = Attribute("The last timestamp that was requested")
    last_index = Attribute("The last index into the Observer that was returned")

class LastData(object):
    implements(ILastData)

    def __init__(self, session):
        self.last_timestamp = 0
        self.last_index = 0

# associate lastdata instances with session
registerAdapter(LastData, Session, ILastData)

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

            def_action = defer.maybeDeferred(self._do_action, request)
            def_action.addCallback(finish_req, request)

            return NOT_DONE_YET

    class DataRequest(AsyncResource):
        isLeaf = True

        def __init__(self, io, session, timestamp):
            """
            @param  io          The interaction observer.
            @param  session_id  The user's session.
            @param  timestamp   The timestamp to request messages after.
            """
            resource.Resource.__init__(self)

            self._io = io
            self._session = session
            self._timestamp = timestamp

        def _do_action(self, request):

            # get associated last request
            last_data = ILastData(self._session)

            use_idx = None

            try:
                timestamp = float("".join(self._timestamp))
                #last_data.last_timestamp = timestamp
            except Exception:
                use_idx = last_data.last_index

            if use_idx is not None:
                # get data out of io
                rawdata = self._io.msg_log[use_idx:]
                proc_alias, _, _ = self._io._get_participants(rawdata)
                mscdata = self._io._get_msc_data(rawdata, proc_alias)

                # store last index
                last_data.last_index = len(self._io.msg_log)

                # jsonize this and return it
                return json.dumps(mscdata)
            else:
                # may have a timestamp here from the user
                raise Exception("no can do chief")

#            msg = yield self._mc.create_instance(EVENTMONITOR_GETDATA_MESSAGE_TYPE)
#            msg.session_id  = self._session_id
#            msg.timestamp   = str(timestamp)
#            # @TODO: subids
#
#            msgdata = yield self._ec.getdata(msg)
#            data = []
#
#            for sub in msgdata.data:
#                subdata = { 'subscription_id' : sub.subscription_id,
#                            'subscription_desc' : sub.subscription_desc,
#                            'events' : [] }
#
#                for event in sub.events:
#                    # @TODO: totally needs to be generic here
#                    evlist = []
#                    for propname, propval in event._Properties.iteritems():
#                        if propname == "additional_data":
#                            continue
#                        evlist.append({'class':'',
#                                       'content':str(getattr(event, propname)),
#                                       'id': propname})
#
#                    for propname, propval in event.additional_data._Properties.iteritems():
#                        evlist.append({'class':'',
#                                       'content':str(getattr(event.additional_data, propname)),
#                                       'id': propname})
#
#                    subdata['events'].append(evlist)
#
#                data.append(subdata)
#
#            # build json response
#            response = { 'data': data,
#                        'lasttime': time.time() }
#
#            defer.returnValue(json.dumps(response))

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
    def __init__(self, io):
        resource.Resource.__init__(self)

        self._io = io

        #self._mainpage = static.File(os.path.join(os.path.dirname(__file__), "data", "msc.html"))

    def getChild(self, name, request):

        if name == "data":
            return self.DataRequest(self._io, request.getSession(), request.postpath)

        elif name == "ctl":
            return self.ControlRequest(request.getSession().uid)

        return self

    def render_GET(self, request):
        # TODO: testing only, load every time
        #log.critical(request)
        #log.critical(request.prepath)

        # reload session stored object every time so we can get the data over and over - very useful
        lastdata = ILastData(request.getSession())
        lastdata.last_index = 0

        listpath = list(request.prepath)
        if len(listpath) == 1 and listpath[0] == '':
            listpath[0] = "msc.html"

        try:
            f = resource_stream(__name__, "data/%s" % "/".join(listpath))
            data = f.readlines()
            f.close()
        except IOError:
            request.setResponseCode(404)
            return "404 not found"

        # figure out mime type, in a hacky manner - most browsers don't care and will get it right
        mime = "text/%s"
        if ".js" in listpath[-1]:
            mime = mime % "javascript"
        else:
            mime = mime % "html"

        webdata = static.Data(''.join(data), mime)
        return webdata.render_GET(request)

class MSCWebProcess(Process):
    """
    """

    @defer.inlineCallbacks
    def plc_init(self):
        Process.plc_init(self)

        self._io = InteractionObserver()
        yield self.register_life_cycle_object(self._io)

        self._web = EventMonitorWebResource(self._io)
        self._site = server.Site(self._web, logPath="mscweb.log")
        reactor.listenTCP(9999, self._site)
        print "Listening on http://localhost:9999"

factory = ProcessFactory(MSCWebProcess)
