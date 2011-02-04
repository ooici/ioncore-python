#!/usr/bin/env python

"""
@file ion/services/dm/distribution/notification.py
@author Dave Foster <dfoster@asascience.com>
@brief Notification classes for doing notification (logging + otherwise)
"""

NOTIFY_EXCHANGE_SPACE = 'notify_exchange'
NOTIFY_EXCHANGE_TYPE = 'topic'

from ion.core.messaging.receiver import Receiver
from twisted.internet import defer, reactor
from twisted.web import server, resource

from ion.core.object import object_utils
from ion.core.messaging.message_client import MessageClient

import string
try:
    import json
except:
    import simplejson as json

notification_type = object_utils.create_type_identifier(object_id=2304, version=1)
log_notification_type = object_utils.create_type_identifier(object_id=2305, version=1)

class NotificationPublisherReceiver(Receiver):
    def __init__(self, name, **kwargs):
        """
        Constructor override.
        Sets up publisher config for using our notification exchange, used by send.
        Also sets up a MessageClient instance.
        """
        kwargs = kwargs.copy()

        # add our custom exchange stuff to be passed through to the ultimate result of send
        kwargs['publisher_config'] = { 'exchange' : NOTIFY_EXCHANGE_SPACE,
                                       'exchange_type' : NOTIFY_EXCHANGE_TYPE,
                                       'durable': False,
                                       'mandatory': True,
                                       'immediate': False,
                                       'warn_if_exists': False }

        Receiver.__init__(self, name, **kwargs)

        self._msgclient = MessageClient(proc=kwargs['process'])

class LoggingPublisherReceiver(NotificationPublisherReceiver):
    """
    Publisher for log messages.
    The Python logging handler creates one of these at an appropriate time.
    """

    def log(self, record, **kwargs):
        """
        Sends a log message to an exchange.
        """
        def complete_send(result, record):
            # result are a list of tuples of the form (success, result)
            # the internal results are tuples of (msg_repo, msg_object)

            # ensure success first
            assert [x[0] for x in result] == [True, True]

            # get both message objects
            not_msg, log_not_msg = [x[1] for x in result]

            log_not_msg = not_msg.CreateObject(log_notification_type)

            # fill them with record details
            #not_msg.body = record.getMessage()

            log_not_msg.message = record.getMessage()
            log_not_msg.levelname = record.levelname
            log_not_msg.levelno = record.levelno
            log_not_msg.asctime = record.asctime
            log_not_msg.createdtime = str(record.created)
            log_not_msg.filename = record.filename
            log_not_msg.funcName = record.funcName
            log_not_msg.lineno = record.lineno
            log_not_msg.module = record.module
            log_not_msg.pathname = record.pathname
            not_msg.additional_data = log_not_msg        # link them

            recipient = "_not.log.%s.%s" % (record.levelname, record.module)

            #print "NOT REALLY SENDING YET", str(log_not_msg)
            # perform send!
            self.send(recipient=recipient,
                      content=not_msg,
                      headers={})

        # first we need to create messages to populate
        d1 = self._msgclient.create_instance(notification_type)
        d2 = self._msgclient.create_instance(log_notification_type)

        # add callback on both so we can continue (can't yield here (maybe))
        dl = defer.DeferredList([d1, d2])
        dl.addCallback(complete_send, record=record)

class LoggingWebResource(resource.Resource):

    class LoggingWebAjaxResource(resource.Resource):
        isLeaf = True
        def __init__(self, msgslice, lastindex):
            resource.Resource.__init__(self)
            self._msgslice = msgslice
            self._lastindex = lastindex

        def render_GET(self, request):
            msgs = ""
            for m in self._msgslice:
                msgs += "<li>%s</li>" % str(m['content'].additional_data.message)

            data = { 'html': msgs,
                     'lastindex': self._lastindex }
            return json.dumps(data);

    page_template = """
        <html>
            <head>
                <script type="text/javascript" src="https://ajax.googleapis.com/ajax/libs/jquery/1.4.4/jquery.min.js"></script>
            </head>
            <body>
                <input id="lastslice" type="hidden" value="0" />
                <input type="button" id="stopupdates" value="Stop" />
                <ul id="log"></ul>
            </body>
            <script type="text/javascript">
                var intervalID;
                $$(document).ready(function() { 
                    intervalID = window.setInterval(function() {
                        req = "/" + $$('input#lastslice').val();
                        $$.getJSON(req, function(data) {
                            $$('ul#log').append(data.html);
                            $$('input#lastslice').val(data.lastindex);
                        });
                    }, 2000);

                    $$('input#stopupdates').click(function() { 
                        window.clearInterval(intervalID);
                    });
                });
            </script>
        </html>
        """
    def __init__(self):
        self._msgs = []
        resource.Resource.__init__(self)

    def getChild(self, name, request):
        try:
            since = int(name)
            return self.LoggingWebAjaxResource(self._msgs[since:], len(self._msgs))
        except ValueError:
            pass

        return self

    def render_GET(self, request):
        template = string.Template(self.page_template)
        return template.substitute()

class LoggingReceiver(Receiver):
    """
    Example log notification receiver.

    Listens for log messages on the notification exchange and prints them.
    Sample of how to look at messages on the notification exchange.
    """

    def __init__(self, *args, **kwargs):
        kwargs = kwargs.copy()
        kwargs['handler'] = self.printlog
        self.loglevel = kwargs.pop('loglevel', 'WARN')
        Receiver.__init__(self, *args, **kwargs)

        self._msgs = []

        self._web = LoggingWebResource()
        self._site = server.Site(self._web)
        reactor.listenTCP(9999, self._site)
        print "Listening on http://localhost:9999"

    @defer.inlineCallbacks
    def on_initialize(self, *args, **kwargs):
        """
        @retval Deferred
        """
        assert self.xname, "Receiver must have a name"
        yield self._init_receiver({}, store_config=True)

    @defer.inlineCallbacks
    def _init_receiver(self, receiver_config, store_config=False):
        receiver_config = receiver_config.copy()
        receiver_config.update( {'exchange': NOTIFY_EXCHANGE_SPACE,
                                 'exchange_type':  NOTIFY_EXCHANGE_TYPE,
                                 'durable': False,
                                 'queue': None,     # make it up for us
                                 'binding_key': "_not.log.idontexist.#",
                                 'routing_key': "_not.log.idontexist.#",
                                 'exclusive': False,
                                 'mandatory': True,
                                 'warn_if_exists' : False,
                                 'no_ack': False,
                                 'auto_delete' : True,
                                 'immediate' : False })

        yield Receiver._init_receiver(self, receiver_config, store_config)

        # add logging bindings to self.consumer
        levels = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL', 'EXCEPTION']
        try:
            idx = levels.index(self.loglevel)
        except ValueError:
            idx = levels[2]

        for level in levels[idx:]:
            self.consumer.backend.queue_bind(queue=self.consumer.queue,
                                             exchange=self.consumer.exchange,
                                             routing_key="_not.log.%s.#" % level,
                                             arguments={})

    def printlog(self, payload, msg):
        self._msgs.append(payload)
        self._web._msgs = self._msgs
        logmsg = payload['content'].additional_data
        print logmsg.levelname, ": (", logmsg.module, "):",logmsg.message
        msg.ack()

