#!/usr/bin/env python

"""
@file ion.play.resful_objects.py
@author Paul Hubbard
@date 3/23/11
@see http://jcalderone.livejournal.com/48953.html
@brief RESTful webserver that looks up Ion protocol buffer definitions by object id.
Designed as a Doxygen add-on that will link client APIs back to the protocol definition.

So

GET /id/2307/1

will return the protobuf file with that definition

Might also lookup by name:

GET /search/topic

which returns links to ids having 'topic' in them, case insensitive

Run with buildout Python:
 bin/mypython ion/play/restful_objects.py
 
"""

import logging as log

from twisted.web import resource
from twisted.web.server import Site
from twisted.internet import reactor

from ion.core.object.object_utils import find_type_ids, return_proto_file

TCP_PORT = 2312
HOSTNAME = 'ooici.net'

title = '<html><head><title>Ion object locator</title></head><body>'
graphic = '<a href="http://%s:%d/"><img src="http://ooici.net/global.logo.jpeg" alt="logo"></a>' \
    % (HOSTNAME, TCP_PORT)
searchbox = '''
<h3>Search for objects</h3>
<form action="/search/" method="get" <input name="regex" value="topic" size="64" type="text"/> </form>
<p>
'''
page_header = title + graphic + searchbox

page_footer = '</body></html>'

class IDResource(resource.Resource):
    """
    Return the GPB source file for a given ID.
    @bug ignores the version number (no support in object_utils)
    """
    def __init__(self, id_number_string, version_string):
        resource.Resource.__init__(self)

        self.id = int(id_number_string)
        self.version = int(version_string)
        # Force it to quit looking for children
        self.isLeaf = True
        log.info('ID lookup for ID %d version %d' % (self.id, self.version))

    def render_GET(self, request):
        request.write('<pre>%s</pre>' % return_proto_file(self.id))
        return ''

class RegexResource(resource.Resource):
    """
    Query protobufs, return list of URLs that index into IDResource.
    """
    def __init__(self, regex):
        resource.Resource.__init__(self)
        self.regex = regex
        # Force it to quit looking for children
        self.isLeaf = True

    def _id_to_url(self, id):
        return 'http://%s:%d/id/%d/1' % (HOSTNAME, TCP_PORT, int(id))

    def render_GET(self, request):
        log.debug('Regex search for "%s"' % self.regex)
        idlist = find_type_ids(self.regex)
        header = '<h3>Objects that match "%s"</h3><nl>' % self.regex
        
        footer = '</nl>'
        request.write(page_header)
        request.write(header)

        for entry in idlist:
            request.write('<li><a href="%s">%s</a></li>' % (self._id_to_url(entry), entry))

        request.write(footer)
        request.write(page_footer)
        return ''
    
class StaticNavPage(resource.Resource):
    """
    Glue it together, mostly for the human-readable text and links
    """
    def render_GET(self, request):
        request.write(page_header)
        request.write('<h3>GPB Locator</h3>RESTful object reference, use /id and /search. E.g.<p>')
        request.write('<a href="http://%s:%d/id/10/1">%s:%d/10/1</a> to lookup ID 10<p>' %
                    (HOSTNAME, TCP_PORT, HOSTNAME, TCP_PORT))
        request.write('or<p><a href="http://%s:%d/search/topic">%s:%d/search/topic</a> to search for "topic"' %
                    (HOSTNAME, TCP_PORT, HOSTNAME, TCP_PORT))
        request.write(page_footer)
        return ''

class RootPage(resource.Resource):
    """
    If a child is requested, generate it on the fly (will create a new package record)
    """
    def getChild(self, id_str, request):
        pstr = request.path.split('/')

        if id_str == 'id':
            return IDResource(pstr[2], pstr[3])
        elif id_str == 'search':
            try:
                regex_arg = request.args.get('regex')[0]
                return RegexResource(regex_arg)
            except:
                return RegexResource(pstr[2])
        elif id_str == '':
            return StaticNavPage()
        else:
            return resource.Resource.getChild(self, id_str, request)

def main():
    log.basicConfig(level=log.DEBUG,
        format='%(asctime)s %(levelname)s [%(funcName)s] %(message)s')

    root = RootPage()
    factory = Site(root)
    reactor.listenTCP(TCP_PORT, factory)
    log.info('http://%s:%d/' % (HOSTNAME, TCP_PORT))

if __name__ == '__main__':
    main()
    reactor.run()
