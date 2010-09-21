import os, sys, tty, termios

from twisted.internet import reactor
from twisted.internet import stdio
from twisted.conch import insults
from twisted.conch import manhole

from ion.core import ionconst

def get_virtualenv():
    if 'VIRTUAL_ENV' in os.environ:
        virtual_env = os.path.join(os.environ.get('VIRTUAL_ENV'),
                        'lib',
                        'python%d.%d' % sys.version_info[:2],
                        'site-packages')
        return "[env: %s]" % virtual_env
    return "[env: system]"

class ConsoleManhole(manhole.ColoredManhole):
    ps = ('><> ', '... ')

    def initializeScreen(self):
        """@todo This should show relevant and useful development info:
         - python version
         - dependencies
          o versions
          o install path
          o
         - virtualenv (if used)

        @todo Dependency info will be listed in the setup file
        """
        # self.terminal.reset()
        self.terminal.write('OOI CI COI Capability Container Prototype \r\n')
        self.terminal.write('AyP ><> (Asynchronous Python Shell)\r\n')
        self.terminal.write('%s \r\n' % ionconst.version)
        self.terminal.write('%s \r\n' % get_virtualenv())
        self.terminal.write('[container: %s@%s.%d] \r\n' % (os.getlogin(), os.uname()[1], os.getpid()))
        self.terminal.write('\r\n')
        self.terminal.write(self.ps[self.pn])
        self.setInsertMode()

    def Xhandle_TAB(self):
        completer = rlcompleter.Completer(self.namespace)
        input_string = ''.join(self.lineBuffer)
        reName = "([a-zA-Z_][a-zA-Z_0-9]*)$"
        reAttribute = "([a-zA-Z_][a-zA-Z_0-9]*[.]+[a-zA-Z_.0-9]*)$"
        nameMatch = re.match(reName, input_string)
        attMatch = re.match(reAttribute, input_string)
        if nameMatch:
            matches = completer.global_matches(input_string)
        if attMatch:
            matches = completer.attr_matches(input_string)
        # print matches

    # def handle_INT(self):

    def connectionLost(self, reason):
        reactor.stop()


def main(namespace=locals()):
    try:
        fd = sys.__stdin__.fileno()
        oldSettings = termios.tcgetattr(fd)
        tty.setraw(fd)
        p = insults.insults.ServerProtocol(ConsoleManhole, namespace)
        namespace.update({'__ashell': p})
        stdio.StandardIO(p)
        from twisted.python import log
        # log.startLogging(sys.stdout)
        reactor.run()
    finally:
        termios.tcsetattr(fd, termios.TCSANOW, oldSettings)
        os.write(fd, "\r\x1bc\r")

if __name__ == '__main__':
    from twisted.python.rebuild import rebuild, updateInstance, latestFunction, latestClass
    from ion.core.cc.spawnable import send, ps, ms, spawn, kill
    from ion.core.cc.container import Container
    from ion.core.cc import container
    from twisted.python import log
    reactor.callWhenRunning(container.test)
    main(locals())
