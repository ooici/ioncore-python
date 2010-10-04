"""
@author Dorian Raymer
@author Michael Meisinger
@brief Python Capability Container shell
"""

import os, sys, tty, termios

from twisted.internet import stdio
from twisted.conch.insults import insults
from twisted.conch import manhole

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

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
         - virtualenv (if used)

        @todo Dependency info will be listed in the setup file
        """
        # self.terminal.reset()
        self.terminal.write('\r\n')
        self.terminal.write('ION Python Capability Container (version %s)\r\n' % (ionconst.VERSION))
        self.terminal.write('%s \r\n' % get_virtualenv())
        self.terminal.write('[container id: %s@%s.%d] \r\n' % (os.getlogin(), os.uname()[1], os.getpid()))
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

    def handle_QUIT(self):
        self.terminal.write('Bye!')
        # write reset to terminal before connection close OR
        # use os.write to fd method below?
        # self.terminal.write("\r\x1bc\r")
        self.terminal.loseConnection()
        # os.write(fd, "\r\x1bc\r")
        # then what?

    def connectionLost(self, reason):
        self.factory.stop()

def makeNamespace():
    from ion.core.cc.shell_api import send, ps, ms, spawn, kill
    from ion.core.cc.container import Container
    from ion.core.id import Id

    namespace = locals()
    return namespace

class Control(object):

    fd = None
    terminalProtocol = None
    oldSettings = None
    class cc(object): pass
    cc = cc() # Extension slot for shell functions

    def start(self, ccService):
        log.info('Shell Start')
        fd = sys.__stdin__.fileno()
        fdout = sys.__stdout__.fileno()

        self.oldSettings = termios.tcgetattr(fd)
        # tty.setraw(fd)
        tty.setraw(fd, termios.TCSANOW) # when=now
        self.fd = fd

        # stdout fd
        outSettings = termios.tcgetattr(fdout)
        outSettings[1] = 3 # do CR
        termios.tcsetattr(fdout, termios.TCSANOW, outSettings)

        namespace = makeNamespace()

        serverProtocol = insults.ServerProtocol(ConsoleManhole, namespace)
        serverProtocol.factory = self

        namespace['ccService'] = ccService
        namespace['tsp'] = serverProtocol
        namespace['cc'] = self.cc

        self.serverProtocol = serverProtocol
        stdio.StandardIO(serverProtocol)

    def stop(self):
        termios.tcsetattr(self.fd, termios.TCSANOW, self.oldSettings)
        log.info('Shell Stop')
        # if terminal write reset doesnt work in handle QUIT, use this
        os.write(self.fd, "\r\x1bc\r")
        log.info('Shell exited. Press Ctrl-c to stop container')

try:
    control
except NameError:
    control = Control()

def _augment_shell(self):
    """
    Dirty little helper functions attached to the 'cc' object in the
    container shell. Quick spawn of processes and send
    """
    from ion.core.cc.shell import control
    if not hasattr(control, 'cc'):
        return
    log.info("Augmenting Container Shell...")
    control.cc.agent = self
    control.cc.config = ion_config
    control.cc.pids = procRegistry.kvs
    control.cc.svcs = processes
    control.cc.procs = receivers
    def send(recv, op, content=None, headers=None, **kwargs):
        if content == None: content = {}
        if recv in control.cc.pids: recv = control.cc.pids[recv]
        d = self.send(recv, op, content, headers, **kwargs)
    control.cc.send = send
    def rpc_send(recv, op, content=None, headers=None, **kwargs):
        if content == None: content = {}
        if recv in control.cc.pids: recv = control.cc.pids[recv]
        d = self.rpc_send(recv, op, content, headers, **kwargs)
    control.cc.rpc_send = rpc_send
    def _get_target(name):
        mod = name
        for p in control.cc.svcs.keys():
            if p.startswith(name):
                mod = control.cc.svcs[p]['class'].__module__
                name = p
                break
        return (mod, name)
    def _get_node(node=None):
        if type(node) is int:
            for cid in self.containers.keys():
                if cid.find(str(node)) >= 0:
                    node = str(self.containers[cid]['agent'])
                    break
        return node
    def spawn(name, node=None, args=None):
        (mod,name) = _get_target(name)
        if node != None:
            node = _get_node(node)
            self.send(node,'spawn',{'module':mod})
        else:
            d = self.spawn_child(ProcessDesc(name=name, module=mod))
    control.cc.spawn = spawn
    def svc():
        for pk,p in control.cc.svcs.iteritems():
            print pk, p['class'].__module__
    control.cc.svc = svc
    def ps():
        for r in control.cc.procs:
            print r.label, r.name
            setattr(control.cc, r.label, r.process)
    control.cc.ps = ps
    def nodes():
        nodes = {}
        for c in self.containers.values():
            nodes[str(c['node'])] = 1
        return nodes.keys()
    control.cc.nodes = nodes
    control.cc.cont = lambda: [str(k) for k in self.containers.keys()]
    control.cc.info = lambda: self.containers[str(Container.id)]
    control.cc.identify = lambda: self.send(self.ann_name, 'identify', '', {'quiet':True})
    control.cc.getinfo = lambda n: self.send(_get_node(n), 'get_info', '')
    control.cc.ping = lambda n: self.send(_get_node(n), 'ping', '', {'quiet':True})
    control.cc.help = "CC Helpers. ATTRS: agent, config, pids, svcs, procs, help FUNC: send, rpc_send, spawn, svc, ps, nodes, cont, info, identify, ping"
