"""
@author Dorian Raymer
@author Michael Meisinger
@brief Python Capability Container shell
"""

import os, sys, tty, termios
import rlcompleter
import re
import math

from twisted.application import service
from twisted.internet import stdio
from twisted.conch.insults import insults
from twisted.conch import manhole

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ionconst

CTRL_A = '\x01'
CTRL_E = '\x05'

def get_virtualenv():
    if 'VIRTUAL_ENV' in os.environ:
        virtual_env = os.path.join(os.environ.get('VIRTUAL_ENV'),
                        'lib',
                        'python%d.%d' % sys.version_info[:2],
                        'site-packages')
        return "[env: %s]" % virtual_env
    return "[env: system]"


CTRL_R = "\x12"
ESC = "\x1b"

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
        msg = """
    ____                ______                    ____        __  __
   /  _/____  ____     / ____/____  ________     / __ \__  __/ /_/ /_  ____  ____
   / / / __ \/ __ \   / /    / __ \/ ___/ _ \   / /_/ / / / / __/ __ \/ __ \/ __ \ 
 _/ / / /_/ / / / /  / /___ / /_/ / /  /  __/  / ____/ /_/ / /_/ / / / /_/ / / / /
/___/ \____/_/ /_/   \____/ \____/_/   \___/  /_/    \__, /\__/_/ /_/\____/_/ /_/
                                                    /____/                         
"""
        # Make new banners using: http://patorjk.com/software/taag/
        self.terminal.write(msg)
        self.terminal.write('ION Python Capability Container (version %s)\r\n' % (ionconst.VERSION))
        self.terminal.write('%s \r\n' % get_virtualenv())
        self.terminal.write('[container id: %s@%s.%d] \r\n' % (os.getlogin(), os.uname()[1], os.getpid()))
        self.terminal.write('\r\n')
        self.terminal.write(self.ps[self.pn])
        self.setInsertMode()

        self.historysearch = False
        self.historysearchbuffer = []
        self.historyFail = False

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

        # save the last 2500 lines of history to the history buffer
        if not self.namespace['cc'].config['no_history']:
            try:
                outhistory = "\n".join(self.historyLines[-2500:])

                f = open(os.path.join(os.environ["HOME"], '.cchistory'), 'w')
                f.writelines(outhistory)
                f.close()
            except (IOError, TypeError):
                # i've seen sporadic TypeErrors when joining the history lines - complaining
                # about seeing a list when expecting a string. Can't figure out how to reproduce
                # it consistently, but it deals with exiting the shell just after an error in the
                # REPL. In any case, don't worry about it, and don't clobber history.
                pass

        self.factory.stop()

    def handle_CTRLR(self):
        if self.historysearch:
            self.findNextMatch()
        else:
            self.historysearch = True
        self.printHistorySearch()

    def handle_RETURN(self):
        self.stopHistorySearch()
        manhole.ColoredManhole.handle_RETURN(self)

    def handle_BACKSPACE(self):
        if self.historysearch:
            if len(self.historysearchbuffer):
                self.historyFail = False
                self.historysearchbuffer.pop()
                self.printHistorySearch()
            # should vbeep on else here
        else:
            manhole.ColoredManhole.handle_BACKSPACE(self)

    def handle_UP(self):
        self.stopHistorySearch()
        manhole.ColoredManhole.handle_UP(self)

    def handle_DOWN(self):
        self.stopHistorySearch()
        manhole.ColoredManhole.handle_DOWN(self)

    def handle_INT(self):
        self.stopHistorySearch()
        self.historyPosition = len(self.historyLines)
        manhole.ColoredManhole.handle_INT(self)

    def handle_RIGHT(self):
        self.stopHistorySearch()
        manhole.ColoredManhole.handle_RIGHT(self)

    def handle_LEFT(self):
        self.stopHistorySearch()
        manhole.ColoredManhole.handle_LEFT(self)

    def handle_ESC(self):
        self.stopHistorySearch()

    def stopHistorySearch(self):
        wassearch = self.historysearch
        self.historysearch = False
        self.historysearchbuffer = []
        if wassearch:
            self.printHistorySearch()

    def printHistorySearch(self):
        self.terminal.saveCursor()
        self.terminal.index()
        self.terminal.write('\r')
        self.terminal.cursorPos.x = 0
        self.terminal.eraseLine()
        if self.historysearch:
            if self.historyFail:
                self.addOutput("failing-")
            self.addOutput("history-search: " + "".join(self.historysearchbuffer) + "_")
        self.terminal.restoreCursor()

    def findNextMatch(self):
        # search from history search pos to 0, uninclusive

        historyslice = self.historyLines[:self.historyPosition-1]
        cursearch = ''.join(self.historysearchbuffer)

        foundone = False
        historyslice.reverse()
        for i in range(len(historyslice)):
            line = historyslice[i]
            if cursearch in line:
                self.historyPosition = len(historyslice) - i
                self.historysearch = False

                if self.lineBufferIndex > 0:
                    self.terminal.cursorBackward(self.lineBufferIndex)
                self.terminal.eraseToLineEnd()

                self.lineBuffer = []
                self.lineBufferIndex = 0
                self._deliverBuffer(line)

                # set x to matching coordinate
                matchidx = line.index(cursearch)
                self.terminal.cursorBackward(self.lineBufferIndex - matchidx)
                self.lineBufferIndex = matchidx

                self.historysearch = True
                foundone = True
                break

        if not foundone:
            self.historyFail = True

    def characterReceived(self, ch, moreCharactersComing):
        if self.historysearch:
            self.historyFail = False
            self.historyPosition = len(self.historyLines)
            self.historysearchbuffer.append(ch)
            self.findNextMatch()
            self.printHistorySearch()
        else:
            manhole.ColoredManhole.characterReceived(self, ch, moreCharactersComing)

    def connectionMade(self):
        manhole.ColoredManhole.connectionMade(self)
        self.keyHandlers[CTRL_R] = self.handle_CTRLR
        self.keyHandlers[ESC] = self.handle_ESC

        # read in history from history file on disk, set internal history/position
        if not self.namespace['cc'].config['no_history']:
            try:
                f = open(os.path.join(os.environ["HOME"], '.cchistory'), 'r')
                for line in f:
                    self.historyLines.append(line.rstrip('\n'))

                f.close()

                self.historyPosition = len(self.historyLines)
            except IOError:
                pass

class DebugManhole(manhole.Manhole):
    ps = ('<>< ', '... ')

    def connectionMade(self):
        manhole.Manhole.connectionMade(self)
        self.keyHandlers.update({
            CTRL_A: self.handle_HOME,
            CTRL_E: self.handle_END,
            })

    def initializeScreen(self):
        self.terminal.write('Ion Remote Container Shell\r\n')
        self.terminal.write('\r\n')
        self.terminal.write('%s \r\n' % get_virtualenv())
        self.terminal.write('[host: %s] \r\n' % (os.uname()[1],))
        self.terminal.write('[cwd: %s] \r\n' % (os.getcwd(),))
        self.terminal.write('\r\n')
        self.terminal.write(self.ps[self.pn])
        self.setInsertMode()

    def terminalSize(self, width, height):
        self.width = width
        self.height = height

    def handle_QUIT(self):
        self.terminal.loseConnection()

    def handle_TAB(self):
        completer = rlcompleter.Completer(self.namespace)
        def _no_postfix(val, word):
            return word
        completer._callable_postfix = _no_postfix
        head_line, tail_line = self.currentLineBuffer()
        search_line = head_line
        cur_buffer = self.lineBuffer
        cur_index = self.lineBufferIndex

        def find_term(line):
            chrs = []
            attr = False
            for c in reversed(line):
                if c == '.':
                    attr = True
                if not c.isalnum() and c not in ('_', '.'):
                    break
                chrs.insert(0, c)
            return ''.join(chrs), attr

        search_term, attrQ = find_term(search_line)

        if not search_term:
            return manhole.Manhole.handle_TAB(self)

        if attrQ:
            matches = completer.attr_matches(search_term)
            matches = list(set(matches))
            matches.sort()
        else:
            matches = completer.global_matches(search_term)

        def same(*args):
            if len(set(args)) == 1:
                return args[0]
            return False

        def progress(rem):
            letters = []
            while True:
                to_compare = []
                for elm in rem:
                    if not elm:
                        return letters
                    to_compare.append(elm.pop(0))
                letter = same(*to_compare)
                if letter:
                    letters.append(letter)
                else:
                    return letters

        def group(l):
            """
            columns is the width the list has to fit into
            the longest word length becomes the padded length of every word, plus a
            uniform space padding. The sum of the product of the number of words
            and the word length plus padding must be less than or equal to the
            number of columns.
            """
            rows, columns = self.height, self.width
            l.sort()
            number_words = len(l)
            longest_word = len(max(l)) + 2
            words_per_row = int(columns / (longest_word + 2)) - 1
            number_rows = int(math.ceil(float(number_words) / words_per_row))

            grouped_words = [list() for i in range(number_rows)]
            for i, word in enumerate(l):
                """
                row, col = divmod(i_word, number_rows)
                row is the index of element in a print column
                col is the number of the col list to append to
                """
                r, c = divmod(i, number_rows)
                grouped_words[c].append(pad(word, longest_word))
            return grouped_words

        def pad(word, full_length):
            padding = ' ' * (full_length - len(word))
            return word + padding

        def max(l):
            last_max = ''
            for elm in l:
                if len(elm) > len(last_max):
                    last_max = elm
            return last_max

        if matches is not None:
            rem = [list(s.partition(search_term)[2]) for s in matches]
            more_letters = progress(rem)
            n = len(more_letters)
            lineBuffer = list(head_line) + more_letters + list(tail_line)
            if len(matches) > 1:
                groups = group(matches)
                line = self.lineBuffer
                self.terminal.nextLine()
                self.terminal.saveCursor()
                for row in groups:
                    s = '  '.join(map(str, row))
                    self.addOutput(s, True)
                if tail_line:
                    self.terminal.cursorBackward(len(tail_line))
                    self.lineBufferIndex -= len(tail_line)
            self._deliverBuffer(more_letters)


def makeNamespace():
    #from ion.core.cc.shell_api import send, ps, ms, spawn, kill, info, rpc_send, svc, nodes, identify, makeprocess, ping
    from ion.core.cc.shell_api import *
    from ion.core.id import Id

    namespace = locals()
    return namespace


class Control(object):

    fd = None
    serverProtocol = None
    oldSettings = None

    # A dict with the locals() of the shell, once started
    namespace = None
    # A dict to collect the shell variables before shell is started
    pre_namespace = {}

    def start(self, ccService):
        #log.info('Shell Start')
        fd = sys.__stdin__.fileno()
        fdout = sys.__stdout__.fileno()

        self.oldSettings = termios.tcgetattr(fd)
        # tty.setraw(fd)
        tty.setraw(fd, termios.TCSANOW) # when=now
        self.fd = fd

        # stdout fd
        outSettings = termios.tcgetattr(fdout)
        outSettings[1] = termios.OPOST | termios.ONLCR
        termios.tcsetattr(fdout, termios.TCSANOW, outSettings)

        namespace = makeNamespace()

        serverProtocol = insults.ServerProtocol(ConsoleManhole, namespace)
        serverProtocol.factory = self
        self.serverProtocol = serverProtocol

        namespace['control'] = self
        namespace['cc'] = ccService
        namespace['tsp'] = serverProtocol
        namespace.update(self.pre_namespace)

        stdio.StandardIO(serverProtocol)
        self.namespace = self.serverProtocol.terminalProtocol.namespace
        from ion.core.cc import shell_api
        shell_api.namespace = self.namespace
        shell_api._update()

    def stop(self):
        termios.tcsetattr(self.fd, termios.TCSANOW, self.oldSettings)
        #log.info('Shell Stop')
        # if terminal write reset doesnt work in handle QUIT, use this
        os.write(self.fd, "\r\x1bc\r")
        log.info('Shell exited. Press Ctrl-c to stop container')

    def add_term_name(self, key, value):
        if self.namespace:
            log.debug("adding to pre_namespace: %s = %s" % (str(key), str(value)))
            self.namespace[key] = value
        else:
            log.debug("adding to namespace: %s = %s" % (str(key), str(value)))
            self.pre_namespace[key] = value

try:
    control
except NameError:
    control = Control()

class STDIOShell(service.Service):

    def __init__(self, ccService):
        self.ccService = ccService
        #self.shell_control = Control()
        self.shell_control = control

    def startService(self):
        service.Service.startService(self)
        self.shell_control.start(self.ccService)

    def stopService(self):
        service.Service.stopService(self)
        # This Control provides no way to externally disconnect the shell
        #self.shell_control.stop()

from twisted.conch.telnet import TelnetTransport, TelnetBootstrapProtocol
from twisted.internet import protocol
from twisted.internet import reactor

class TelnetShell(service.Service):
    """
    @brief Debugging tool for inspect the container name space with a
    Manhole interpreter over a network protocol (ssh or telnet).
    """
    _portfilename_base = '.ccdebugport-'

    def __init__(self, namespace=None):
        if namespace is None:
            namespace = {}
        self.namespace = namespace
        self._portfilename = self._portfilename_base + str(os.getpid())

    def _write_port_file(self, port):
        """
        """
        f = open(self._portfilename, 'w')
        f.write(str(port))
        f.close()

    def _remove_port_file(self):
        os.unlink(self._portfilename)

    def _start_listener(self):
        f = protocol.ServerFactory()
        f.protocol = lambda: TelnetTransport(TelnetBootstrapProtocol,
                                            insults.ServerProtocol,
                                            DebugManhole,
                                            self.namespace)
        listeningPort = reactor.listenTCP(0, f)
        self._write_port_file(listeningPort._realPortNumber)


    def startService(self):
        service.Service.startService(self)
        self._start_listener()

    def stopService(self):
        service.Service.stopService(self)
        self._remove_port_file()
