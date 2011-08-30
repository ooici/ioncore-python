"""
@author Dorian Raymer
@author Michael Meisinger
@author Dave Foster <dfoster@asascience.com>
@brief Python Capability Container shell
"""

import os
import re
import math
import sys 
import tty
import termios
import rlcompleter

from twisted.application import service
from twisted.internet import stdio
from twisted.conch.insults import insults
from twisted.conch import manhole, recvline
from twisted.python import text

from ion.core import ionconst

CTRL_A = '\x01'
CTRL_E = '\x05'
CTRL_R = "\x12"
CTRL_Q = "\x11"
ESC = "\x1b"

PROMPT_HISTORY = { True  : ("><> ", "... "),
                   False : ("--> ", "... ") }

def get_virtualenv():
    if 'VIRTUAL_ENV' in os.environ:
        virtual_env = os.path.join(os.environ.get('VIRTUAL_ENV'),
                        'lib',
                        'python%d.%d' % sys.version_info[:2],
                        'site-packages')
        return "[env: %s]" % virtual_env
    return "[env: system]"

def real_handle_TAB(self):
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
        try:
            # @TODO: this blows up if doing something like from ion.ser<TAB> - needs more smarts.
            matches = completer.attr_matches(search_term)
            matches = list(set(matches))
            matches.sort()
        except:
            matches = []
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

class PreprocessedInterpreter(manhole.ManholeInterpreter):
    """
    """

    def __init__(self, handler, locals=None, filename="<console>", preprocess=None):
        """
        Initializes a PreprocessedInterpreter.

        @param preprocess   A dict mapping Regex expressions (which match lines) to
                            callables. The callables should take a single parameter (a
                            string with the line) and return either None or a string to
                            send to the interpreter. If None is returned, the callable
                            is assumed to have handled it and it is not sent to the
                            interpreter.
        """
        preprocess = preprocess or {}
        self._preprocessHandlers = preprocess
        manhole.ManholeInterpreter.__init__(self, handler, locals, filename)

    def addPreprocessHandler(self, regex, handler):
        self._preprocessHandlers[regex] = handler

    def delPreprocessHandler(self, regex):
        del(self._preprocessHandlers[regex])

    def push(self, line):
        """
        pre parse input lines
        """
        newline = line
        for regex, handler in self._preprocessHandlers.items():
            mo = regex.match(line)
            if mo is not None:
                retval = handler(line)
                if retval is None:
                    return False        # handled, all good
                newline = retval
                break

        #if line and line[-1] == '?':
        #    line = 'obj_info(%s)' % line[:-1]
        return manhole.ManholeInterpreter.push(self, newline)


class ConsoleManhole(manhole.ColoredManhole):
    ps = PROMPT_HISTORY[True]

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
        self.history_append = True      # controls appending of history
        self.historysearch = False
        self.historysearchbuffer = []
        self.historyFail = False # self.terminal.reset()

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
        self.printHistoryAppendStatus()
        self.terminal.write('\r\n')
        self.terminal.write(self.ps[self.pn])
        self.setInsertMode()

    handle_TAB = real_handle_TAB

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

    def handle_CTRLQ(self):
        self.history_append = not self.history_append
        self.ps = PROMPT_HISTORY[self.history_append]
        self.printHistoryAppendStatus()
        self.drawInputLine()

    def printHistoryAppendStatus(self):
        self.terminal.write('\r\n')
        self.terminal.write('History appending is ')
        if self.history_append:
            self.terminal.write('ON')
        else:
            self.terminal.write('OFF')
        self.terminal.write('. Press Ctrl+Q to toggle.\r\n')

    def handle_RETURN(self):
        """
        Handles the Return/Enter key being pressed. We subvert HistoricRecvLine's
        behavior here becuase it is insufficient, and call the only other override
        that matters, in RecvLine.
        """
        self.stopHistorySearch()

        # only add the current line buffer to the history if it a) exists and b) is distinct
        # from the previous history line. You don't want 10 entries of the same thing.
        if self.lineBuffer:
            curLine = ''.join(self.lineBuffer)
            if self.history_append:
                if len(self.historyLines) == 0 or self.historyLines[-1] != curLine:
                    self.historyLines.append(curLine)

        self.historyPosition = len(self.historyLines)
        recvline.RecvLine.handle_RETURN(self)

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

        preprocess = { re.compile(r'^.*\?$') : self.obj_info }
        self.interpreter = PreprocessedInterpreter(self, self.namespace, preprocess=preprocess)

        self.keyHandlers.update({
            CTRL_A: self.handle_HOME,
            CTRL_E: self.handle_END,
            CTRL_R: self.handle_CTRLR,
            CTRL_Q: self.handle_CTRLQ,
            ESC: self.handle_ESC,
            })

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

    def obj_info(self, item, format='print'):
        """Print useful information about item."""
        # Item is a string with the ? trailing, chop it off.
        item = item[:-1]

        # now eval it
        try:
            item = eval(item, globals(), self.namespace)
        except Exception, e:
            self.terminal.write('\r\n')
            self.terminal.write(str(e))
            return None

        if item == '?':
            self.terminal.write('Type <object>? for info on that object.')
            return
        _name = 'N/A'
        _class = 'N/A'
        _doc = 'No Documentation.'
        if hasattr(item, '__name__'):
            _name = item.__name__
        if hasattr(item, '__class__'):
            _class = item.__class__.__name__
        _id = id(item)
        _type = type(item)
        _repr = repr(item)
        if callable(item):
            _callable = "Yes"
        else:
            _callable = "No"
        if hasattr(item, '__doc__'):
            maybe_doc = getattr(item, '__doc__')
            if maybe_doc:
                _doc = maybe_doc
            _doc = _doc.strip()   # Remove leading/trailing whitespace.
        info = {'name':_name, 'class':_class, 'type':_type, 'repr':_repr, 'doc':_doc}
        if format is 'print':
            self.terminal.write('\r\n')
            for k,v in info.iteritems():
                self.terminal.write("%s: %s\r\n" % (str(k.capitalize()), str(v)))

            self.terminal.write('\r\n\r\n')
            return None
        elif format is 'dict':
            raise ValueError("TODO: no work")
            return info

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

    handle_TAB = real_handle_TAB

def makeNamespace():
    #from ion.core.cc.shell_api import send, ps, ms, spawn, kill, info, rpc_send, svc, nodes, identify, makeprocess, ping
    from ion.core.cc.shell_api import info, ps, ms, svc, send, rpc_send, spawn, makeprocess, ping, kill, nodes, identify, get_proc, mping
    #from ion.core.cc.shell_api import *
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
        # if terminal write reset doesnt work in handle QUIT, use this
        os.write(self.fd, "\r\x1bc\r")
        os.write(self.fd, 'Shell exited. Press Ctrl-c to stop container\n')

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
