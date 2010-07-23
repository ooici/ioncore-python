"""
@file ion/services/coi/hostsensor/base_daemon.py
@author Brian Fox
@brief Daemonizes python programs

@todo This is generic and should be relocated somewhere more appropriate.
"""

import sys, os, time, atexit
import logging
from signal import SIGTERM 

class Daemon:
    """
    A generic daemon class.  Subclass the Daemon class and override 
    the run() method
    """
    
    
    def __init__(
                 self, 
                 pidfile,
                 logfile, 
                 stdin='/dev/null', 
                 stdout='/dev/null', 
                 stderr='/dev/null'
                 ):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
        self.logfile = logfile
        logging.basicConfig(filename=self.logfile, level=logging.DEBUG)
    
    
    def daemonize(self):
        """
        Do the UNIX double-fork magic, see Stevens' "Advanced 
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """

        logging.debug('Daemonizing process.')
        pid = str(os.getpid())
        f = file(self.pidfile,'w+')
        f.write("%s\n" % pid)
        f.close()
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit first parent
                sys.exit(0) 
                logging.debug("fork #1 succeeded.\n")
        except OSError, e: 
            logging.error("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)


        # decouple from parent environment
        os.chdir("/") 
        os.setsid() 
        os.umask(0) 
    
        # do second fork
        try: 
            pid = os.fork() 
            if pid > 0:
                # exit from second parent
                sys.exit(0) 
                logging.debug("fork #2 succeeded.")
        except OSError, e: 
            logging.error("fork #2 failed: %d (%s)" % (e.errno, e.strerror))
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1) 

    
        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
        logging.debug("Detached stdout, stdin, and stderr")
    
        # write pidfile
        pid = str(os.getpid())
        atexit.register(self.delpid)
        file(self.pidfile,'w+').write("%s\n" % pid)
        logging.debug("Writing pid file: %s = %s" % (self.pidfile, str(pid)))

        
    def delpid(self):
        os.remove(self.pidfile)
        logging.debug("Deleted pid file: %s = %s" % (self.pidfile, str(pid)))


    def start(self):
        """
        Start the daemon
        """
        logging.info("--- Starting daemon ---")
        # Check for a pidfile to see if the daemon already runs
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
    
        if pid:
            logging.error("Pidfile already exists. pid file: %s = %s" % (self.pidfile, str(pid)))
            sys.stderr.write("Pidfile already exists. pid file: %s = %s\n" % (self.pidfile, str(pid)))
            sys.exit(1)
        
        # Start the daemon
        self.daemonize()
        self.run()


    def stop(self):
        """
        Stop the daemon
        """
        logging.info("--- Stopping daemon ---")
        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None
    
        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return # not an error in a restart

        # Try killing the daemon process    
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                sys.exit(1)


    def restart(self):
        """
        Restart the daemon
        """
        logging.info("--- Restarting daemon ---")
        self.stop()
        self.start()


    def status(self, clean=False):
        logging.info("--- Status ---")
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except:
            if (clean):
                print 'Already stopped cleanly'
                sys.exit(0)
            print 'Stopped'
            sys.exit(1)
        try:
            os.kill(pid, 0)
            if (clean):
                print 'Already running cleanly, try stop'
                sys.exit(0)
            print 'Running' 
            sys.exit(0)
        except OSError:
            if (clean):
                try:
                    os.remove(self.pidfile)
                    print "Cleaned"
                    sys.exit(0)
                except OSError:
                    print "Failed to delete " + self.pidfile
                    sys.exit(-1)
                    
            print 'Bad state'
            print 'PID file exits but isn''t matched to a running process'
            print 'PID file: %s\nPID:      %s'%(self.pidfile,pid)
            sys.exit(-1)

    
    def processCommandLine(self):
        if len(sys.argv) == 2:
            if 'start' == sys.argv[1]:
                self.start()
            elif 'stop' == sys.argv[1]:
                self.stop()
            elif 'restart' == sys.argv[1]:
                self.restart()
            elif 'status' == sys.argv[1]:
                self.status()
            elif 'clean' == sys.argv[1]:
                self.status(True)
            else:
                print "Unknown command"
                sys.exit(2)
            sys.exit(0)
        else:
            print "usage: %s start|stop|restart" % sys.argv[0]
            sys.exit(2)


    def run(self):
        """
        You should override this method when you subclass Daemon. It will 
        be called after the process has been daemonized by start() or restart().
        """
        
