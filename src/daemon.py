#!/usr/bin/env python

import sys, os, time, atexit, argparse

from signal import SIGTERM

# Class that implements basic functions of a daemon program
class Daemon:

    def __init__(self, pidfile):
        self.pidfile = pidfile

    # create a subprocess and kill the father
    def daemonize(self):
        os.chdir("/")
        #os.setsid()
        os.umask(0)

        try:
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile,'w+').write("%s\n" % pid)

    def delpid(self):
        os.remove(self.pidfile)


    # start daemon process
    def start(self, options):
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        # daemon already running?
        if pid:
            message = "Daemon alreary running\n"
            sys.stderr.write(message)
            sys.exit(1)

        self.daemonize()

        sys.stdout.write('Starting daemon...\n')
        self.run(options)

    # stop daemon process
    def stop(self, options):
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        # is not daemon running?
        if not pid:
            message = "Daemon is not running\n"
            sys.stderr.write(message)
            return

        sys.stdout.write('Stopping daemon...\n')

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

    # restart daemon process
    def restart(self, options):
        self.stop(options)
        self.start(options)

    # run logical of daemon
    def run(self, options):
        # TODO create logical
        print 'Do nothing'


if __name__ == "__main__":
    # new instance of daemon
    daemon = Daemon('/tmp/sync.pid')

    # defines of available args
    parser = argparse.ArgumentParser(
                          description='Syncronize elasticsearch and cassandra')
    parser.add_argument('op', choices=['start', 'stop', 'restart'])
    parser.add_argument('--time', type=int, default=60,
                        help="Time in seconds for each syncronization")

    # get parsed args
    args = parser.parse_args()

    # call passed operation with passed args
    getattr(daemon, args.op)(args)
