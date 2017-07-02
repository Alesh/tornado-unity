""" Helper classes to build uniting tornado apps
"""
import os
import queue
import signal
import logging
import multiprocessing
from time import time

import tornado.log
import tornado.ioloop
from tornado.util import ObjectDict

from .utils import fqc_name
from .configs import ModuleConfig
from .endpoint import SubProcess, EndPoint


class Service(EndPoint):
    """ Multiprocessing uniting service
    """

    def __init__(self, config, logger=None):
        assert isinstance(config, ModuleConfig)
        self._router = multiprocessing.Queue()
        self._subprocesses = dict()
        self._debug = config.debug
        self._stopping = False
        self._config = config
        self._ioloop = None
        self._pid = os.getpid()
        self._stopping = False
        super().__init__(self._router, self._debug)
        self._watchdog_ping_timeout = getattr(config, 'watchdog_ping_timeout', 10.0)
        tornado.log.enable_pretty_logging(logger=logger)

    @property
    def config(self):
        """ Config container """
        return self._config

    def before_start(self):
        """ Called before start event loop.
        """

    def start(self, webapp_class, *subprocess_clases):
        """ Starts service
        """
        all(issubclass(cls, SubProcess) for cls in subprocess_clases)
        if self._pid == os.getpid():
            self._ioloop = tornado.ioloop.IOLoop()
            super().start(self._ioloop)
            for class_ in subprocess_clases:
                self._create_process(fqc_name(class_), class_)
            self._ioloop.add_handler(self._router._reader.fileno(),
                                     self._on_router, self._ioloop.READ)

            webapp = webapp_class(self)
            self.before_start()
            webapp.listen(self.config.port, self.config.host)
            signal.signal(signal.SIGINT, self._handle_signal)
            signal.signal(signal.SIGTERM, self._handle_signal)
            self._ioloop.add_timeout(time(),
                                     lambda: logging.info("Service has started with pid: %s", os.getpid()))
            self._ioloop.add_timeout(time() + self._watchdog_ping_timeout, self._send_ping)
            self._ioloop.start()
        if self._ioloop is not None:
            self._ioloop.close()
            self._ioloop = None
            self._stopping = False

    def stop(self):
        """ Stops service
        """
        if self._ioloop is not None:
            self._stopping = True
            if self._pid == os.getpid():
                for subprocess in self._subprocesses.values():
                    if subprocess.process.is_alive():
                        subprocess.process.terminate()
                        subprocess.process.join()
            self._ioloop.stop()
        self._stopping = False

    def _create_process(self, name, class_, *args):
        if self._pid == os.getpid() and not self._stopping:
            if class_ is None:
                if name in self._subprocesses:
                    class_ = self._subprocesses[name].get('class_')
            assert class_ is not None
            if name not in self._subprocesses:
                self._subprocesses[name] = ObjectDict(class_=class_, process=None)
            # Kill process with the same name if exists
            if self._subprocesses[name].process is not None:
                process = self._subprocesses[name].process
                logging.error("Subprocess %s(%s) is restarting", name, process.pid)
                if process.is_alive():
                    process.terminate()
                del self._subprocesses[name]
                self._subprocesses[name] = ObjectDict(class_=class_)
            # Crerate new
            subprocesses = class_(self.config, self._router)
            process = multiprocessing.Process(target=self._start_process,
                                              args=(subprocesses, name))
            process.start()
            if process.is_alive():
                sentinel = process.sentinel
                self._subprocesses[name].channel = subprocesses._channel
                self._subprocesses[name].process = process

                def setup_respawn(sentinel, *args):
                    self._ioloop.remove_handler(sentinel)
                    self._ioloop.add_timeout(time() + 0.2, self._create_process, name, None)
                self._ioloop.add_handler(sentinel, setup_respawn, self._ioloop.READ)
            else:
                logging.error("Cannot start subprocess '%s'", name)

    def _start_process(self, subprocess, name):
        if self._pid == os.getpid():
            raise RuntimeError('It cannot be called from main process')
        self.stop()
        self._subprocesses.clear()
        self._subprocesses[name] = subprocess
        self._router = None
        self._ioloop = None
        subprocess.start()

    def _send_ping(self):
        for name in self._subprocesses:
            try:
                self._router.put((name, ('PING', )), block=False)
            except queue.Full:
                logging.error("Cannot send ping to '%s', router query is full", name)
        self._ioloop.add_timeout(time() + self._watchdog_ping_timeout, self._send_ping)

    def _handle_signal(self, signum, _):
        if self._ioloop:
            logging.warning('Service got system signal with number: %s and will be stopped', signum)
            self._ioloop.add_callback_from_signal(self.stop)

    def _on_router(self, *args):
        recipient, message = self._router.get(block=False)
        if self._debug:
            logging.info("Router for '%s' received message: %s", recipient, message)
        channel = None
        if recipient in self._subprocesses:
            channel = self._subprocesses[recipient].channel
        elif recipient == fqc_name(self):
            channel = self._channel
        else:
            logging.warning("Cannot route for '%s' message: %s", recipient, message)
            return
        try:
            channel.put(message, block=False)
            if self._debug:
                logging.info("Router sent for '%s' message: %s", recipient, message)
        except queue.Full:
            logging.warning("Cannot send message to '%s', its query is full", recipient)
