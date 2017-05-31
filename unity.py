""" Helper classes to build uniting tornado apps
"""
import os
import queue
import signal
import logging
import multiprocessing
from time import time
from functools import partial

import tornado.ioloop
from tornado.util import ObjectDict
import importlib.machinery


class Config(object):
    """ Config container based on module
    """

    def __init__(self, default, defined=None, ):
        def load_module(filename, name):
            loader = importlib.machinery.SourceFileLoader(name, filename)
            return loader.load_module()

        self._defined = None
        if defined is not None:
            self._defined = load_module(defined, defined)
        self._default = load_module(default, default)

    def __getattr__(self, name):
        if name.startswith('_'):
            return self.__getattribute__(name)
        else:
            if hasattr(self._defined, name):
                return getattr(self._defined, name)
            return getattr(self._default, name)


class SubProcess(object):
    """ Base united subprocess
    """

    def __init__(self, config, router):
        self.ioloop = None
        self.config = config
        self._router = router
        self._last_ping = time()
        self._channel = multiprocessing.Queue()
        self._watchdog_timeout = getattr(self.config, 'subprocess_watchdog_timeout', 5)

    def before_start(self):
        """ Called before start event loop.
        """

    def on_stop(self):
        """ Called on stop event loop.
        """

    def send_message(self, recipient, message):
        """ Sends message
        """
        try:
            self._router.put((recipient, message), block=False)
        except queue.Full:
            logging.error("Cannot send message to '%s', router query is full", recipient)

    def on_message(self, message):
        """ Called when message reseived.
        """
        raise NotImplementedError

    def _check_ping(self):
        if (time() - self._last_ping) >= self._watchdog_timeout:
            logging.warning("Subprocess '%s' pid: %s has terminated by watchdog", self._name, os.getpid())
            self._stop()
        else:
            self.ioloop.add_timeout(time() + 1.0, self._check_ping)

    def _start(self):
        self.ioloop = tornado.ioloop.IOLoop()
        self.ioloop.add_handler(self._channel._reader.fileno(),
                                self._on_channel, self.ioloop.READ)
        self.ioloop.add_timeout(time(),
                                lambda: logging.info("Subprocess '%s' has started with"
                                                     " pid: %s", self._name, os.getpid()))
        self.ioloop.add_timeout(time() + 1.0, self._check_ping)
        self.before_start()
        self.ioloop.start()
        self.ioloop.close()
        self.ioloop = None

    def _stop(self):
        self.on_stop()
        if self.ioloop:
            self.ioloop.stop()

    @property
    def _name(self):
        return self.__class__.__qualname__

    def _on_channel(self, *args):
        message = self._channel.get(block=False)
        self._last_ping = time()
        if 'ping' not in message:
            self.on_message(message)


class Service(object):
    """ Multiprocessing uniting service
    """

    def __init__(self, config):
        self.ioloop = None
        self.config = config
        self._webapp = None
        self._pendings = dict()
        self._subprocesses = dict()
        self._router = multiprocessing.Queue()
        self._stop = False

    def start(self, webapp_class, *subprocess_clases):
        """ Starts service
        """
        config = self.config
        self.ioloop = tornado.ioloop.IOLoop()
        for class_ in subprocess_clases:
            self._create_process(class_.__qualname__, class_)
        self.ioloop.add_handler(self._router._reader.fileno(),
                                self._on_router, self.ioloop.READ)

        self._webapp = webapp_class(self)
        self.before_start()
        self._webapp.listen(config.port, config.host)
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        self.ioloop.add_timeout(time(),
                                lambda: logging.info("Service has started with pid: %s", os.getpid()))
        self.ioloop.add_timeout(time() + 1.0, self._send_ping)
        self.ioloop.start()
        self.ioloop.close()
        self.ioloop = None
        self._stop = False

    def before_start(self):
        """ Called before start event loop.
        """

    def stop(self):
        """ Stops service
        """
        self._stop = True
        self._webapp = None
        for subprocess in self._subprocesses.values():
            if subprocess.process.is_alive():
                subprocess.do_stop()
                subprocess.process.terminate()
        self.ioloop.stop()

    def _send_ping(self):
        for name in self._subprocesses:
            data = dict(ping=time())
            try:
                self._router.put((name, data), block=False)
            except queue.Full:
                logging.error("Cannot send ping to '%s', router query is full", name)
        self.ioloop.add_timeout(time() + 1.0, self._send_ping)

    def _handle_signal(self, signum, _):
        logging.warning('Got system signal with number: %s, bye', signum)
        self.ioloop.add_callback_from_signal(self.stop)

    def _create_process(self, name, class_, *args):
        """ Subprocess creater and controller.
        """
        if self._stop:
            return
        if class_ is None:
            if name in self._subprocesses:
                class_ = self._subprocesses[name].get('class_')
        assert class_ is not None
        if name not in self._subprocesses:
            self._subprocesses[name] = ObjectDict(class_=class_, process=None)
        # Kill process with the same name if exists
        if self._subprocesses[name].process is not None:
            process = self._subprocesses[name].process
            logging.error("Subprocess '%s' with pid: %s is restarting", name, process.pid)
            if process.is_alive():
                process.terminate()
            if 'sentinel' in self._subprocesses[name]:
                self.ioloop.remove_handler(self._subprocesses[name].sentinel)
            del self._subprocesses[name]
            self._subprocesses[name] = ObjectDict(class_=class_)
        # Crerate new
        instance = class_(self.config, self._router)
        process = multiprocessing.Process(target=instance._start)
        process.start()
        if process.is_alive():
            sentinel = process.sentinel
            self._subprocesses[name].do_stop = instance._stop
            self._subprocesses[name].channel = instance._channel
            self._subprocesses[name].process = process
            self._subprocesses[name].sentinel = sentinel
            self._subprocesses[name].time = time()
            callback = partial(self._create_process, name, None)
            self.ioloop.add_handler(sentinel, callback, self.ioloop.READ)
        else:
            logging.error("Cannot start subprocess '%s'", name)

    def _on_router(self, *args):
        """ Вызывается при поступлении сообщения в роутер.
        """
        recipient, message = self._router.get(block=False)
        if recipient in self._subprocesses:
            try:
                self._subprocesses[recipient].channel.put(message, block=False)
            except queue.Full:
                logging.warning("Cannot send message to '%s', its query is full", recipient)
        elif recipient == 'Future':
            future_id = message['future_id']
            future = self._pendings.pop(future_id)
            future.set_result(message['result'])
        else:
            logging.warning("Cannot route message: %s", [recipient, message])
