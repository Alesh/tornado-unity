""" Endpoint classes
"""
import os
import queue
import signal
import logging
from time import time
import multiprocessing
import tornado.ioloop
from tornado.concurrent import Future
from .utils import fqc_name


class EndPoint(object):
    """ Interprocess communication endpoint.
    """

    def __init__(self, router, debug=False):
        self._debug = debug
        self._router = router
        self._pendings = dict()
        self._channel = multiprocessing.Queue()

    def start(self, ioloop):
        ioloop.add_handler(self._channel._reader.fileno(),
                           self._on_channel, ioloop.READ)

    def on_message(self, message):
        """ Called when message reseived.
        """
        raise NotImplementedError

    def send_message(self, recipient, message):
        """ Sends message
        """
        try:
            message = (recipient, ('MESSAGE', message))
            self._router.put_nowait(message)
            if self._debug:
                logging.info("Endpoint '%s' sent message: %s", fqc_name(self), message)
        except queue.Full:
            logging.error("Cannot send message to '%s', router query is full", recipient)

    def remote_call(self, recipient, method, *args, **kwargs):
        future = Future()
        future_id = (id(future), time())
        self._pendings[future_id] = future
        try:
            message = (recipient, ('CALL', method, args, kwargs, future_id, fqc_name(self)))
            self._router.put_nowait(message)
            if self._debug:
                logging.info("Endpoint '%s' sent message: %s", fqc_name(self), message)
        except queue.Full as exc:
            future.set_exception(exc)
            logging.error("Cannot remote call to '%s', router query is full", recipient)
        return future

    def _on_channel(self, *args):
        message = self._channel.get_nowait()
        if self._debug:
            logging.info("Endpoint '%s' received message: %s", fqc_name(self), message)
        self._on_message(message)

    def _on_message(self, message):
        if message and message[0] == 'MESSAGE':
            self.on_message(message[1])
        elif message and message[0] == 'FUTURE':
            _, future_id, has_result, result = message
            future = self._pendings.pop(future_id, None)
            if future:
                if has_result:
                    future.set_result(result)
                else:
                    future.set_exception(result)
            else:
                logging.warning('Future with id %s not found but result for ut received', future_id)
        elif message and message[0] == 'CALL':
            _, name, args, kwargs, future_id, recipient = message
            method = getattr(self, name, None)
            has_result = False
            try:
                if method is not None:
                    result = method(*args, **kwargs)
                    has_result = True
                else:
                    raise LookupError("Endpoint '{}' has not method '{}'".format(fqc_name(self), name))
            except Exception as exc:
                result = exc
            try:
                message = (recipient, ('FUTURE', future_id, has_result, result))
                self._router.put_nowait(message)
                if self._debug:
                    logging.info("Endpoint '%s' sent message: %s", fqc_name(self), message)
            except queue.Full:
                logging.error("Cannot send future result to '%s', router query is full", recipient)


class SubProcess(EndPoint):
    """ Base united subprocess
    """

    def __init__(self, config, router, watchdog_check_timeout=12.0):
        self._ioloop = None
        self._config = config
        self._last_ping = time()
        super().__init__(router, self._config.debug)
        self._watchdog_check_timeout = getattr(config, 'watchdog_check_timeout', watchdog_check_timeout)

    @property
    def config(self):
        """ Config container """
        return self._config

    def before_start(self):
        """ Called before start event loop.
        """

    def on_stop(self):
        """ Called on stop event loop.
        """

    def start(self):
        self._ioloop = tornado.ioloop.IOLoop()
        super().start(self._ioloop)
        self._ioloop.add_timeout(time(),
                                 lambda: logging.info("Subprocess %s(%s) has started"
                                                      "", fqc_name(self), os.getpid()))
        self._ioloop.add_timeout(time() + self._watchdog_check_timeout, self._check_ping)
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        self.before_start()
        try:
            self._ioloop.start()
        except KeyboardInterrupt:
            pass
        finally:
            self._ioloop.close()
            self._ioloop = None

    def stop(self):
        self.on_stop()
        if self._ioloop:
            self._ioloop.stop()

    def _handle_signal(self, signum, _):
        if self._ioloop and signum == signal.SIGTERM:
            logging.warning("Subprocess %s(%s) got system signal with number: %s"
                            " and will be stopped", fqc_name(self), os.getpid(), signum)
            self.stop()

    def _on_message(self, message):
        if message:
            self._last_ping = time()
            if message[0] != 'PING':
                super()._on_message(message)

    def _check_ping(self):
        if (time() - self._last_ping) >= self._watchdog_check_timeout:
            logging.warning("Subprocess %s(%s) has terminated by watchdog", fqc_name(self), os.getpid())
            self.stop()
        else:
            self._ioloop.add_timeout(time() + 1.0, self._check_ping)
