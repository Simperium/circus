import os
import uuid
import time
import logging
import signal
import prctl
import shlex
import multiprocessing
import subprocess


try:
    from collections import OrderedDict
except ImportError:
    OrderedDict = dict

import msgpack

import gevent
import gevent.queue
import gevent.pool
import gevent.event


from zmq.core.error import ZMQError
import zmq.green as zmq


logger = logging.getLogger(__name__)


# Exceptions
###############################################################################

class TimeoutException(Exception):
    pass


# Serve
###############################################################################

class Serve(object):
    """
    takes an object which provides:
        - name          name of the service
        - setup()       method to create resources needed for the service
        - teardown()    method to run once the service has stopped
        - children      list of children to stop

            children of a service should provide a stop method, which returns a
            gevent.event.Event that will be set once the child service has
            completed stopping.

    on start, serve will set a copy of it's stop on your service, so your
    service can initiate a stop (see services.redis-shards for an example)
    """
    def __init__(self, service):
        self.service = service
        self.stopping = False

    def start(self, **kw):
        logging.info('starting %s' % self.service.name)

        self.stopped = gevent.event.Event()
        self.stopping = False
        gevent.signal(signal.SIGINT, self.stop)
        gevent.signal(signal.SIGTERM, self.stop)

        # hand service a copy of our stop, so it can initiate a stop
        self.service.stop = self.stop
        self.service.setup(**kw)
        return self

    def stop(self, *a, **kw):
        if not self.stopping:
            def run_teardown():
                shutdowns = [child.stop() for child in self.service.children]
                for shutdown in shutdowns:
                    shutdown.wait()
                if hasattr(self.service, 'teardown'):
                    self.service.teardown()
                logging.info('stopped %s.' % self.service.name)
                self.stopped.set()
            self.stopping = True
            gevent.spawn(run_teardown)

    def wait(self):
        self.stopped.wait()


# Pools
###############################################################################

class ResourcePool(object):
    def __init__(self, factory, name='pool', minsize=None, maxsize=None):
        if not isinstance(maxsize, (int, long)):
            raise TypeError('Expected integer, got %r' % (maxsize, ))
        self.factory = factory
        self.name = name
        self.minsize = minsize
        self.maxsize = maxsize
        self.available = gevent.queue.Queue()
        self.unavailable = set()
        self.created = 0
        self.stopped = False

        self.metrics = Metrics(name=name, status=self.metric_status)

        if minsize:
            items = [self.get() for x in xrange(minsize)]
            for item in items: self.put(item)

    def metric_status(self):
        self.metrics.set('free', self.free())

    def free(self):
        return (self.maxsize - self.created) + self.available.qsize()

    def get(self):
        if self.stopped:
            raise Exception("pool is stopped")
        if self.created >= self.maxsize or self.available.qsize():
            item = self.available.get()
        else:
            self.created += 1
            try:
                item = self.factory()
            except:
                self.created -= 1
                raise
        self.metrics.incr('get')
        self.unavailable.add(item)
        return item

    def put(self, item):
        if not item in self.unavailable:
            raise Exception("unknown item: %s" % item)
        self.metrics.incr('put')
        self.available.put(item)
        self.unavailable.remove(item)
        self.check_stopped()

    def stop(self):
        self.stopped = gevent.event.Event()
        self.check_stopped()
        return self.stopped

    def check_stopped(self):
        if self.stopped:
            if not self.unavailable:
                self.metrics.stop().wait()
                self.stopped.set()


# Metrics
###############################################################################

class Metrics(object):
    def __init__(self, name='metrics', status=lambda: None, span=10):
        self.name = name
        self.status = status
        self.span = span
        self.metrics = OrderedDict()
        self.start = time.time()
        self.g = gevent.spawn(self.main)

    def incr(self, name, value=1):
        self.metrics[name] = self.metrics.get(name, 0) + value

    def clock(self, name):
        self.metrics[name] = round((time.time() - self.start)*1000, 2)

    def set(self, name, value):
        self.metrics[name] = value

    def emit(self):
        self.status()
        self.clock('span')
        logging.info('%s %s' % (
            self.name, ' '.join('%s=%s' % x for x in self.metrics.iteritems())))
        self.start = time.time()
        self.metrics = OrderedDict()

    def main(self):
        while True:
            gevent.sleep(self.span)
            self.emit()

    def stop(self, *args, **kw):
        self.g.kill()
        self.emit()
        self.stopped = gevent.event.Event()
        self.stopped.set()
        return self.stopped

#
# Processes
###############################################################################

class Execv(object):
    """
    launches a managed subprocess via the multiprocessing model
    """
    def __init__(self, command, health_check=None):
        self.stopped = gevent.event.Event()
        self.health_check = \
            health_check or getattr(self, 'health_check', lambda: True)
        self.p = multiprocessing.Process(target=self.launch, args=(command,))
        self.p.start()
        self.g = gevent.Greenlet.spawn(self.main)

    def main(self):
        # multiprocessing.join blocks in c, so we can't use that
        # 'fraid we need to poll
        while True:
            gevent.sleep(1)
            if not self.p.is_alive():
                break
            if not self.health_check():
                break
        self.stopped.set()

    @staticmethod
    def launch(command):
        # terminate the child process when the parent dies
        prctl.prctl(prctl.PDEATHSIG, signal.SIGTERM)
        args = shlex.split(command)
        os.execv(args[0], args)

    def wait(self):
        self.g.join()

    def stop(self):
        def shutdown():
            self.p.terminate() # sends a SIGTERM
            self.wait()
            self.stopped.set()
        if not self.stopped.is_set():
            gevent.spawn(shutdown)
        return self.stopped


class ExecvPiped(object):
    """
    launches a managed subprocess, and exposes the subprocesses stdin and
    stdout
    """
    def __init__(self, command, health_check=None):
        self.stopped = gevent.event.Event()
        self.health_check = \
            health_check or getattr(self, 'health_check', lambda: True)

        args = shlex.split(command)
        self.p = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            preexec_fn=self.bind)

        self.g = gevent.Greenlet.spawn(self.main)

    def write(self, message):
        self.p.stdin.write(message)
        self.p.stdin.flush()

    def read(self, size):
        return self.p.stdout.read(size)

    @staticmethod
    def bind():
        # terminate the child process when the parent dies
        prctl.prctl(prctl.PDEATHSIG, signal.SIGTERM)

    def main(self):
        while True:
            gevent.sleep(3)
            if self.p.poll() != None:
                break
            if not self.health_check():
                break
        self.stopped.set()

    def wait(self):
        self.g.join()

    def stop(self):
        def shutdown():
            try:
                self.p.terminate() # sends a SIGTERM
                self.wait()
            except OSError:
                pass
            self.stopped.set()
        if not self.stopped.is_set():
            gevent.spawn(shutdown)
        return self.stopped


# ZMQ
###############################################################################

if zmq.zmq_version() != '3.2.0':
    raise Exception(
        'zmq version out of sync, aborting to not annihilate all services')


BOTH=3
SEND=2
RECV=1


class Base(object):
    def __init__(self,
            port,
            host=None,
            method=None,
            pool=None,
            name='',
            **kw):
        self.stopped = False

        socket_type = self.__zmq_socket_type__
        if not method:
            method = self.__zmq_method__

        self.multipart = getattr(self, '__zmq_multipart__', False)

        if not host:
            if method == 'bind':
                host = '*'
            else:
                host = '127.0.0.1'

        self.socket_type = socket_type
        self.method = method

        self.port = port
        self.name = name

        if hasattr(self, 'init'):
            self.init(**kw)

        context = zmq.Context.instance()
        self.socket = context.socket(socket_type)
        if port:
            getattr(self.socket, method)("tcp://%s:%s" % (host, port))

        if self.__zmq_direction__ & RECV:
            if not pool:
                pool = gevent.pool.Group()
            self.pool = pool
            self.g = gevent.Greenlet.spawn(self.main)

    def _send(self, message):
        self.debug("send:", message)
        if self.multipart:
            self.socket.send_multipart(message)
        else:
            self.socket.send(message)

    def _recv(self, flags=0):
        if self.multipart:
            message = self.socket.recv_multipart(flags)
        else:
            message = self.socket.recv(flags)
        self.debug("recv:", message)
        self.pool.spawn(self.handle, message)

    def main(self):
        while True:
            self._recv()

    def stop(self, *a, **kw):
        self.debug('stopping')
        self.stopped = gevent.event.Event()
        def shutdown():
            self.g.kill()
            while True:
                try:
                    self._recv(zmq.NOBLOCK)
                except ZMQError:
                    break
            self.socket.close()
            self.pool.join()
            self.debug('stopped.')
            self.stopped.set()
        gevent.spawn(shutdown)
        return self.stopped

    def __str__(self):
        return '  %-20s %s' % (self.name, object.__str__(self))

    def debug(self, *a):
        logger.debug(' '.join([str(x) for x in [self]+list(a)]))

    def info(self, *a):
        logger.info(' '.join([str(x) for x in [self]+list(a)]))


class Pub(Base):
    __zmq_socket_type__ = zmq.PUB
    __zmq_method__ = 'connect'
    __zmq_direction__ = SEND

    def publish(self, message):
        self._send(message)


class Sub(Base):
    __zmq_socket_type__ = zmq.SUB
    __zmq_method__ = 'connect'
    __zmq_direction__ = RECV

    def add(self, value):
        self.socket.setsockopt(zmq.SUBSCRIBE, value)

    def remove(self, value):
        self.socket.setsockopt(zmq.UNSUBSCRIBE, value)


class XPub(Base):
    __zmq_socket_type__ = zmq.XPUB
    __zmq_method__ = 'bind'
    __zmq_direction__ = BOTH

    def init(self):
        self.links = []

    def add(self, x):
        self.links.append(x)

    def handle(self, message):
        for x in self.links: x._send(message)


class XSub(Base):
    __zmq_socket_type__ = zmq.XSUB
    __zmq_method__ = 'bind'
    __zmq_direction__ = BOTH

    def init(self):
        self.links = []

    def add(self, x):
        self.links.append(x)

    def handle(self, message):
        for x in self.links: x._send(message)


class Push(Base):
    __zmq_socket_type__ = zmq.PUSH
    __zmq_method__ = 'connect'
    __zmq_direction__ = SEND
    __zmq_multipart__ = False

    def send(self, message):
        self._send(message)

    def stop(self):
        self.debug('stopping')
        self.stopped = gevent.event.Event()
        self.stopped.set()
        return self.stopped


class Pull(Base):
    __zmq_socket_type__ = zmq.PULL
    __zmq_method__ = 'bind'
    __zmq_direction__ = RECV

    def init(self, api=None):
        self.api = api

    def handle(self, message):
        self.api(message)


class Dealer(Base):
    __zmq_socket_type__ = zmq.DEALER
    __zmq_method__ = 'connect'
    __zmq_direction__ = BOTH
    __zmq_multipart__ = True

    def init(self, timeout=2):
        self._calls = {}
        self.timeout = timeout

    def handle(self, message):
        _id, response = message
        if _id in self._calls:
            self._calls.pop(_id).set(response)
        if self.stopped:
            if not self._calls:
                self.stopped.set()

    def send(self, message):
        if self.stopped:
            raise Exception("stopped.")
        _id = uuid.uuid4().bytes
        event = self._calls[_id] = gevent.event.AsyncResult()
        self._send([_id, message])
        try:
           self._recv(zmq.NOBLOCK)
        except ZMQError, e:
            if e.errno == zmq.EAGAIN: pass
            else: raise
        try:
            ret = gevent.with_timeout(self.timeout, event.get)
        except gevent.timeout.Timeout, e:
            self._calls.pop(_id)
            raise TimeoutException()
        return ret

    def stop(self, *a, **kw):
        if not self.stopped:
            self.debug('stopping')
            self.stopped = gevent.event.Event()
            def shutdown():
                self.debug('stopping, waiting for %s outstanding calls' %
                    len(self._calls))
                # TODO: put a timeout on this
                self.pool.join()
                self.g.kill()
                self.socket.close()
                self.debug('shutdown.')
                self.stopped.set()
            gevent.spawn(shutdown)
        return self.stopped

    def __del__(self):
        self.stop()


class Router(Base):
    __zmq_socket_type__ = zmq.ROUTER
    __zmq_method__ = 'bind'
    __zmq_direction__ = BOTH
    __zmq_multipart__ = True

    def init(self, api=None):
        self.api = api

    def handle(self, request):
        routing = request[:-1]
        message = request[-1]
        response =  self.api(message)
        self._send(routing + [response])


# API
###############################################################################

class APIServer(object):
    def __init__(self, socket, api):
        self.socket = socket
        self.api = api
        self.socket.api = self.handle

    def handle(self, message):
        name, a, kw = msgpack.unpackb(message)
        response =  getattr(self.api, 'remote_%s' % name)(*a, **kw)
        return msgpack.packb(response)

    def stop(self):
        return self.socket.stop()


class APIClient(object):
    def __init__(self, socket):
        self.socket = socket

    def __getattr__(self, name):
        def call_remote(*a, **kw):
            message = msgpack.packb((name, a, kw))
            response = self.socket.send(message)
            if response != None:
                return msgpack.unpackb(response)
        return call_remote

    def stop(self):
        return self.socket.stop()

    def __del__(self):
        self.stop()
