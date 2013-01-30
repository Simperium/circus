import gevent
import socket
import unittest
import msgpack

from circus import g


def get_free_ports(num, host=None):
    if not host:
        host = '127.0.0.1'
    sockets = []
    ret = []
    for i in xrange(num):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((host, 0))
        ret.append(s.getsockname()[1])
        sockets.append(s)
    for s in sockets:
        s.close()
    return ret


class DealerTest(unittest.TestCase):
    def test_simple(self):
        port, = get_free_ports(1)
        router = g.Router(port, api=lambda x: x)
        dealer = g.Dealer(port)
        self.assertEqual(dealer.send('hi'), 'hi')

    def test_timeout(self):
        def sleepy(x):
            gevent.sleep(0.2)
            return x
        port, = get_free_ports(1)
        router = g.Router(port, api=sleepy)
        dealer = g.Dealer(port, timeout=0.1)
        self.assertRaises(g.TimeoutException, dealer.send, 'hi')
        gevent.sleep(0.3)


class PushTest(unittest.TestCase):
    def test_simple(self):
        port, = get_free_ports(1)
        class C(object):
            def __call__(self, message):
                self.message = message
                return 'foo'
        api = C()
        pull = g.Pull(port, api=api)
        push = g.Push(port)
        self.assertEqual(push.send('hi'), None)
        gevent.sleep(0.001)
        self.assertEqual(api.message, 'hi')


class APITest(unittest.TestCase):
    def test_dealer(self):
        class Echo(object):
            def remote_echo(self, message):
                return message
        port, = get_free_ports(1)
        server = g.APIServer(g.Router(port), api=Echo())
        client = g.APIClient(g.Dealer(port))
        self.assertEqual(client.echo('hi'), 'hi')

    def test_push(self):
        class Metrics(object):
            def __init__(self):
                self.x = 0
            def remote_incr(self, delta):
                self.x += delta
        metrics = Metrics()
        port, = get_free_ports(1)
        server = g.APIServer(g.Pull(port), api=metrics)
        client = g.APIClient(g.Push(port))
        self.assertEqual(client.incr(3), None)
        gevent.sleep(0.001)
        self.assertEqual(metrics.x, 3)
        del client
        gevent.sleep(0.001)

