import os
import functools

import gevent

from circus import g


class Porker(g.ExecvPiped):
    def __init__(self):
        command = os.path.dirname(__file__) + '/porker'
        super(Porker, self).__init__(command)
        gevent.spawn(self.prepare)

    def prepare(self):
        ready = self.read(5)
        assert ready == "READY"
        print "READY", self.p.pid


class Service(object):
    name = 'foo'

    def setup(self):
        self.router = g.Router(6060, api=self.handle)
        self.pool = g.ResourcePool(Porker, minsize=5, maxsize=10)
        self.children = [self.router, self.pool]

    def handle(self, message):
        porker = self.pool.get()
        porker.write(message)
        while True:
            print porker.read(1)
