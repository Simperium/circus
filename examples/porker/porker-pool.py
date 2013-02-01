import os
import struct
import functools

import gevent

from circus import g


class Service(object):
    name = 'porker-pool'

    def setup(self):
        self.router = g.Router(6060, api=self.handle)

        command = os.path.dirname(__file__) + '/porker'
        Porker = functools.partial(g.ExecvPiped, command)
        self.pool = g.ResourcePool(Porker, minsize=5, maxsize=10)

        self.children = [self.router, self.pool]

    def handle(self, message):
        porker = self.pool.get()
        try:
            porker.write(struct.pack('L', len(message))+message)
            length, = struct.unpack('L', porker.read(4))
            if not length:
                return ''
            message = porker.read(length)
            return message
        finally:
            self.pool.put(porker)
