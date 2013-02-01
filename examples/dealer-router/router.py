from circus import g


class Service(object):
    name = 'router'

    def setup(self):
        self.router = g.Router(6060, api=self.handle)
        self.children = [self.router]

    def handle(self, message):
        print message
        return 'ack: ' + message
