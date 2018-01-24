# -*- coding: utf-8 -*-
from io import BytesIO
from msgpack import Unpacker


class Writer:

    def __init__(self, server):
        self.server = server

    def write(self, data):
        self.server._buf.write(data)

    async def drain(self):
        pass

    def close(self):
        pass


class MockRecvServer:

    def __init__(self):
        self._writer = Writer(self)
        self._buf = BytesIO()

    async def factory(self, sender):
        return None, self._writer

    def get_recieved(self):
        self._buf.seek(0)
        return list(Unpacker(self._buf, encoding='utf-8'))
