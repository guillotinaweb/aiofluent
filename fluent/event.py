# -*- coding: utf-8 -*-

import time

from aiofluent import sender


class Event(object):
    def __init__(self, label, data, **kwargs):
        assert isinstance(data, dict), 'data must be a dict'
        sender_ = kwargs.get('sender', sender.get_global_sender())
        timestamp = kwargs.get('time', int(time.time()))
        sender_.emit_with_time(label, timestamp, data)


class AsyncEvent(object):
    def __init__(self, label, data, **kwargs):
        assert isinstance(data, dict), 'data must be a dict'
        self.label = label
        self.data = data
        self.sender_ = kwargs.get('sender', sender.get_global_sender())
        self.timestamp = kwargs.get('time', int(time.time()))

    async def __call__(self):
        await self.sender_.async_emit_with_time(
            self.label, self.timestamp, self.data)
