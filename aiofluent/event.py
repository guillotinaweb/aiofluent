# -*- coding: utf-8 -*-

from aiofluent import sender

import time


class AsyncEvent(object):
    def __init__(self, label, data, **kwargs):
        assert isinstance(data, dict), 'data must be a dict'
        self.label = label
        self.data = data
        self.sender_ = kwargs.get('sender', sender.get_global_sender())
        self.timestamp = kwargs.get('time', time.time())

    async def __call__(self):
        await self.sender_.async_emit_with_time(
            self.label, self.timestamp, self.data)


async def send_event(label, data, **kwargs):
    assert isinstance(data, dict), 'data must be a dict'
    sender_ = kwargs.get('sender', sender.get_global_sender())
    timestamp = kwargs.get('time', time.time())
    await sender_.async_emit_with_time(label, timestamp, data)
