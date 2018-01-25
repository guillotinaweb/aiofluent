# -*- coding: utf-8 -*-

from aiofluent import event, sender

import pytest


@pytest.mark.asyncio
async def test_logging(test_sender):
    # XXX: This tests succeeds even if the fluentd connection failed
    # send event with tag app.follow
    await event.send_event('follow', {
        'from': 'userA',
        'to': 'userB'
    })


@pytest.mark.asyncio
async def test_logging_with_timestamp(test_sender):
    # send event with tag app.follow, with timestamp
    await event.send_event('follow', {
        'from': 'userA',
        'to': 'userB'
    }, time=int(0))


@pytest.mark.asyncio
async def test_no_last_error_on_successful_event(test_sender):
    global_sender = sender.get_global_sender()
    await event.send_event('unfollow', {
        'from': 'userC',
        'to': 'userD'
    })

    assert global_sender.last_error is None
    sender.close()
