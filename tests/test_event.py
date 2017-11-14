# -*- coding: utf-8 -*-

from aiofluent import event, sender


def test_logging(test_sender):
    # XXX: This tests succeeds even if the fluentd connection failed
    # send event with tag app.follow
    event.Event('follow', {
        'from': 'userA',
        'to': 'userB'
    })


def test_logging_with_timestamp(test_sender):
    # XXX: This tests succeeds even if the fluentd connection failed

    # send event with tag app.follow, with timestamp
    event.Event('follow', {
        'from': 'userA',
        'to': 'userB'
    }, time=int(0))


def test_no_last_error_on_successful_event(test_sender):
    global_sender = sender.get_global_sender()
    event.Event('unfollow', {
        'from': 'userC',
        'to': 'userD'
    })

    assert global_sender.last_error is None
    sender.close()
