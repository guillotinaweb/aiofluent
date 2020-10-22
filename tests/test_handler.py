import aiofluent.handler
from unittest.mock import patch
import asyncio
import logging
import pytest

async def wait_for_queue(handler):
    while handler._queue is None or handler._queue._queue is None:
        await asyncio.sleep(0.01)
    while handler._queue.qsize() > 0:
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_simple(mock_server):
    handler = aiofluent.handler.FluentHandler(
        'app.follow', connection_factory=mock_server.factory)
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger('fluent.test')
    log.setLevel(logging.INFO)
    handler.setFormatter(aiofluent.handler.FluentRecordFormatter())
    log.handlers = []
    log.addHandler(handler)
    log.info({
        'from': 'userA',
        'to': 'userB'
    })
    await wait_for_queue(handler)
    handler.close()
    data = mock_server.get_recieved()
    assert 1 == len(data)
    assert 3 == len(data[0])
    assert 'app.follow' == data[0][0]
    assert 'userA' == data[0][2]['from']
    assert 'userB' == data[0][2]['to']
    assert data[0][1]
    assert isinstance(data[0][1], float)


@pytest.mark.asyncio
async def test_custom_fmt(mock_server):
    handler = aiofluent.handler.FluentHandler(
        'app.follow', connection_factory=mock_server.factory)

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger('fluent.test')
    log.setLevel(logging.INFO)
    handler.setFormatter(
        aiofluent.handler.FluentRecordFormatter(fmt={
            'name': '%(name)s',
            'lineno': '%(lineno)d',
            'emitted_at': '%(asctime)s',
        })
    )
    log.handlers = []
    log.addHandler(handler)
    log.info({'sample': 'value'})
    await wait_for_queue(handler)
    handler.close()

    data = mock_server.get_recieved()
    assert 'name' in data[0][2]
    assert 'fluent.test' == data[0][2]['name']
    assert 'lineno' in data[0][2]
    assert 'emitted_at' in data[0][2]


@pytest.mark.asyncio
async def test_json_encoded_message(mock_server):
    handler = aiofluent.handler.FluentHandler(
        'app.follow', connection_factory=mock_server.factory)

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger('fluent.test')
    log.setLevel(logging.INFO)
    handler.setFormatter(aiofluent.handler.FluentRecordFormatter())
    log.handlers = []
    log.addHandler(handler)
    log.info('{"key": "hello world!", "param": "value"}')
    await wait_for_queue(handler)
    handler.close()

    data = mock_server.get_recieved()
    assert 'key' in data[0][2]
    assert 'hello world!' in data[0][2]['key']


@pytest.mark.asyncio
async def test_unstructured_message(mock_server):
    handler = aiofluent.handler.FluentHandler(
        'app.follow', connection_factory=mock_server.factory)

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger('fluent.test')
    log.setLevel(logging.INFO)
    handler.setFormatter(aiofluent.handler.FluentRecordFormatter())
    log.handlers = []
    log.addHandler(handler)
    log.info('hello %s', 'world')
    await wait_for_queue(handler)
    handler.close()

    data = mock_server.get_recieved()
    assert 'message' in data[0][2]
    assert 'hello world' == data[0][2]['message']


@pytest.mark.asyncio
async def test_unstructured_formatted_message(mock_server):
    handler = aiofluent.handler.FluentHandler(
        'app.follow', connection_factory=mock_server.factory)

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger('fluent.test')
    log.setLevel(logging.INFO)
    handler.setFormatter(aiofluent.handler.FluentRecordFormatter())
    log.handlers = []
    log.addHandler(handler)
    log.info('hello world, %s', 'you!')
    await wait_for_queue(handler)
    handler.close()

    data = mock_server.get_recieved()
    assert 'message' in data[0][2]
    assert 'hello world, you!' == data[0][2]['message']


@pytest.mark.asyncio
async def test_non_string_simple_message(mock_server):
    handler = aiofluent.handler.FluentHandler(
        'app.follow', connection_factory=mock_server.factory)

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger('fluent.test')
    log.setLevel(logging.INFO)
    handler.setFormatter(aiofluent.handler.FluentRecordFormatter())
    log.handlers = []
    log.addHandler(handler)
    log.info(42)
    await wait_for_queue(handler)
    handler.close()

    data = mock_server.get_recieved()
    assert 'message' in data[0][2]


@pytest.mark.asyncio
async def test_non_string_dict_message(mock_server):
    handler = aiofluent.handler.FluentHandler(
        'app.follow', connection_factory=mock_server.factory)

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger('fluent.test')
    log.setLevel(logging.INFO)
    handler.setFormatter(aiofluent.handler.FluentRecordFormatter())
    log.handlers = []
    log.addHandler(handler)
    log.info({42: 'root'})
    await wait_for_queue(handler)
    handler.close()

    data = mock_server.get_recieved()
    # For some reason, non-string keys are ignored
    assert 42 not in data[0][2]


class MockQueueTask:

    def done(self):
        return False

    def cancel(self):
        pass


@pytest.mark.asyncio
async def test_discard_message_over_limit(mock_server):
    handler = aiofluent.handler.FluentHandler(
        'app.follow', connection_factory=mock_server.factory)

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger('fluent.test')
    log.setLevel(logging.INFO)
    handler.setFormatter(
        aiofluent.handler.FluentRecordFormatter(fmt={
            'name': '%(name)s',
            'lineno': '%(lineno)d',
            'emitted_at': '%(asctime)s',
        })
    )
    aiofluent.handler.FluentHandler._queue_task = MockQueueTask()
    aiofluent.handler.FluentHandler._queue = aiofluent.handler.LogQueue(
        asyncio.Queue(maxsize=aiofluent.handler.MAX_QUEUE_SIZE))
    log.handlers = []
    log.addHandler(handler)
    for _ in range(aiofluent.handler.MAX_QUEUE_SIZE):
        log.info({'sample': 'value'})

    qsize = handler._queue.qsize()
    assert qsize >= aiofluent.handler.MAX_QUEUE_SIZE
    log.info({'sample': 'value'})
    # does not add, same queue size...
    assert handler._queue.qsize() == qsize
    handler.close()


@pytest.mark.asyncio
async def test_discard_message_over_limit_does_not_space_stderr(mock_server):
    handler = aiofluent.handler.FluentHandler(
        'app.follow', connection_factory=mock_server.factory)

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger('fluent.test')
    log.setLevel(logging.INFO)
    handler.setFormatter(
        aiofluent.handler.FluentRecordFormatter(fmt={
            'name': '%(name)s',
            'lineno': '%(lineno)d',
            'emitted_at': '%(asctime)s',
        })
    )
    aiofluent.handler.FluentHandler._queue_task = MockQueueTask()
    aiofluent.handler.FluentHandler._queue = aiofluent.handler.LogQueue(
        asyncio.Queue(maxsize=1))
    log.handlers = []
    log.addHandler(handler)
    log.info({'sample': 'value'})

    with patch('aiofluent.handler.sys.stderr.write') as stderr:
        log.info({'sample': 'value'})
        stderr.assert_called()

    with patch('aiofluent.handler.sys.stderr.write') as stderr:
        # should not log this time
        log.info({'sample': 'value'})
        stderr.assert_not_called()

    handler.close()
