# -*- coding: utf-8 -*-
import aiofluent.sender
import socket


def test_no_kwargs():
    aiofluent.sender.setup("tag")
    actual = aiofluent.sender.get_global_sender()
    assert actual.tag == "tag"
    assert actual.host == "localhost"
    assert actual.port == 24224
    assert actual.timeout == 3.0

    from aiofluent.sender import _set_global_sender
    _set_global_sender(None)

def test_host_and_port():
    aiofluent.sender.setup("tag", host="myhost", port=24225)
    actual = aiofluent.sender.get_global_sender()
    assert actual.tag == "tag"
    assert actual.host == "myhost"
    assert actual.port == 24225
    assert actual.timeout == 3.0

    from aiofluent.sender import _set_global_sender
    _set_global_sender(None)

def test_tolerant():
    aiofluent.sender.setup("tag", host="myhost", port=24225, timeout=1.0)
    actual = aiofluent.sender.get_global_sender()
    assert actual.tag == "tag"
    assert actual.host == "myhost"
    assert actual.port == 24225
    assert actual.timeout == 1.0

    from aiofluent.sender import _set_global_sender
    _set_global_sender(None)


def test_simple(mock_sender, mock_server):
    mock_sender.emit('foo', {'bar': 'baz'})
    mock_sender._close()
    data = mock_server.get_recieved()
    assert 1 == len(data)
    assert 3 == len(data[0])
    assert 'test.foo' == data[0][0]
    assert {'bar': 'baz'} == data[0][2]
    assert data[0][1]
    assert isinstance(data[0][1], int)

def test_no_last_error_on_successful_emit(mock_sender, mock_server):
    mock_sender.emit('foo', {'bar': 'baz'})
    mock_sender._close()

    assert mock_sender.last_error is None

def test_last_error_property(mock_sender, mock_server):
    EXCEPTION_MSG = "custom exception for testing last_error property"
    mock_sender.last_error = socket.error(EXCEPTION_MSG)

    assert mock_sender.last_error.args[0] == EXCEPTION_MSG

def test_clear_last_error(mock_sender, mock_server):
    EXCEPTION_MSG = "custom exception for testing clear_last_error"
    mock_sender.last_error = socket.error(EXCEPTION_MSG)
    mock_sender.clear_last_error()

    assert mock_sender.last_error is None
