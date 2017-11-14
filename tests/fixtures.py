from aiofluent import sender
from tests import mockserver

import pytest


@pytest.fixture(scope="function")
def mock_server():
    server = mockserver.MockRecvServer('localhost')
    yield server


@pytest.fixture(scope="function")
def test_sender(mock_server):
    sender.setup('app', port=mock_server.port)
    yield sender
    sender.close()
    sender._set_global_sender(None)


@pytest.fixture(scope="function")
def mock_sender(mock_server):
    msender = sender.FluentSender(tag='test', port=mock_server.port)
    yield msender
    msender.close()
