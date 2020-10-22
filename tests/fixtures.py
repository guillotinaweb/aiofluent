from aiofluent import sender
from tests import mockserver

import pytest


@pytest.fixture(scope="function")
async def mock_server():
    server = mockserver.MockRecvServer()
    yield server


@pytest.fixture(scope="function")
def test_sender(mock_server):
    sender.setup('app', connection_factory=mock_server.factory)
    yield sender
    sender.close()
    sender._set_global_sender(None)


@pytest.fixture(scope="function")
def mock_sender(mock_server):
    msender = sender.FluentSender(
        tag='test', connection_factory=mock_server.factory)
    yield msender
    msender.close()
