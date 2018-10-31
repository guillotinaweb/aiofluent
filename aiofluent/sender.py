# -*- coding: utf-8 -*-
import asyncio
import msgpack
import socket
import sys
import time
import traceback
import struct

_global_sender = None


def _set_global_sender(sender):
    """ [For testing] Function to set global sender directly
    """
    global _global_sender
    _global_sender = sender


def setup(tag, **kwargs):
    global _global_sender
    _global_sender = FluentSender(tag, **kwargs)


def get_global_sender():
    return _global_sender


def close():
    get_global_sender().close()


async def connection_factory(sender):
    try:
        return await asyncio.wait_for(
            asyncio.open_connection(sender._host, sender._port),
            sender._timeout)
    except (asyncio.TimeoutError, asyncio.CancelledError) as ex:
        sys.stderr.write(f'Timeout connecting to fluentd')
        sender.last_error = ex
    except Exception as ex:
        sys.stderr.write(f'Unknown error connecting to fluentd')
        sender.last_error = ex


class EventTime(msgpack.ExtType):
    def __new__(cls, timestamp):
        seconds = int(timestamp)
        nanoseconds = int(timestamp % 1 * 10 ** 9)
        return super(EventTime, cls).__new__(
            cls,
            code=0,
            data=struct.pack(">II", seconds, nanoseconds),
        )


class FluentSender(object):
    def __init__(self,
                 tag,
                 host='localhost',
                 port=24224,
                 bufmax=1 * 1024 * 1024,
                 timeout=3,
                 verbose=False,
                 buffer_overflow_handler=None,
                 retry_timeout=30,
                 connection_factory=connection_factory,
                 nanosecond_precision=False,
                 **kwargs):

        self._tag = tag
        self._host = host
        self._port = port
        self._bufmax = bufmax
        self._timeout = timeout
        self._verbose = verbose
        self._buffer_overflow_handler = buffer_overflow_handler
        self._nanosecond_precision = nanosecond_precision

        self._pendings = None
        self._reader = None
        self._writer = None
        self._retry_timeout = retry_timeout

        self._last_error = None
        self._last_error_time = 0
        self._lock = None

        self._connection_factory = connection_factory

    @property
    def lock(self):
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def get_writer(self):
        async with self.lock:
            if self._writer is not None:
                return self._writer

            if (self._last_error_time + self._retry_timeout) > time.time():
                return

            result = await self._connection_factory(self)
            if result:
                self._reader, self._writer = result
            return self._writer

    async def async_emit(self, label, data, timestamp=None):
        if timestamp is not None:
            ev_time = timestamp
        elif self._nanosecond_precision:
            ev_time = EventTime(time.time())
        else:
            ev_time = int(time.time())
        return await self.async_emit_with_time(label, ev_time, data)

    async def async_emit_with_time(self, label, timestamp, data):
        if self._nanosecond_precision and isinstance(timestamp, float):
            timestamp = EventTime(timestamp)
        try:
            bytes_ = self._make_packet(label, timestamp, data)
        except Exception as e:
            self.last_error = e
            bytes_ = self._make_packet(label, timestamp,
                                       {"level": "CRITICAL",
                                        "message": "Can't output to log",
                                        "traceback": traceback.format_exc()})
        return await self._async_send(bytes_)

    def _make_packet(self, label, timestamp, data):
        if label:
            tag = '.'.join((self._tag, label))
        else:
            tag = self._tag
        packet = (tag, timestamp, data)
        if self._verbose:
            print(packet)
        return msgpack.packb(packet)

    async def _async_send(self, bytes_):
        try:
            result = await self._async_send_internal(bytes_)
        except Exception:
            result = None
        return result

    async def _async_send_internal(self, bytes_):
        # buffering
        if self._pendings:
            self._pendings += bytes_
            bytes_ = self._pendings

        try:
            writer = await self.get_writer()
            if writer is None:
                self.clean(bytes_)
                return False
            writer.write(bytes_)
            await asyncio.wait_for(writer.drain(), self._timeout)

            self._pendings = None
            self._last_error_time = 0
            return True
        except (socket.error, asyncio.TimeoutError,
                asyncio.CancelledError, OSError, BlockingIOError) as e:
            self.last_error = e

            # Connection error, retry connecting
            self.clean(bytes_)
            async with self.lock:
                self.close()
            self._writer = self._reader = None
            return False
        except Exception as ex:
            self.last_error = ex
            sys.stderr.write('Unhandled exception sending data')
            self.clean(bytes_)
            return False

    def clean(self, bytes_=b''):
        if self._pendings and (len(self._pendings) > self._bufmax):
            self._call_buffer_overflow_handler(self._pendings)
            self._pendings = None
        else:
            self._pendings = bytes_

    def _call_buffer_overflow_handler(self, pending_events):
        try:
            if self._buffer_overflow_handler:
                self._buffer_overflow_handler(pending_events)
        except Exception:
            # User should care any exception in handler
            pass

    @property
    def last_error(self):
        return self._last_error

    @last_error.setter
    def last_error(self, err):
        if err is not None:
            self._last_error = err
            self._last_error_time = time.time()
        else:
            self.clear_last_error()

    def clear_last_error(self):
        self._last_error = None
        self._last_error_time = 0

    def close(self):
        if self._writer is not None:
            try:
                self._writer.close()
            except RuntimeError:
                # event loop already closed
                pass
            self._reader = None
            self._writer = None
