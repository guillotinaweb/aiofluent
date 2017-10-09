# -*- coding: utf-8 -*-
import asyncio
import msgpack
import os
import socket
import threading
import time
import traceback

IS_IPV6 = True if 'IPV6' in os.environ else False

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


class FluentSender(object):
    def __init__(self,
                 tag,
                 host='localhost',
                 port=24224,
                 bufmax=1 * 1024 * 1024,
                 timeout=3.0,
                 verbose=False,
                 buffer_overflow_handler=None,
                 **kwargs):

        self.tag = tag
        self.host = host
        self.port = port
        self.bufmax = bufmax
        self.timeout = timeout
        self.verbose = verbose
        self.buffer_overflow_handler = buffer_overflow_handler

        self.socket = None
        self.pendings = None
        self.lock = threading.Lock()
        self.alock = asyncio.Lock()
        self._last_error_threadlocal = threading.local()

        try:
            self._reconnect()
        except socket.error:
            # will be retried in emit()
            self._close()

    def emit(self, label, data):
        cur_time = int(time.time())
        return self.emit_with_time(label, cur_time, data)

    def emit_with_time(self, label, timestamp, data):
        try:
            bytes_ = self._make_packet(label, timestamp, data)
        except Exception as e:
            self.last_error = e
            bytes_ = self._make_packet(label, timestamp,
                                       {"level": "CRITICAL",
                                        "message": "Can't output to log",
                                        "traceback": traceback.format_exc()})
        return self._send(bytes_)

    async def async_emit(self, label, data, timestamp=None):
        if timestamp is None:
            timestamp = int(time.time())
        return await self.async_emit_with_time(label, timestamp, data)

    async def async_emit_with_time(self, label, timestamp, data):
        try:
            bytes_ = self._make_packet(label, timestamp, data)
        except Exception as e:
            self.last_error = e
            bytes_ = self._make_packet(label, timestamp,
                                       {"level": "CRITICAL",
                                        "message": "Can't output to log",
                                        "traceback": traceback.format_exc()})
        return await self._async_send(bytes_)

    def close(self):
        self.lock.acquire()
        try:
            if self.pendings:
                try:
                    self._send_data(self.pendings)
                except Exception:
                    self._call_buffer_overflow_handler(self.pendings)

            self._close()
            self.pendings = None
        finally:
            self.lock.release()

    def _make_packet(self, label, timestamp, data):
        if label:
            tag = '.'.join((self.tag, label))
        else:
            tag = self.tag
        packet = (tag, timestamp, data)
        if self.verbose:
            print(packet)
        return msgpack.packb(packet)

    def _send(self, bytes_):
        self.lock.acquire()
        try:
            return self._send_internal(bytes_)
        finally:
            self.lock.release()

    async def _async_send(self, bytes_):
        await self.alock.acquire()
        try:
            result = await self._async_send_internal(bytes_)
        except Exception:
            result = None
        finally:
            self.alock.release()
        return result

    def _send_internal(self, bytes_):
        # buffering
        if self.pendings:
            self.pendings += bytes_
            bytes_ = self.pendings

        try:
            self._send_data(bytes_)

            # send finished
            self.pendings = None

            return True
        except socket.error as e:
            self.last_error = e

            # close socket
            self._close()

            # clear buffer if it exceeds max bufer size
            if self.pendings and (len(self.pendings) > self.bufmax):
                self._call_buffer_overflow_handler(self.pendings)
                self.pendings = None
            else:
                self.pendings = bytes_

            return False

    async def _async_send_internal(self, bytes_):
        # buffering
        if self.pendings:
            self.pendings += bytes_
            bytes_ = self.pendings

        try:
            await self._async_send_data(bytes_)

            # send finished
            self.pendings = None

            return True
        except socket.error as e:
            self.last_error = e

            # close socket
            self._close()

            # clear buffer if it exceeds max bufer size
            if self.pendings and (len(self.pendings) > self.bufmax):
                self._call_buffer_overflow_handler(self.pendings)
                self.pendings = None
            else:
                self.pendings = bytes_

            return False

    def _send_data(self, bytes_):
        # reconnect if possible
        self._reconnect()
        # send message
        self.socket.sendall(bytes_)

    async def _async_send_data(self, bytes_):
        # reconnect if possible
        await self._async_reconnect()
        # send message
        loop = asyncio.get_event_loop()
        await loop.sock_sendall(self.socket, bytes_)

    def _reconnect(self):
        if not self.socket:
            if self.host.startswith('unix://'):
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(self.timeout)
                sock.connect(self.host[len('unix://'):])
            else:
                if IS_IPV6 is False:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(self.timeout)
                    sock.connect((self.host, self.port))
                else:
                    sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                    sock.settimeout(self.timeout)
                    sock.connect((self.host, self.port, 0, 0))
            self.socket = sock

    async def _async_reconnect(self):
        loop = asyncio.get_event_loop()
        if not self.socket:
            if self.host.startswith('unix://'):
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(self.timeout)
                await loop.sock_connect(sock, (self.host[len('unix://'):]))
            else:
                if IS_IPV6 is False:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(self.timeout)
                    await loop.sock_connect(sock, ((self.host, self.port)))
                else:
                    sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                    sock.settimeout(self.timeout)
                    await loop.sock_connect(sock, ((self.host, self.port, 0, 0)))
            self.socket = sock

    def _call_buffer_overflow_handler(self, pending_events):
        try:
            if self.buffer_overflow_handler:
                self.buffer_overflow_handler(pending_events)
        except Exception as e:
            # User should care any exception in handler
            pass

    @property
    def last_error(self):
        return getattr(self._last_error_threadlocal, 'exception', None)

    @last_error.setter
    def last_error(self, err):
        self._last_error_threadlocal.exception = err

    def clear_last_error(self, _thread_id=None):
        if hasattr(self._last_error_threadlocal, 'exception'):
            delattr(self._last_error_threadlocal, 'exception')

    def _close(self):
        if self.socket:
            self.socket.close()
        self.socket = None
