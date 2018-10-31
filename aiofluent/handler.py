# -*- coding: utf-8 -*-

from aiofluent import sender

import asyncio
import json
import logging
import socket
import sys
import time


class FluentRecordFormatter(logging.Formatter, object):
    """ A structured formatter for Fluent.

    Best used with server storing data in an ElasticSearch cluster for example.

    :param fmt: a dict with format string as values to map to provided keys.
    """
    def __init__(self, fmt=None, datefmt=None, style='%'):
        super(FluentRecordFormatter, self).__init__(None, datefmt)

        if not fmt:
            self._fmt_dict = {
                'sys_host': '%(hostname)s',
                'sys_name': '%(name)s',
                'sys_module': '%(module)s',
            }
        else:
            self._fmt_dict = fmt

        self.hostname = socket.gethostname()

    def format(self, record):
        # Compute attributes handled by parent class.
        super(FluentRecordFormatter, self).format(record)
        # Add ours
        record.hostname = self.hostname
        # Apply format
        data = {}
        for key, value in self._fmt_dict.items():
            try:
                data[key] = value % record.__dict__
            except KeyError:
                # we are okay with missing values here...
                pass

        self._structuring(data, record)
        return data

    def usesTime(self):
        return any([value.find('%(asctime)') >= 0
                    for value in self._fmt_dict.values()])

    def _structuring(self, data, record):
        """ Melds `msg` into `data`.

        :param data: dictionary to be sent to fluent server
        :param msg: :class:`LogRecord`'s message to add to `data`.
          `msg` can be a simple string for backward compatibility with
          :mod:`logging` framework, a JSON encoded string or a dictionary
          that will be merged into dictionary generated in :meth:`format.
        """
        msg = record.msg

        if isinstance(msg, dict):
            self._add_dic(data, msg)
        elif isinstance(msg, str):
            try:
                self._add_dic(data, json.loads(str(msg)))
            except ValueError:
                msg = record.getMessage()
                self._add_dic(data, {'message': msg})
        else:
            self._add_dic(data, {'message': msg})

    @staticmethod
    def _add_dic(data, dic):
        for key, value in dic.items():
            if isinstance(key, str):
                data[str(key)] = value


MAX_QUEUE_SIZE = 100


class LogQueue:

    def __init__(self):
        self._queue = None

    async def consume_queue(self, initial_record, handler):
        self._queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self._queue.put_nowait((initial_record, handler, int(time.time())))
        while True:
            record, handler, timestamp = await self._queue.get()
            try:
                await handler.async_emit(record, timestamp)
            except:  # noqa
                sys.stderr.write(
                    f'Error processing log')
            finally:
                self._queue.task_done()

    def qsize(self):
        if self._queue is None:
            return 0
        return self._queue.qsize()

    def put_nowait(self, *args):
        self._queue.put_nowait(*args)


class FluentHandler(logging.Handler):
    '''
    Logging Handler for fluent.
    '''

    # class singletons
    _queue = None
    _queue_task = None

    def __init__(self,
                 tag,
                 host='localhost',
                 port=24224,
                 timeout=3,
                 verbose=False,
                 loop=None,
                 nanosecond_precision=False,
                 **kwargs):
        self.loop = loop
        self.tag = tag
        self.nanosecond_precision = nanosecond_precision
        self.sender = sender.FluentSender(tag,
                                          host=host, port=port,
                                          timeout=timeout, verbose=verbose,
                                          nanosecond_precision=self.nanosecond_precision,
                                          **kwargs)
        logging.Handler.__init__(self)

    def emit(self, record):
        if FluentHandler._queue_task is None or FluentHandler._queue_task.done():
            # the queue should be a singleton, we don't need a task
            # for every log handler
            try:
                FluentHandler._queue = LogQueue()
                FluentHandler._queue_task = asyncio.ensure_future(
                    FluentHandler._queue.consume_queue(record, self), loop=self.loop)
            except RuntimeError:
                sys.stderr.write(
                    'No event loop running to send log to fluentd')
        else:
            try:
                FluentHandler._queue.put_nowait((record, self, int(time.time())))
            except asyncio.QueueFull:
                sys.stderr.write(
                    f'Hit max log queue size({MAX_QUEUE_SIZE}), '
                    'discarding message')
            except AttributeError:
                sys.stderr.write('Error sending async fluentd message')

    async def async_emit(self, record, timestamp=None):
        data = self.format(record)
        return await self.sender.async_emit(None, data, timestamp)

    def close(self):
        self.acquire()
        try:
            self.sender.close()
            logging.Handler.close(self)
            if self._queue_task is not None and not self._queue_task.done():
                try:
                    self._queue_task.cancel()
                except RuntimeError:
                    pass
        finally:
            self.release()
