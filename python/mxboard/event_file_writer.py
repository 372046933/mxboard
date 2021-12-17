# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Writes events to disk in a logdir."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import os.path
import socket
import threading
import time

import six

from .proto import event_pb2
from .record_writer import RecordWriter
from . import file_io

logging.basicConfig()


class EventsWriter(object):
    """Writes `Event` protocol buffers to an event file. This class is ported from
    EventsWriter defined in
    https://github.com/tensorflow/tensorflow/blob/master/tensorflow/core/util/events_writer.cc"""
    def __init__(self, file_prefix, verbose=True):
        """
        Events files have a name of the form
        '/file/path/events.out.tfevents.[timestamp].[hostname][file_suffix]'
        """
        self._file_prefix = file_prefix
        self._file_suffix = ''
        self._filename = None
        self._recordio_writer = None
        self._num_outstanding_events = 0
        self._logger = None
        if verbose:
            self._logger = logging.getLogger(__name__)
            self._logger.setLevel(logging.INFO)

    def __del__(self):
        self.close()

    def _init_if_needed(self):
        if self._recordio_writer is not None:
            return
        self._filename = self._file_prefix + ".out.tfevents." + str(time.time())[:10]\
                         + "." + socket.gethostname() + self._file_suffix
        self._recordio_writer = RecordWriter(self._filename)
        if self._logger is not None:
            self._logger.info('successfully opened events file: %s', self._filename)
        event = event_pb2.Event()
        event.wall_time = time.time()
        self.write_event(event)
        self.flush()  # flush the first event

    def init_with_suffix(self, file_suffix):
        """Initializes the events writer with file_suffix"""
        self._file_suffix = file_suffix
        self._init_if_needed()

    def write_event(self, event):
        """Appends event to the file."""
        # Check if event is of type event_pb2.Event proto.
        if not isinstance(event, event_pb2.Event):
            raise TypeError("expected an event_pb2.Event proto, "
                            " but got %s" % type(event))
        return self._write_serialized_event(event.SerializeToString())

    def _write_serialized_event(self, event_str):
        if self._recordio_writer is None:
            self._init_if_needed()
        self._num_outstanding_events += 1
        self._recordio_writer.write_record(event_str)

    def flush(self):
        """Flushes the event file to disk."""
        if self._num_outstanding_events == 0 or self._recordio_writer is None:
            return
        self._recordio_writer.flush()
        if self._logger is not None:
            self._logger.info('wrote %d %s to disk', self._num_outstanding_events,
                              'event' if self._num_outstanding_events == 1 else 'events')
        self._num_outstanding_events = 0

    def close(self):
        """Flushes the pending events and closes the writer after it is done."""
        self.flush()
        if self._recordio_writer is not None:
            self._recordio_writer.close()
            self._recordio_writer = None


def _get_sentinel_event():
    """Generate a sentinel event for terminating worker."""
    return event_pb2.Event()


class EventFileWriter(object):
    """This class is adapted from EventFileWriter in Tensorflow:
    https://github.com/tensorflow/tensorflow/blob/master/tensorflow/python/summary/writer/event_file_writer.py
    Writes `Event` protocol buffers to an event file.
    The `EventFileWriter` class creates an event file in the specified directory,
    and asynchronously writes Event protocol buffers to the file. The Event file
    is encoded using the tfrecord format, which is similar to RecordIO.
    """

    EVENT_SIZE_LIMIT = 1 << 30
    EVENT_FILE_SIZE_LIMIT = 1 << 31
    EVENT_DIR_SIZE_LIMIT = 1 << 32
    EVENT_FILE_NAME_PREFIX = 'events.out.tfevents.'

    def __init__(self, logdir, max_queue=10, flush_secs=120, filename_suffix=None, verbose=True):
        """Creates a `EventFileWriter` and an event file to write to.
        On construction the summary writer creates a new event file in `logdir`.
        This event file will contain `Event` protocol buffers, which are written to
        disk via the add_event method.
        The other arguments to the constructor control the asynchronous writes to
        the event file:
        """
        self._logdir = logdir
        file_io.makedirs(logdir)
        parse_result = six.moves.urllib.parse.urlparse(self._logdir)
        self._scheme = parse_result.scheme
        self._path = parse_result.path

        self._event_queue = six.moves.queue.Queue(max_queue)
        self._ev_writer = EventsWriter(os.path.join(self._logdir, "events"), verbose=verbose)
        self._flush_secs = flush_secs
        self._sentinel_event = _get_sentinel_event()
        if filename_suffix is not None:
            self._ev_writer.init_with_suffix(filename_suffix)
        self._closed = False
        self._worker = _EventLoggerThread(self._event_queue, self._ev_writer,
                                          self._flush_secs, self._sentinel_event)

        self._worker.start()
        self._total_bytes = 0
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.setLevel(logging.INFO)

    def get_logdir(self):
        """Returns the directory where event file will be written."""
        return self._logdir

    def reopen(self):
        """Reopens the EventFileWriter.
        Can be called after `close()` to add more events in the same directory.
        The events will go into a new events file.
        Does nothing if the `EventFileWriter` was not closed.
        """
        if self._closed:
            self._worker = _EventLoggerThread(self._event_queue, self._ev_writer,
                                              self._flush_secs, self._sentinel_event)
            self._worker.start()
            self._closed = False

    def add_event(self, event):
        """Adds an event to the event file."""
        # Limit single event size within 1GB
        if event.ByteSize() > self.EVENT_SIZE_LIMIT:
            self._logger.warning('Event size %d larger than %d, event is dropped',
                                 event.ByteSize(), self.EVENT_SIZE_LIMIT)
            return
        if self._closed:
            self._logger.warning('Writer closed, event is dropped')
            return

        # Limit hourly events size within 2GB
        if self._total_bytes + event.ByteSize() > self.EVENT_FILE_SIZE_LIMIT:
            if not self.event_file_span_hour():
                self._logger.warning(
                    'Events file larger than %d, event is dropped',
                    self.EVENT_FILE_SIZE_LIMIT)
            else:
                # Open a new events file writer
                self._logger.info('Will write a new events file')
                self.close()
                self.reopen()
                self.maybe_move_to_archive()
                self._total_bytes = event.ByteSize()
                self._event_queue.put(event)
        else:
            # File size within 2GB, do not check span hour
            self._event_queue.put(event)
            self._total_bytes += event.ByteSize()

    def event_file_span_hour(self):
        if self._total_bytes == 0:
            return False
        if not hasattr(self._ev_writer, '_filename'):
            return False
        time_int = int(time.time())
        # events file name looks line log_dir/events.out.tfevents...
        events_file_time = int(
            self._ev_writer._filename[self._ev_writer._filename.find(
                self.EVENT_FILE_NAME_PREFIX):].split('.')[3])
        return time_int - events_file_time > 3600

    def maybe_move_to_archive(self):
        """# Limit total events file size to 4GB under `self._logdir`
           If limit reached, move those files to `self._logdir + _ + archive`
        """
        if self._scheme == '':
            self._move_local()
        elif self._scheme in ('hdfs', 'viewfs'):
            self._move_hdfs()
        else:
            raise ValueError("Unsupported scheme:%s" % self._scheme)

    def _move_local(self):
        total_bytes = 0
        needs_to_move = []
        events_file_name = list(
            filter(lambda x: x.startswith(self.EVENT_FILE_NAME_PREFIX),
                   os.listdir(self._path)))
        events_file_size = {
            fname: os.path.getsize(os.path.join(self._path, fname))
            for fname in events_file_name
        }
        for _, sz in six.iteritems(events_file_size):
            total_bytes += sz
        if total_bytes <= self.EVENT_DIR_SIZE_LIMIT:
            return

        while total_bytes > self.EVENT_DIR_SIZE_LIMIT:
            total_bytes -= events_file_size[events_file_name[0]]
            needs_to_move.append(events_file_name[0])
            events_file_name.pop(0)
        self._logger.info('Needs to move %s', needs_to_move)
        archive_path = self._path + '_' + 'archive'
        os.makedirs(archive_path, exist_ok=True)
        for fname in needs_to_move:
            self._logger.info('Move %s to %s', os.path.join(self._path, fname),
                              os.path.join(archive_path, fname))
            os.rename(os.path.join(self._path, fname),
                        os.path.join(archive_path, fname))

    def _move_hdfs(self):
        import pyarrow.fs
        total_bytes = 0
        needs_to_move = []
        fs_ = pyarrow.fs.HadoopFileSystem(host='default', port=0)
        events_file_info = list(
            filter(
                lambda x: x.type == pyarrow.fs.FileType.File and x.base_name.
                startswith(self.EVENT_FILE_NAME_PREFIX),
                fs_.get_file_info(
                    pyarrow.fs.FileSelector(self._path, recursive=True))))
        for finfo in events_file_info:
            total_bytes += finfo.size
        if total_bytes <= self.EVENT_DIR_SIZE_LIMIT:
            return

        while total_bytes > self.EVENT_DIR_SIZE_LIMIT:
            total_bytes -= events_file_info[0].size
            needs_to_move.append(events_file_info[0])
            events_file_info.pop(0)
        self._logger.info('Needs to move %s', needs_to_move)
        archive_path = self._path + '_' + 'archive'
        fs_.create_dir(archive_path)
        for finfo in needs_to_move:
            self._logger.info('Move %s to %s', finfo.path,
                              os.path.join(archive_path, finfo.base_name))
            fs_.move(finfo.path, os.path.join(archive_path, finfo.base_name))

    def flush(self):
        """Flushes the event file to disk.
        Call this method to make sure that all pending events have been written to disk.
        """
        self._event_queue.join()
        self._ev_writer.flush()

    def close(self):
        """Flushes the event file to disk and close the file.
        Call this method when you do not need the summary writer anymore.
        """
        if not self._closed:
            self.add_event(self._sentinel_event)
            self.flush()
            self._worker.join()
            self._ev_writer.close()
            self._closed = True


class _EventLoggerThread(threading.Thread):
    """Thread that logs events. Copied from
    https://github.com/tensorflow/tensorflow/blob/master/tensorflow/python/summary/writer/event_file_writer.py#L133"""

    def __init__(self, queue, ev_writer, flush_secs, sentinel_event):
        """Creates an _EventLoggerThread."""
        threading.Thread.__init__(self)
        self.daemon = True
        self._queue = queue
        self._ev_writer = ev_writer
        self._flush_secs = flush_secs
        # The first event will be flushed immediately.
        self._next_event_flush_time = 0
        self._sentinel_event = sentinel_event

    def run(self):
        while True:
            event = self._queue.get()
            if event is self._sentinel_event:
                self._queue.task_done()
                break
            try:
                self._ev_writer.write_event(event)
                # Flush the event writer every so often.
                now = time.time()
                if now > self._next_event_flush_time:
                    self._ev_writer.flush()
                    # Do it again in two minutes.
                    self._next_event_flush_time = now + self._flush_secs
            finally:
                self._queue.task_done()
