#coding=utf-8
import contextlib
import logging
import os
import six
import sys

UNSUPPORTED_SCHEME_PATTERN = 'Unsupported scheme %s'
UNSUPPORTED_MODE_PATTERN = 'Unsupported mode %s'


def mkdir(path):
    """Creates a directory with the name given by `path`.

    Args:
      path: string, name of the directory to be created

    Notes: The parent directories need to exist. Use `makedirs`
    instead if there is the possibility that the parent dirs don't exist.

    Raises:
      errors.OpError: If the operation fails.
  """
    parse_result = six.moves.urllib.parse.urlparse(path)
    if parse_result.scheme == '':
        os.mkdir(path)
    elif parse_result.scheme.lower() in ('hdfs', 'viewfs'):
        _mkdir_hdfs(path, parse_result)
    else:
        logging.error(UNSUPPORTED_SCHEME_PATTERN, parse_result.scheme)
        raise ValueError(UNSUPPORTED_SCHEME_PATTERN % path)


def makedirs(path):
    """Recursively creates a directory with the name given by `path`.

    Args:
      path: string, name of the directory to be created

    Raises:
      errors.OpError: If the operation fails.
    """
    parse_result = six.moves.urllib.parse.urlparse(path)
    if parse_result.scheme == '':
        os.makedirs(path)
    elif parse_result.scheme.lower() in ('hdfs', 'viewfs'):
        _mkdir_hdfs(path, parse_result)
    else:
        logging.error(UNSUPPORTED_SCHEME_PATTERN, parse_result.scheme)
        raise ValueError(UNSUPPORTED_SCHEME_PATTERN % path)


def _mkdir_hdfs(path, parse_result):
    if six.PY2:
        _mkdir_hdfs_py2(path, parse_result)
    elif six.PY3:
        _mkdir_hdfs_py3(path, parse_result)


def _mkdir_hdfs_py2(path, parse_result):
    """mkdir on HDFS
    Args:
      parse_result: like urllib.parse.ParseResult, a 6-tuple:
        (scheme, netloc, path, params, query, fragment)
    """
    import pyarrow
    fs = pyarrow.hdfs.connect()
    fs.mkdir(parse_result.path)


def _mkdir_hdfs_py3(path, parse_result):
    """mkdir on HDFS
    Args:
      parse_result: like urllib.parse.ParseResult, a 6-tuple: 
        (scheme, netloc, path, params, query, fragment)
    """
    import pyarrow.fs
    hdfs = pyarrow.fs.HadoopFileSystem(host='default', port=0)
    hdfs.create_dir(parse_result.path, recursive=True)

@contextlib.contextmanager
def open(path, mode):
    """Open an output stream
    """
    parse_result = six.moves.urllib.parse.urlparse(path)
    try:
        fp = None
        if parse_result.scheme == '':
            fp = open(path, mode)
            yield fp
        elif parse_result.scheme.lower() in ('hdfs', 'viewfs'):
            fp = _open_hdfs(path, parse_result, mode)
            yield fp
        else:
            logging.error(UNSUPPORTED_SCHEME_PATTERN, parse_result.scheme)
            raise ValueError(UNSUPPORTED_SCHEME_PATTERN % path)
    finally:
        if fp:
            fp.close()


def _open_hdfs(path, parse_result, mode):
    """Open output stream on HDFS
    Args:
      mode: 'rb', 'wb',
    """
    if mode not in ('rb', 'wb', 'ab'):
        logging.error(UNSUPPORTED_MODE_PATTERN, mode)
        raise ValueError(UNSUPPORTED_MODE_PATTERN % mode)
    if six.PY2:
        return _open_hdfs_py2(path, parse_result, mode)
    elif sys.PY3:
        return _open_hdfs_py3(path, parse_result, mode)


def _open_hdfs_py2(path, parse_result, mode):
    import pyarrow
    fs = pyarrow.hdfs.connect()
    return fs.open(parse_result.path, mode)


def _open_hdfs_py3(path, parse_result, mode):
    import pyarrow.fs
    hdfs = pyarrow.fs.HadoopFileSystem(host='default', port=0)
    if mode[0] == 'r':
        return hdfs.open_input_stream(parse_result.path)
    elif mode == 'wb':
        return hdfs.open_output_stream(parse_result.path)
    else:
        # open_append_stream does not create empty file if target does not exist
        # See https://issues.apache.org/jira/browse/ARROW-14925
        return hdfs.open_append_stream(parse_result.path)
