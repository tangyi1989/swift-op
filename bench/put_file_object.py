#!/usr/bin/env python
#*_* coding=utf8 *_*

""" 测试写入Swift object disk file的性能 """

import os
import logging

from time import time
from eventlet import tpool
from eventlet.green.httplib import CannotSendRequest

from collections import defaultdict
from contextlib import contextmanager

from swift.common import utils
from swift.common import direct_client
from swift.common.swob import Request
from swift.obj.diskfile import DiskFile
from swift.obj import server as object_server
from swift.common.utils import mkdirs, renamer, ThreadPool, \
    normalize_timestamp

import swiftclient as client

from utils import gen_text, get_auth_token, ConnectionPool, \
    CONCURRENCY, PROXY_IP, DATADIR, DEVICE_PATH
from bench import SwiftBenchPUT

LOG = logging.getLogger(__name__)

class PUTObject():

    def __init__(self):
        """Set up for testing swift.object.server.ObjectController"""

        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.HASH_PATH_PREFIX = 'startcap'

        # Notice this!!!
        self.testdir = DEVICE_PATH
        mkdirs(os.path.join(self.testdir, 'sdb1', 'tmp'))

        conf = {'devices': self.testdir, 'mount_check': 'false'}
        self.object_controller = object_server.ObjectController(conf)
        self.object_controller.bytes_per_sync = 1
        self._orig_tpool_exc = tpool.execute
        tpool.execute = lambda f, *args, **kwargs: f(*args, **kwargs)

        self.test_meta = gen_text(1024)

        # Disk file
        self.logger = LOG
        self.devices = DEVICE_PATH
        self.mount_check = False
        self.threads_per_disk = 0  # notice
        self.disk_chunk_size = 65536
        self.bytes_per_sync = 512 * 1024 * 1024
        self.threadpools = defaultdict(
            lambda: ThreadPool(nthreads=self.threads_per_disk))
        self.token, self.url = get_auth_token()
        self.container = gen_text(5)
        resp = {}
        client.put_container(
            self.url, self.token, self.container, response_dict=resp)
        assert resp['status'] == 201
        
        self.conn_pool = ConnectionPool(self.url, CONCURRENCY)

    @contextmanager
    def connection(self):
        try:
            hc = self.conn_pool.get()
            try:
                yield hc
            except CannotSendRequest:
                try:
                    hc.close()
                except Exception:
                    pass
                self.failures += 1
                hc = self.conn_pool.create()
        finally:
            self.conn_pool.put(hc)

    def _diskfile(self, device, partition, account, container, obj, **kwargs):
        """Utility method for instantiating a DiskFile."""

        kwargs.setdefault('mount_check', self.mount_check)
        kwargs.setdefault('bytes_per_sync', self.bytes_per_sync)
        kwargs.setdefault('disk_chunk_size', self.disk_chunk_size)
        kwargs.setdefault('threadpool', self.threadpools[device])
        kwargs.setdefault('obj_dir', DATADIR)

        return DiskFile(self.devices, device, partition, account,
                        container, obj, self.logger, **kwargs)

    def write_swift_disk_file(self, obj_name, content):
        disk_file = self._diskfile('sdb1', 'p', 'a', 'c', obj_name)
        fsize = len(content)
        with disk_file.create(size=fsize) as writer:
            metadata = {
                'X-Timestamp': time(),
                'Content-Type': "test",
                'ETag': "123",
                'Content-Length': str(len(content))
            }
            writer.write(content)
            writer.put(metadata)

    def write_file(self, obj_name, content):
        obj_path = os.path.join(DEVICE_PATH, '/sdb1/tmp_put', obj_name)
        mv_path = os.path.join(
            DEVICE_PATH, '/sdb1/tmp_put', '%s.mv' % obj_name)
        meta_path = os.path.join(
            DEVICE_PATH, '/sdb1/tmp_put', '%s.meta' % obj_name)

        with open(meta_path, 'wb') as f:
            f.write(self.test_meta)
            os.fsync(f.fileno())

        with open(obj_path, 'wb') as f:
            f.write(content)
            os.fsync(f.fileno())

        renamer(obj_path, mv_path)

    def PUT_file(self, obj_name, content):
        path = '/sdb1/p/a/c/%s' % obj_name
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            path, environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Length': len(content),
                     'Content-Type': 'application/octet-stream'})

        req.body = content
        resp = req.get_response(self.object_controller)
        # Make sure we created our file
        assert resp.status_int == 201

        return resp

    def PUT_without_swob(self, obj_name, content):
        path = '/sdb1/p/a/c/%s' % obj_name
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            path, environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Length': len(content),
                     'Content-Type': 'application/octet-stream'},
            body=content)

        resp = self.object_controller.PUT(req)
        assert resp.status_int == 201

        return resp

    def PUT_through_object(self, obj_name, content):
        node = {'ip': PROXY_IP, 'port': 6000, 'device': 'sdb1'}
        partition = 'p'
        direct_client.direct_put_object(node, partition,
                                        'a', 'c', obj_name,
                                        content, len(content))


if __name__ == "__main__":
    put_object = PUTObject()
    bencher = SwiftBenchPUT(4096, file_size=1024 * 128,
                            worker_num=4, coro_concurrency=26)
    bencher.run(put_object.PUT_file)
