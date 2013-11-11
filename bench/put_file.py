#!/usr/bin/env python
#*_* coding=utf8 *_*

""" 测试写入Swift object disk file的性能 """

import os
import random
import logging

from time import time
from eventlet import tpool
from tempfile import mkdtemp
from cStringIO import StringIO
from collections import defaultdict

from swift.common import utils
from swift.common.swob import Request
from swift.obj.diskfile import DiskFile
from swift.obj import server as object_server
from swift.common.utils import mkdirs, renamer, ThreadPool, \
        normalize_timestamp

LOG = logging.getLogger(__name__)
DATADIR = 'objects'

class PUTCase():

    def __init__(self):
        """Set up for testing swift.object.server.ObjectController"""
        
        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.HASH_PATH_PREFIX = 'startcap'
        self.testdir = \
            os.path.join(mkdtemp(), 'tmp_test_object_server_ObjectController')
        mkdirs(os.path.join(self.testdir, 'sdb1', 'tmp'))
        conf = {'devices': self.testdir, 'mount_check': 'false'}
        self.object_controller = object_server.ObjectController(conf)
        self.object_controller.bytes_per_sync = 1
        self._orig_tpool_exc = tpool.execute
        tpool.execute = lambda f, *args, **kwargs: f(*args, **kwargs)

        self.test_meta = self.gen_text(1024)

        # Disk file
        self.logger = LOG
        self.devices = '/srv/node/'
        self.mount_check = True
        self.threads_per_disk = 0  # notice
        self.disk_chunk_size = 65536
        self.bytes_per_sync = 512 * 1024 * 1024
        self.threadpools = defaultdict(
            lambda: ThreadPool(nthreads=self.threads_per_disk))

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
        #resp = self.object_controller.PUT(req)
        return resp

    def gen_text(self, length=1024):
        plain_text = "qwertyuiopasdfghjklzxcvbnm1234567890"
        text_length = len(plain_text)
        buf = StringIO()

        while length > 0:
            c = plain_text[random.randint(0, text_length - 1)]
            buf.write(c)
            length -= 1

        buf.seek(0)

        return buf.read()

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
        obj_path = os.path.join('/srv/node/sdb1/tmp_put', obj_name)
        mv_path = os.path.join('/srv/node/sdb1/tmp_put', '%s.mv' % obj_name)
        meta_path = os.path.join(
            '/srv/node/sdb1/tmp_put', '%s.meta' % obj_name)

        with open(meta_path, 'wb') as f:
            f.write(self.test_meta)
            os.fsync(f.fileno())

        with open(obj_path, 'wb') as f:
            f.write(content)
            os.fsync(f.fileno())

        renamer(obj_path, mv_path)

    def run(self, times, f, file_size=1024 * 64):
        t = times
        content = self.gen_text(file_size)
        
        start_time = time()
        while t > 0:
            obj_name = self.gen_text(24)
            f(obj_name, content)
            t -= 1

        end_time = time()
        cost = end_time - start_time

        print 'Times : %s, Cost seconds : %s' % (times, end_time - start_time)
        print 'file_size : %s, IO : %s' % (file_size, file_size * times / cost)

if __name__ == "__main__":
    put_case = PUTCase()
    put_case.run(1024, put_case.write_swift_disk_file)
    put_case.run(1024, put_case.PUT_file)
