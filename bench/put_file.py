#!/usr/bin/env python
#*_* coding=utf8 *_*

""" 测试写入Swift object disk file的性能 """

import os
import random
import logging
import urllib2
import re
import eventlet
import eventlet.pools

from time import time
from eventlet import tpool
from cStringIO import StringIO
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

LOG = logging.getLogger(__name__)
DATADIR = 'objects'

# 请修改此变量
DEVICE_PATH = '/srv/node/'
PROXY_IP = '192.168.0.64'
ACCOUNT = 'test'
USER = 'testadmin'
PASSWORD = 'testing'
CONCURRENCY = 100


AUTH_URL = "http://%s:8080/auth/v1.0" % PROXY_IP
PROXY_URL = "http://%s:8080/v1" % PROXY_IP


def gen_text(length=1024):
    plain_text = "qwertyuiopasdfghjklzxcvbnm1234567890"
    text_length = len(plain_text)
    buf = StringIO()

    while length > 0:
        c = plain_text[random.randint(0, text_length - 1)]
        buf.write(c)
        length -= 1

    buf.seek(0)

    return buf.read()

def get_auth_token(account, user, password):
    """
    Get Authenticate token and Storage URL

    Returns:
    (token, storage_url)
    """
    # initialize request
    request = urllib2.Request(AUTH_URL)
    request.add_header('X-Auth-User', ':'.join((account, user)))
    request.add_header('X-Auth-Key', password)
    try:
        # get token and storage url
        response = urllib2.urlopen(request)
        auth_token = response.info().getheader('X-Auth-Token')
        local_storage_url = response.info().getheader('X-Storage-Url')
        storage_url = re.sub('127.0.0.1', PROXY_IP, local_storage_url)
        return (auth_token, storage_url)
    # authentication failed
    except urllib2.HTTPError, e:
        err_code = e.getcode()
        if err_code == 401:
            print e
    finally:
        try:
            response.close()
        except NameError:
            pass

class ConnectionPool(eventlet.pools.Pool):

    def __init__(self, url, size):
        self.url = url
        eventlet.pools.Pool.__init__(self, size, size)

    def create(self):
        return client.http_connection(self.url)

class PUTCase():

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
        self.token, self.url = get_auth_token(ACCOUNT, USER, PASSWORD)
        self.container = gen_text(5)
        resp = {}
        client.put_container(self.url, self.token, self.container, response_dict = resp)
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
        with self.connection() as conn:
            node = {'ip': PROXY_IP, 'port': 6000, 'device': 'sdb1'}
            partition = 'p'
            direct_client.direct_put_object(node, partition,
                                            'a', 'c', obj_name,
                                            content, len(content))

    def PUT_through_proxy(self, obj_name, content):
        resp = {}
        with self.connection() as conn:
            client.put_object(self.url, self.token, self.container,
                              obj_name, content, len(content),
                              http_conn=conn,
                              response_dict = resp)
        assert resp['status'] == 201

    def timing_stats(func):
        def wrapped(*args, **kwargs):
            times = args[1] or kwargs.get('times', 0)
            file_size = args[3] if len(args) == 4 else kwargs.get('filesize', 0) or 1024*64
            content = gen_text(file_size)
            kwargs.setdefault('content', content)
            start_time = time()
            func(*args, **kwargs)
            end_time = time()
            cost = end_time - start_time
            print 'Times : %s, Cost seconds : %s' % (times, end_time - start_time)
            print 'file_size : %s, IO : %s' % (file_size, file_size * times / cost)
        return wrapped

    @timing_stats
    def run_http(self, times, f, file_size=1024 * 64, content=''):
        eventlet.patcher.monkey_patch(socket=True)

        pool = eventlet.GreenPool(CONCURRENCY)
        for i in xrange(times):
            obj_name = gen_text(24)
            pool.spawn_n(f, obj_name, content)
        pool.waitall()

    @timing_stats
    def run(self, times, f, file_size=1024 * 64, content=''):
        for i in xrange(times):
            obj_name = gen_text(24)
            f(obj_name, content)

if __name__ == "__main__":
    put_case = PUTCase()
    #put_case.run(1024, put_case.write_file)
    #put_case.run(1024, put_case.write_swift_disk_file)
    #put_case.run(1024, put_case.PUT_file)
    #put_case.run(1024, put_case.PUT_without_swob)
    # put_case.run_http(1024, put_case.PUT_through_proxy)
    put_case.run_http(1024, put_case.PUT_through_object)
