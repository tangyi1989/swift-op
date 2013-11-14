#!/usr/bin/env python
#*_* coding=utf8 *_*

""" 从proxy server端测试Swift文件写入的性能 """
import sys
import logging
from time import time

if 'debug' in sys.argv:
    logger = logging.getLogger("swiftclient")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler(sys.stderr))

from swift.proxy.server import Application as ProxyApplication
from swift.proxy.controllers.obj import ObjectController
from swift.common.utils import normalize_timestamp
from swift.common.swob import Request
from swift.common.memcached import MemcacheRing
import swiftclient as client

from utils import get_auth_token, gen_text
from bench import SwiftBenchPUT

# 请修改此变量
PROXY_IP = '127.0.0.1'
ACCOUNT = 'test'
USER = 'testadmin'
PASSWORD = 'testing'


AUTH_URL = "http://%s:8080/auth/v1.0" % PROXY_IP
ACCOUNT_NAME = 'AUTH_%s' % ACCOUNT   # prefix AUTH_ plus account name

class TestPUT():

    def __init__(self):
        self.app = ProxyApplication(None)
        self.memcache = self._memcache()
        self.token, self.url = get_auth_token(
            ACCOUNT, USER, PASSWORD, AUTH_URL, PROXY_IP)
        self.container = gen_text(5)
        resp = {}
        client.put_container(
            self.url, self.token, self.container, response_dict=resp)
        assert resp['status'] == 201

    def _memcache(self):
        memcache_servers = '127.0.0.1:11211'
        serialization_format = 2
        max_conns = 50
        return MemcacheRing(
            [s.strip() for s in memcache_servers.split(',') if s.strip()],
            allow_pickle=(serialization_format == 0),
            allow_unpickle=(serialization_format <= 1),
            max_conns=max_conns)

    def PUT_in_obj_controller(self, obj_name, content):
        object_controller = ObjectController(
            self.app, ACCOUNT_NAME, self.container, obj_name)
        path = '/%s/%s/%s' % (ACCOUNT_NAME, self.container, obj_name)
        timestamp = normalize_timestamp(time())
        req = Request.blank(
            path, environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Timestamp': timestamp,
                     'Content-Length': len(content),
                     'Content-Type': 'application/octet-stream'})
        req.environ['swift.cache'] = self.memcache
        req.body = content
        resp = object_controller.PUT(req)

        # Make sure we created our file
        assert resp.status_int == 201

        return resp


if __name__ == '__main__':
    putcase = TestPUT()
    bencher = SwiftBenchPUT(16, file_size=1024 * 128,
                            worker_num=4, coro_concurrency=26)
    bencher.run(putcase.PUT_in_obj_controller)
    #putcase.PUT_in_obj_controller('hello', 'world')
