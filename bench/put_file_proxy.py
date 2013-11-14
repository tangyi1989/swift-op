#!/usr/bin/env python
#*_* coding=utf8 *_*

""" 从proxy server端测试Swift文件写入的性能 """
from time import time, sleep

from swift.proxy.server import Application as ProxyApplication
from swift.proxy.controllers.obj import ObjectController
from swift.common.utils import normalize_timestamp
from swift.common.swob import Request
from swift.common.memcached import MemcacheRing
import swiftclient as client

from utils import get_auth_token, gen_text, Manager, \
    ACCOUNT, PROXY_IP


class TestPUT(Manager):
    def __init__(self):
        self.app = ProxyApplication(None)
        self.memcache = self._memcache()
        self.account_name = 'AUTH_%s' % ACCOUNT   # prefix AUTH_ plus account name
        self.token, self.url = get_auth_token()
        self.container = gen_text(5)
        resp = {}
        client.put_container(self.url, self.token, self.container, response_dict = resp)
        assert resp['status'] == 201

    def _memcache(self):
        memcache_servers = '%s:11211' % PROXY_IP
        serialization_format = 2
        max_conns = 50
        return MemcacheRing(
            [s.strip() for s in memcache_servers.split(',') if s.strip()],
            allow_pickle=(serialization_format == 0),
            allow_unpickle=(serialization_format <= 1),
            max_conns=max_conns)

    def PUT_in_obj_controller(self, obj_name, content):
        object_controller = ObjectController(self.app, self.account_name, self.container, obj_name)
        path = '/%s/%s/%s' % (self.account_name, self.container, obj_name)
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

    # TODO: when runing both, may give out error message
    # like 'ERROR:root:Timeout talking to memcached' and
    # speed of the second slows down to half of the speed
    # of running standalone.

    # putcase.run(1024, putcase.PUT_in_obj_controller)
    putcase.run_multiprocessing(1024, putcase.PUT_in_obj_controller, concurrency=16)
