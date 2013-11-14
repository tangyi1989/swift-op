#!/usr/bin/env python
#*_* coding=utf8 *_*

import random
import eventlet
import eventlet.pools
from time import time
from cStringIO import StringIO

import swiftclient as client

# 请修改此变量
DATADIR = 'objects'
DEVICE_PATH = '/srv/node/'
PROXY_IP = '127.0.0.1'
ACCOUNT = 'test'
USER = 'testadmin'
KEY = 'testing'

def gen_text(length=1024):

    """ Generate random string of given length """

    plain_text = "QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnm1234567890"
    text_length = len(plain_text)
    buf = StringIO()

    while length > 0:
        c = plain_text[random.randint(0, text_length - 1)]
        buf.write(c)
        length -= 1

    buf.seek(0)

    return buf.read()

def get_auth_token():
    """
    Get Authenticate token and Storage URL

    Returns:
    (token, storage_url)
    """
    auth_url = "http://%s:8080/auth/v1.0" % PROXY_IP
    url, token = client.get_auth(auth_url,
                                 ':'.join((ACCOUNT, USER)),
                                 KEY)
    return (token, url)

def timing_stats(func):

    """ Stats function call's time cost """

    def wrapped(*args, **kwargs):
        start_time = time()
        func(*args, **kwargs)
        end_time = time()

        print 'Cost seconds' % (end_time - start_time)
        print 'Function : %s, args : %s, kwargs : %s' % (func, args, kwargs)

    return wrapped

class ConnectionPool(eventlet.pools.Pool):

    def __init__(self, url, size):
        self.url = url
        eventlet.pools.Pool.__init__(self, size, size)

    def create(self):
        return client.http_connection(self.url)
