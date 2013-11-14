#!/usr/bin/env python
#*_* coding=utf8 *_*

import re
import random
import urllib2
import eventlet
import eventlet.pools
from time import time
from cStringIO import StringIO

import swiftclient as client


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


def get_auth_token(account, user, password, auth_url, proxy_ip):
    """
    Get Authenticate token and Storage URL

    Returns:
    (token, storage_url)
    """
    # initialize request
    request = urllib2.Request(auth_url)
    request.add_header('X-Auth-User', ':'.join((account, user)))
    request.add_header('X-Auth-Key', password)
    try:
        # get token and storage url
        response = urllib2.urlopen(request)
        auth_token = response.info().getheader('X-Auth-Token')
        local_storage_url = response.info().getheader('X-Storage-Url')
        storage_url = re.sub('127.0.0.1', proxy_ip, local_storage_url)
        return (auth_token, storage_url)
    # authentication failed
    except Exception as e:
        raise e

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
