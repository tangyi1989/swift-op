#!/usr/bin/env python
#*_* coding=utf8 *_*

import random
import re
import multiprocessing
import urllib2
import eventlet
import eventlet.pools
from cStringIO import StringIO
from time import time

import swiftclient as client

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


class Manager():
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
            print 'Times : %s, Cost seconds : %s' % (times, cost)
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

    @timing_stats
    def run_multiprocessing(self, times, f, file_size=1024*64, content='', concurrency=1):
        task = multiprocessing.JoinableQueue()
        def worker():
            for item in iter(task.get, None):
                f(item[0], item[1])
                task.task_done()
            task.task_done()

        procs = []
        for i in xrange(concurrency):
            p = multiprocessing.Process(target=worker)
            procs.append(p)
            p.start()

        for i in xrange(times):
            obj_name = gen_text(24)
            task.put((obj_name, content))

        task.join()

        for i in xrange(concurrency):
            task.put(None)

        task.join()

        for p in procs:
            p.join()