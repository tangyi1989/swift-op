#!/usr/bin/env python
#*_* coding=utf8 *_*

import utils
import eventlet
import time
import multiprocessing
import functools


class BenchManager(object):

    """ Manager for benchmark. """

    def __init__(self, worker_num=4, coro_concurrency=16):
        self.worker_num = worker_num
        self.coro_concurrency = coro_concurrency

    def run_coro(self, times, f, *args, **kwargs):
        eventlet.patcher.monkey_patch(socket=True)

        pool = eventlet.GreenPool(self.coro_concurrency)
        for i in xrange(times):
            pool.spawn_n(f, *args, **kwargs)
        pool.waitall()

    def run_seq(self, times, f, *args, **kwargs):
        for i in xrange(times):
            f(*args, **kwargs)

    def __run_multiproc(self, times, worker, process_func, *args, **kwargs):
        task = multiprocessing.JoinableQueue()

        procs = []
        for i in xrange(self.worker_num):
            p = multiprocessing.Process(
                target=functools.partial(worker, task, process_func))
            procs.append(p)
            p.start()

        for i in xrange(times):
            task.put((args, kwargs))

        task.join()

        for i in xrange(self.worker_num):
            task.put(None)

        task.join()

        for p in procs:
            p.join()

    def run_multiproc_coro(self, times, f, *args, **kwargs):
        def worker(task, process_func):
            pool = eventlet.GreenPool(self.coro_concurrency)

            for item in iter(task.get, None):
                args, kwargs = item
                pool.spawn_n(process_func, *args, **kwargs)
                task.task_done()

            pool.waitall()
            task.task_done()

        self.__run_multiproc(times, worker, f, *args, **kwargs)

    def run_multiproc_seq(self, times, f, *args, **kwargs):

        def worker(task, process_func):
            for item in iter(task.get, None):
                args, kwargs = item
                process_func(*args, **kwargs)
                task.task_done()
            task.task_done()

        self.__run_multiproc(times, worker, f, *args, **kwargs)


class SwiftBenchPUT(BenchManager):

    """ Bench PUT's performance """

    def __init__(self, times, file_size=1024 * 128, **kwargs):
        super(SwiftBenchPUT, self).__init__(**kwargs)
        self.times = times
        self.file_size = file_size
        self.body = utils.gen_text(file_size)

    def with_stats(self, bench_f, f, description):

        start_time = time.time()
        bench_f(self.times, f)
        end_time = time.time()
        cost_seconds = end_time - start_time

        print description
        print "Times : %s, Cost seconds : %s" % \
            (self.times, cost_seconds)
        print "Per Call %s, IO : %s" % \
            (cost_seconds / self.times, self.times *
             self.file_size / cost_seconds)
        print '*********************************************'
        print 

    def run(self, put_func):

        def f():
            obj_name = utils.gen_text(8)
            put_func(obj_name, self.body)

        print
        print
        print "Benching Function : %s, File size : %s" % \
            (put_func.__name__, self.file_size)
        print "Worker number : %s, Coro concurrency : %s" % \
            (self.worker_num, self.coro_concurrency)
        print '-----------------------------------------------'
        print '-----------------------------------------------'
        self.with_stats(self.run_seq, f, "Sequence call")
        self.with_stats(self.run_coro, f, "Use coroutine")
        self.with_stats(self.run_multiproc_seq, f, "Use multiproc seq")
        self.with_stats(self.run_multiproc_coro, f, "Use multiproc coro")
