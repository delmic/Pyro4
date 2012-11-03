"""
Tests for the thread pooled job queue.

Pyro - Python Remote Objects.  Copyright by Irmen de Jong (irmen@razorvine.net).
"""

from __future__ import with_statement
import unittest
import time
import random
from Pyro4.tpjobqueue import ThreadPooledJobQueue
import Pyro4.threadutil


MIN_POOL_SIZE = 5
MAX_POOL_SIZE = 10
IDLE_TIMEOUT = 0.5
JOB_TIME = 0.2

class Job(object):
    def __init__(self, name="unnamed"):
        self.name=name
    def __call__(self):
        # print "Job() '%s'" % self.name
        time.sleep(JOB_TIME - random.random()/10.0)


class TPJobQueueTests(unittest.TestCase):
    def setUp(self):
        Pyro4.config.THREADPOOL_MINTHREADS = MIN_POOL_SIZE
        Pyro4.config.THREADPOOL_MAXTHREADS = MAX_POOL_SIZE
        Pyro4.config.THREADPOOL_IDLETIMEOUT = IDLE_TIMEOUT
    def tearDown(self):
        Pyro4.config.reset()

    def testJQcreate(self):
        with ThreadPooledJobQueue() as jq:
            _=repr(jq)
            self.assertEqual(MIN_POOL_SIZE, jq.workercountSafe)
        jq.drain()

    def testJQsingle(self):
        with ThreadPooledJobQueue() as jq:
            job = Job()
            jq.process(job)
            self.assertEqual(MIN_POOL_SIZE, jq.workercountSafe)
        jq.drain()

    def testJQgrow(self):
        with ThreadPooledJobQueue() as jq:
            for i in range(MIN_POOL_SIZE):
                jq.process(Job(str(i)))
            self.assertTrue(jq.workercountSafe >= MIN_POOL_SIZE)
            self.assertTrue(jq.workercountSafe <= MAX_POOL_SIZE)
            jq.process(Job(str(i+1)))
            self.assertTrue(jq.workercountSafe >= MIN_POOL_SIZE)
            self.assertTrue(jq.workercountSafe <= MAX_POOL_SIZE)
        jq.drain()

    def testJQshrink(self):
        with ThreadPooledJobQueue() as jq:
            self.assertEqual(MIN_POOL_SIZE, jq.workercountSafe)
            jq.process(Job("i1"))
            jq.process(Job("i2"))
            jq.process(Job("i3"))
            jq.process(Job("i4"))
            jq.process(Job("i5"))
            self.assertTrue(jq.workercountSafe >= MIN_POOL_SIZE)
            self.assertTrue(jq.workercountSafe <= MAX_POOL_SIZE)
            time.sleep(JOB_TIME + 1.1*IDLE_TIMEOUT)  # wait till the workers are done
            jq.process(Job("i6"))
            self.assertEqual(MIN_POOL_SIZE, jq.workercountSafe)  # one of the now idle workers should have picked this up
            time.sleep(JOB_TIME + 1.1*IDLE_TIMEOUT)  # wait till the workers are done
            for i in range(2*MAX_POOL_SIZE):
                jq.process(Job(str(i+1)))
            self.assertEqual(MAX_POOL_SIZE, jq.workercountSafe)
            time.sleep(JOB_TIME*2 + 1.1*IDLE_TIMEOUT)  # wait till the workers are done
            self.assertEqual(MIN_POOL_SIZE, jq.workercountSafe)  # should have shrunk back to the minimal pool size
        jq.drain()


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
