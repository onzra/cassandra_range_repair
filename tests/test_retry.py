#! /usr/bin/env python


import os, sys, unittest, pkg_resources, mock, logging
sys.path.insert(0, '..')
sys.path.insert(0, '.')

sys.path.insert(0,os.path.abspath(__file__+"/../../src"))

import range_repair


class FailingExecutor:
    def __init__(self, nfails):
        self._outcomes = [False] * nfails

    def __call__(self):
        if self._outcomes:
            return self._outcomes.pop()
        else:
            return True


def build_fake_retryer(nfails, maxtries, maxsleep=10):
    executor = FailingExecutor(nfails)

    sleeps = []
    sleeper = lambda seconds: sleeps.append(seconds)

    config = range_repair.ExponentialBackoffRetryerConfig(maxtries, 1, 2, maxsleep)
    retryer = range_repair.ExponentialBackoffRetryer(config, lambda ok: ok, executor, sleeper)

    return retryer, sleeps


class RetryTests(unittest.TestCase):
    def test_first_execution_success(self):
        retryer, sleeps = build_fake_retryer(0, 5)
        self.assertEqual(retryer(), True)
        self.assertEqual(sleeps, [])

    def test_seconds_execution_success(self):
        retryer, sleeps = build_fake_retryer(1, 5)
        self.assertEqual(retryer(), True)
        self.assertEqual(sleeps, [1])

    def test_third_execution_success(self):
        retryer, sleeps = build_fake_retryer(2, 5)
        self.assertEqual(retryer(), True)
        self.assertEqual(sleeps, [1, 2])

    def test_too_many_retries(self):
        retryer, sleeps = build_fake_retryer(10, 5)
        self.assertEqual(retryer(), False)
        self.assertEqual(sleeps, [1, 2, 4, 8])

    def test_max_sleep(self):
        retryer, sleeps = build_fake_retryer(10, 7)
        self.assertEqual(retryer(), False)
        self.assertEqual(sleeps, [1, 2, 4, 8, 10, 10])

    def test_disabling_max_sleep(self):
        retryer, sleeps = build_fake_retryer(10, 7, 0)
        self.assertEqual(retryer(), False)
        self.assertEqual(sleeps, [1, 2, 4, 8, 16, 32])

        retryer, sleeps = build_fake_retryer(10, 7, -1)
        self.assertEqual(retryer(), False)
        self.assertEqual(sleeps, [1, 2, 4, 8, 16, 32])
