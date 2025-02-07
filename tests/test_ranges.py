#! /usr/bin/env python


import os, sys, unittest, pkg_resources, mock, logging
sys.path.insert(0, '..')
sys.path.insert(0, '.')

sys.path.insert(0,os.path.abspath(__file__+"/../../src"))

import range_repair

def fake_init(self, options):
    '''Initialize the Token Container by getting the host and ring tokens and
    then confirming the values used for formatting and range
    management.
    :param options: OptionParser result
    :returns: None
    '''
    self.options = options
    self.host_tokens = []
    self.ring_tokens = []
    self.host_token_count = -1
    return

class FakeOptions: pass
    
class range_tests(unittest.TestCase):
    def setUp(self):
        range_repair.TokenContainer.__init__ = fake_init
        f = FakeOptions()
        f.keyspace='pathdb'
        f.columnfamily='path_claims'
        f.host='db-cdev-1.phx3.llnw.net'
        f.steps=5
        f.nodetool='nodetool'
        f.workers=1
        f.local=''
        f.snapshot=''
        f.verbose=True
        f.debug=True
        self.f = f
        return

    def test_Murmur3_range_start_zero(self):
        resultset = []
        t = range_repair.TokenContainer(self.f)
        for x in t.sub_range_generator(0, 3000, steps=5):
            resultset.append(x[0])
        self.assertEqual(resultset, [t.format(0), t.format(600), t.format(1200), t.format(1800), t.format(2400)])
        return

    def test_Murmur3_range_end_zero(self):
        resultset = []
        t = range_repair.TokenContainer(self.f)
        for x in t.sub_range_generator(-3000, 0, steps=5):
            resultset.append(x[0])
        self.assertEqual(resultset, [t.format(-3000), t.format(-2400), t.format(-1800), t.format(-1200), t.format(-600)])
        return

    def test_Murmur3_range_wrap(self):
        resultset = []
        t = range_repair.TokenContainer(self.f)
        endpoint = (2**63)-30
        for x in t.sub_range_generator(endpoint, -endpoint, steps=6):
            resultset.append(x[0])
        self.assertEqual(len(resultset), 7)
        return
        
    def test_Random_range_start_zero(self):
        resultset = []
        t = range_repair.TokenContainer(self.f)
        t.ring_tokens.append(0)
        t.check_for_MD5_tokens()
        for x in t.sub_range_generator(0, 3000, steps=5):
            resultset.append(x[0])
        self.assertEqual(resultset, [t.format(0), t.format(600), t.format(1200), t.format(1800), t.format(2400)])
        return

    def test_Random_range_end_zero(self):
        resultset = []
        t = range_repair.TokenContainer(self.f)
        t.ring_tokens.append(0)
        t.check_for_MD5_tokens()
        for x in t.sub_range_generator(-3000, 0, steps=5):
            resultset.append(x[0])
        self.assertEqual(resultset, [t.format(-3000), t.format(-2400), t.format(-1800), t.format(-1200), t.format(-600)])
        return

    def test_Random_range_wrap(self):
        resultset = []
        t = range_repair.TokenContainer(self.f)
        endpoint = (2**63)-30
        t.ring_tokens.append(0)
        t.check_for_MD5_tokens()
        for x in t.sub_range_generator(endpoint, -endpoint, steps=6):
            resultset.append(x[0])
        self.assertEqual(len(resultset), 6)
        return
    def test_Murmur3_format_length(self):
        t = range_repair.TokenContainer(self.f)
        self.assertEqual(21, len(t.format(0)))
        self.assertEqual(21, len(t.format(100)))
        self.assertEqual(21, len(t.format(-100)))

