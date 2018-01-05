#!/usr/bin/env python
# -*- : coding: utf-8 -*-

import unittest
import redis
import rconfig
from package import *

class TestPackage(unittest.TestCase):
    def setUp(self):
        r = rconfig.Read()
        self.redis = redis.StrictRedis(host=r.host, port=r.port, db=r.db, password=r.password)

    def test_create_package(self):
        class TestPackage:
            def __init__(self):
                self.a = 1
                self.b = 2
        p = create_package(TestPackage())
        self.assertTrue(p.ID != None)

    def test_get_string(self):
        p = Package(payload='こんにちは、世界！', working_channel_key = 'test_workingkey', channel_key = 'test_channelkey')
        rst = from_json(str(p))
        self.assertEquals(u'こんにちは、世界！', rst.Payload)

    def test_json(self):
        s = "{\"ID\":\"6f2h95ad-8691-4c18-8317-a4d891319774\",\"Payload\":\"Hello world\",\"CreatedAt\":\"2017-12-13T12:48:58.714364303+09:00\"}"
        rst = from_json(s)
        self.assertEquals(s, rst.orig_str)

    def test_ack(self) :
        p = Package(payload='こんにちは、世界！', working_channel_key = 'test_workingkey', channel_key = 'test_channelkey')
        p.redis = self.redis
        self.redis.lpush(p.working_channel_key, str(p))
        rs = self.redis.llen(p.working_channel_key)
        p.ack()
        r = self.redis.llen(p.working_channel_key)
        self.assertEquals(0, r-rs)

    def test_fail(self) :
        p = Package(payload='Hello world', working_channel_key = 'test_workingkey',
                    channel_key = 'test_channelkey',
                    failed_channel_key = 'test_channelkey::failed')
        p.redis = self.redis
        ws = self.redis.llen(p.working_channel_key)
        self.redis.lpush(p.working_channel_key, str(p))
        fs = self.redis.llen(p.failed_channel_key)
        p.fail()
        w = self.redis.llen(p.working_channel_key)
        f = self.redis.llen(p.failed_channel_key)
        self.assertEquals(0, w - ws)
        self.assertEquals(1, f - fs)
