#!/usr/bin/env python
# -*- : coding: utf-8 -*-

import unittest
import redis
import threading
import rconfig
from broker import Broker
from datetime import datetime
from channel_name import *
from package import Package

class TestBroker(unittest.TestCase):
    def setUp(self):
        r = rconfig.Read()
        self.broker = Broker("TestBroker")
        self.redis = redis.StrictRedis(host=r.host, port=r.port, db=r.db, password=r.password)

    def test_ping(self):
        self.assertTrue(self.broker.ping())


    def test_write(self):
        oldLen = self.redis.llen(input_channel_key("TestBroker"))
        pck = self.write("This is a test message")
        len = self.redis.llen(input_channel_key("TestBroker")) - oldLen
        self.redis.lpop(input_channel_key("TestBroker"))
        self.assertEquals(1, len)
        self.redis.lrem(input_channel_key("TestBroker"), 0, str(pck))

    def test_write_read_ack(self):
        pck = self.write("Test WriteRead")
        input = self.broker.get_input_channel("consumer1")
        rst = input.read(10)
        self.assertEquals(str(pck), str(rst))
        self.assertEquals(1, self.redis.llen(working_channel_key(self.broker.name, "consumer1")))
        rst.ack()

    def test_write_read_fail(self):
        cName = "TestWriteReadAndFail"
        self.write("Test write read and failed")

        cnt = self.redis.llen(failed_channel_key(self.broker.name, cName))
        input = self.broker.get_input_channel(cName)
        rst = input.read(10)
        self.assertTrue(rst != None)
        self.assertEquals(0,self.redis.llen(failed_channel_key(self.broker.name, cName)) - cnt)
        rst.fail()
        self.assertEquals(1, self.redis.llen(failed_channel_key(self.broker.name, cName)) - cnt)
        self.redis.lrem(rst.failed_channel_key, 0, rst.get_failed_package())

    def test_write_asyncread(self):
        cName = "TestWriteAsncRead"
        input = self.broker.get_input_channel(cName)
        flag = [True]
        exception = [None]
        exit_event = threading.Event()
        def on_resolve(data) :
            data.ack()
            exit_event.set()
        def on_reject(ex) :
            flag[0] = False
            exception[0] = ex
            exit_event.set()
        input.async_read(10).then(on_resolve, on_reject)
        self.write("Test async Read")
        exit_event.wait(5)
        self.assertTrue(flag[0], "Get Ex: " + str(exception[0]))

    def test_respond(self):
        cName = "TestRespond"
        pck = self.write("Test respond")
        input = self.broker.get_input_channel(cName)
        flag = [True]
        exception = [None]
        exit_event = threading.Event()
        def on_resolve(data) :
            try:
                data.respond("respond to " + data.ID)
                data.ack()
            except Exception as e:
                flag[0] = False
                exception[0] = e
            exit_event.set()
        def on_reject(ex):
            flag[0] = False
            exception[0] = ex
            exit_event.set()
        input.async_read(10).then(on_resolve, on_reject)
        exit_event.wait(5)
        self.assertTrue(flag[0], "Get Ex: " + str(exception[0]))
        response_channel = self.broker.get_response_channel(pck.ID)
        rst = response_channel.read(5)
        self.assertIsNotNone(rst)


    def write(self, msg):
        output = self.broker.get_output_channel()
        pck = Package(payload=msg, created_at=datetime.now().isoformat())
        output.write(pck)
        return pck



if __name__ == "__main__":
    unittest.main()
