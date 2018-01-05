#!/usr/bin/env python
# -*- : coding: utf-8 -*-

import unittest
import redis
import threading
import rconfig
import broker
from datetime import datetime
from messenger import Messenger, EIMBMessageHandler
from channel_name import *

topic_name = "MsgerTestTopic"

class EchoHandler(EIMBMessageHandler):
    def on_message(self,pck):
        try:
            pck.respond("OK!")
            pck.ack()
        except Exception as excp:
            print excp
            pck.fail()

class TestBroker(unittest.TestCase):
    def setUp(self):
        r = rconfig.Read()
        self.messenger = Messenger(topic_name)
        self.redis = redis.StrictRedis(host=r.host, port=r.port, db=r.db, password=r.password)

    def test_send(self):
        oldLen = self.redis.llen(input_channel_key(topic_name))
        self.messenger.send("To test messenger send a msg.")
        len = self.redis.llen(input_channel_key(topic_name)) - oldLen
        self.assertEquals(1, len)
        self.redis.lpop(input_channel_key(topic_name))

    def test_request(self):
        flag = [True]
        exception = [None]
        rst = [None]
        exit_event = threading.Event()
        def on_resolve(data) :
            rst[0] = data
            exit_event.set()
        def on_reject(ex) :
            print ex
            flag[0] = False
            exception[0] = ex
            exit_event.set()
        self.messenger.request("To test messenger request a msg.", 5).then(on_resolve, on_reject)
        pck = self.messenger.broker.get_input_channel("request_test_input_reader").read(1)
        pck.respond("OK!")
        pck.ack()
        exit_event.wait(5)
        self.assertEquals("OK!", rst[0].Payload)

    def test_register_handler(self):

        messenger = Messenger("TestBot")
        messenger.register_handler(EchoHandler())

        msg_client = Messenger("TestBot")
        flag = [True]
        exception = [None]
        rst = [None]
        exit_event = threading.Event()
        def on_resolve(data) :
            rst[0] = data
            exit_event.set()
        def on_reject(ex) :
            print ex
            flag[0] = False
            exception[0] = ex
            exit_event.set()
        try:
            msg_client.request("To test handler response", 5).then(on_resolve, on_reject)
            exit_event.wait(5)
            self.assertEquals("OK!", rst[0].Payload)
        finally:
            messenger.stop_monitor()
