#!/usr/bin/env python
# -*- : coding: utf-8 -*-

import redis
import package
from threading import Thread
from async_promises import Promise

class Channel:
    def __init__(self, channel_key, redis):
        self.channel_key = channel_key
        self.redis = redis

class InputChannel(Channel):
    def __init__(self, channel_key, working_channel_key, failed_channel_key, redis):
        self.channel_key = channel_key
        self.working_channel_key = working_channel_key
        self.failed_channel_key = failed_channel_key
        self.redis = redis
    def async_read(self, to):

        def read_in_thread(resolve, reject):
            try:
                pck = self.read(to)
                resolve(pck)
            except Exception as inst:
                reject(inst)


        return Promise(read_in_thread)

    def read(self, to):
        try:
            answer = self.redis.brpoplpush(
                            self.channel_key,
                            self.working_channel_key,
                            to,
            )
            if answer == None :
                return None
        except Exception as inst:
            raise Exception('Failed to read input channel.', type(inst), inst)

        pack = package.from_json(answer)
        pack.channel_key = self.channel_key
        pack.working_channel_key = self.working_channel_key
        pack.failed_channel_key = self.failed_channel_key
        pack.redis = self.redis
        return pack


class OutputChannel(Channel):
    def __init__(self, channel_key, redis):
        Channel.__init__(self, channel_key, redis)
    def write(self, pck):
        try:
            self.redis.lpush(self.channel_key, str(pck))
        except Exception as inst:
            raise

class ResponseChannel(Channel):
    def __init__(self, channel_key, response_channel_key, redis):
        self.channel_key = channel_key
        self.response_channel_key = response_channel_key
        self.redis = redis
    def read(self, to):
        try:
            ## brpop returns a tuple (key, value)
            answer = self.redis.brpop(self.response_channel_key, to)
        except Exception as inst:
            raise
        return package.from_json(answer[1])
