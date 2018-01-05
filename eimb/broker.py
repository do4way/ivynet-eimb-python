#!/usr/bin/env python
# -*- : coding: utf-8 -*-

import redis
import rconfig
from channel_name import *
from channel import *

rcfg = rconfig.Read()

class Broker:
    def __init__(self, name):
        self.name = name
        self.redis = redis.StrictRedis(host=rcfg.host, port=rcfg.port,
                                       db=rcfg.db, password=rcfg.password)
        self.redis.sadd(master_key(), self.name)

    def ping(self):
        try:
            return self.redis.ping()
        except Exception as inst:
            raise inst


    def get_output_channel(self):
        return OutputChannel(input_channel_key(self.name),self.redis)

    def get_input_channel(self, cName):
        return InputChannel(input_channel_key(self.name),
                            working_channel_key(self.name, cName),
                            failed_channel_key(self.name, cName),
                            self.redis)

    def get_response_channel(self, rid):
        return ResponseChannel(input_channel_key(self.name),
                               response_channel_key(self.name, rid),
                               self.redis)
