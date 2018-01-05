#!/usr/bin/env python
# -*- : coding: utf-8 -*-

import os

class Config:
    def __init__(self, host, port, db, password):
        self.host = host
        self.port = port
        self.db = db
        self.password = password

def get_env_var(name) :
    try :
        v = os.environ[name]
    except KeyError:
        return None
    return v

def Read():
    redis_host = get_env_var('REDIS_HOST_NAME')
    if (redis_host == None):
        redis_host = '127.0.0.1'
    redis_port = get_env_var('REDIS_HOST_PORT')
    if (redis_port == None):
        redis_port = '6379'
    redis_db = get_env_var('REDIS_DB')
    if (redis_db == None):
        redis_db='0'
    redis_password = get_env_var('REDIS_PASSWORD')

    return Config(
        host=redis_host,
        port=redis_port,
        db=int(redis_db),
        password=redis_password,
        )
