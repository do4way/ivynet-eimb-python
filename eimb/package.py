#!/usr/bin/env python
# -*- : coding: utf-8 -*-

import uuid
import json
from datetime import datetime as date
from collections import OrderedDict
from dateutil.tz import *


def datetimeNowISO8601():
    return date.now(tzlocal()).replace(microsecond=0).isoformat()



class EimbEncoder(json.JSONEncoder) :
    def default(self, obj) :
        rst = OrderedDict()
        dict = obj.__dict__
        for  key in [a for a in dict if a[0].isupper() ]:
            rst[key] = dict[key]
        return rst
class PackageEncoder(json.JSONEncoder) :
    def default(self, obj):
        rst = OrderedDict()
        dict = obj.__dict__
        for key in ["ID", "Payload", "CreatedAt"]:
            rst[key] = dict[key]
        return rst

def as_package(dct):
    return Package(ID=dct['ID'],payload=dct['Payload'], created_at=dct['CreatedAt'])

class Package :
    def __init__(self, ID = None, payload = None, created_at = None,
                 channel_key = None, working_channel_key = None,
                 failed_channel_key = None, failed_at = None, redis = None, orig_str = None) :
        if ID == None :
            ID = str(uuid.uuid4())
        self.ID = ID
        self.Payload = payload
        if created_at == None:
            created_at =  datetimeNowISO8601()
        self.CreatedAt = created_at
        self.channel_key = channel_key
        self.working_channel_key = working_channel_key
        self.failed_channel_key = failed_channel_key
        self.failed_at = failed_at
        self.orig_str = orig_str
        self.redis = redis
    def __str__(self) :
        return json.dumps(self, sort_keys=True, cls=EimbEncoder)

    def get_failed_package(self) :
        fp = dict(
            Payload = self.Payload,
            CreatedAt = self.CreatedAt,
            FailedAt = self.failed_at,
        )
        return json.dumps(fp, sort_keys=True)

    def ack(self) :
        if (self.redis == None) :
            raise Exception("Package not contained")
        try:
            self.redis.lrem(self.working_channel_key, 0, self.orig_str)
        except Exception as inst:
            raise Exception("Failed to ack.", type(inst), inst)

    def fail(self) :
        if self.redis == None :
            raise Exception("Package not contained")
        try:
            self.redis.lrem(self.working_channel_key, 0, self.orig_str)
            self.failed_at = datetimeNowISO8601()
            self.redis.rpush(self.failed_channel_key, self.get_failed_package())
        except Exception as inst:
            raise Exception("Failed to send failed message.", type(inst), inst)

    def respond(self, obj) :
        try:
            response = create_package(obj)
            self.redis.lpush(self.channel_key + "::" + self.ID, str(response) )
        except Exception as inst:
            raise

def create_package(payload, pckID=None) :
    try :
        if isinstance(payload, basestring):
            msg = payload
        elif isinstance(payload, list):
            msg = json.dumps(payload)
        elif isinstance(payload, dict):
            msg = json.dumps(payload)
        else:
            msg = json.dumps(payload.__dict__, sort_keys=True)
    except Exception as inst :
        raise Exception("Failed to serialize the object when create package.", type(inst), inst)

    return Package(payload=msg, ID=pckID)

def from_json(strs):
    try :
        p = json.loads(strs, object_hook=as_package)
        p.orig_str=strs
        return p
    except Exception as inst :
        raise

if __name__ == '__main__' :
    class TestPayload :
        def __init__(self) :
            self.x = 2
            self.y = 3
    p = create_package(TestPayload())
    print p
    print json.dumps(p,sort_keys=True, cls=EimbEncoder)
    print p.get_failed_package()
