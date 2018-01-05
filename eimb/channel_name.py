#!/usr/bin/env python
# -*- : coding: utf-8 -*-

def master_key():
    return "eimbs::master"

def input_channel_key(bName):
    return "eimbs::" + bName

def working_prefix(bName):
    return input_channel_key(bName) + "::working"

def working_channel_key(qName, cName):
    return working_prefix(qName) + "::" + cName

def failed_channel_key(qName, cName):
    return working_channel_key(qName, cName) + "::failed"

def response_channel_key(bName, requestID):
    return input_channel_key(bName) + "::" + requestID
