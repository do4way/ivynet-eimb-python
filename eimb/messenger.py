#!/usr/bin/env python
# -*- : coding: utf-8 -*-



import threading
import logging
import uuid
import broker
import package
from async_promises import Promise
from abc import ABCMeta, abstractmethod



logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')
class MonitorThread(threading.Thread):
    def __init__(self, messenger, group=None, target=None, name=None,
                 verbose = None):
        threading.Thread.__init__(self, group=group, target=target, name=name, verbose=verbose)
        self.messenger = messenger
        self.logger = logging.getLogger(__name__)

    def run(self):
        #pass
        while(self.messenger.signal == 0x00):
            try:
                pck = self.messenger.read(5)
                if pck != None:
                    for h in self.messenger.handlers:
                        h.on_message(pck)
            except Exception as exp:
                self.logger.error(str(exp))
        print "stop mmonitoring for a message queue"

class EIMBMessageHandler:
    __metaclass__ = ABCMeta
    @abstractmethod
    def on_message(self):
        raise Exception("NotImplementedException")

class Messenger:
    def __init__(self, name):
        self.lock = threading.Lock()
        self.broker = broker.Broker(name)
        self.on_monitoring = False
        self.handlers  = []
        self.signal = 0x00

    def send(self, payload, pckID=None):
        try:
            pck = package.create_package(payload, pckID)
            self.broker.get_output_channel().write(pck)
        except Exception:
            raise

    def read(self, to):
        try:
            pck = self.broker.get_input_channel(self.bot_name()).read(to)
            return pck
        except Exception :
            raise


    def request(self, payload, timeout):
        try:
            pck = package.create_package(payload)
            self.broker.get_output_channel().write(pck)
        except Exception as excp:
            return Promise.reject(excp)
        response_channel = self.broker.get_response_channel(pck.ID)
        def request_in_thread(resolve, reject):
            try:
                data = response_channel.read(timeout)
                resolve(data)
            except Exception as excp:
                reject(excp)
        return Promise(request_in_thread)


    def register_handler(self, handler):

        if self.is_handler_registered(handler):
            return

        self.add_handler(handler)

        if self.on_monitoring:
            return

        self.lock.acquire()
        try:
            if not self.on_monitoring:
                self.start_monitor()
                self.on_monitoring = True
        finally:
            self.lock.release()

    def is_handler_registered(self, h):
        for handler in self.handlers :
            if (h == handler):
                return True
        return False


    def add_handler(self, h):
        if (not isinstance(h, EIMBMessageHandler)):
            raise Exception("NotAnValidMessageHandlerException")
        self.lock.acquire()
        self.handlers.append(h)
        self.lock.release()

    def start_monitor(self):
        t = MonitorThread(self)
        t.daemon = False
        t.start()

    def stop_monitor(self):
        self.signal = 0x01

    def bot_name(self):
        return self.broker.name + "::bot::" + str(uuid.uuid4())
