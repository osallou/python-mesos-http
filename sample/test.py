import json
import datetime
import time
import os
import sys
import threading
import logging
import signal
import sys
import uuid

from mesoshttp.client import MesosClient
from mesoshttp.message import mesos_pb2

class Test(object):


    class MesosFramework(threading.Thread):

        def __init__(self, client):
            threading.Thread.__init__(self)
            self.client = client
            self.stop = False

        def run(self):
            try:
                self.client.register()
            except KeyboardInterrupt:
                print('Stop requested by user, stopping framework....')


    def __init__(self):
        logging.basicConfig()
        self.logger = logging.getLogger(__name__)
        #signal.signal(signal.SIGINT, signal.SIG_IGN)
        logging.getLogger('mesoshttp').setLevel(logging.DEBUG)
        self.driver = None
        self.client = MesosClient(mesos_urls=['http://127.0.0.1:5050'])
        self.client.on(MesosClient.SUBSCRIBED, self.subscribed)
        self.client.on(MesosClient.OFFERS, self.offer_received)
        self.client.on(MesosClient.UPDATE, self.status_update)
        self.th = Test.MesosFramework(self.client)
        self.th.start()
        while True and self.th.isAlive():
            try:
                self.th.join(1)
            except KeyboardInterrupt:
                self.shutdown()
                break


    def shutdown(self):
        print('Stop requested by user, stopping framework....')
        self.logger.warn('Stop requested by user, stopping framework....')
        self.driver.tearDown()
        self.client.stop = True
        self.stop = True


    def subscribed(self, driver):
        self.logger.warn('SUBSCRIBED')
        self.driver = driver

    def status_update(self, update):
        if update['status']['state'] == 'TASK_RUNNING':
            self.driver.kill(update['status']['agent_id']['value'], update['status']['task_id']['value'])

    def offer_received(self, offers):
        self.logger.warn('OFFER: %s' % (str(offers)))
        i = 0
        for offer in offers:
            if i == 0:
                self.run_job(offer)
            else:
                offer.decline()
            i+=1

    def run_job(self, offer):
        task = mesos_pb2.TaskInfo()
        container = mesos_pb2.ContainerInfo()
        container.type = 2  # Mesos
        tid = uuid.uuid4().hex
        command = mesos_pb2.CommandInfo()
        command.value = 'sleep 30'
        task.command.MergeFrom(command)
        task.task_id.value = tid
        # task.slave_id.value = offer.offer.slave_id.value
        task.name ='sample test'

        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = 1

        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value =1000

        docker = mesos_pb2.ContainerInfo.MesosInfo()
        docker.image.type = 2  # Docker
        docker.image.docker.name = 'debian'
        container.mesos.MergeFrom(docker)
        task.container.MergeFrom(container)

        offer.accept([task])

test_mesos = Test()
