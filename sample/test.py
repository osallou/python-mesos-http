import json
import os
import threading
import logging
import uuid

from mesoshttp.client import MesosClient

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
        logging.getLogger('mesoshttp').setLevel(logging.DEBUG)
        
        self.driver = None
        # Note: leader.mesos address requires Mesos DNS
        #self.client = MesosClient(mesos_urls=['zk://leader.mesos:2181/mesos'])
        # If you are purely using Mesos, you should use explicit address of Master
        # Example: Zookeeper master discovery
        #self.client = MesosClient(mesos_urls=['zk://127.0.0.1:2181/mesos'])
        # Example: Directly address Mesos 
        #self.client = MesosClient(mesos_urls=['http://127.0.0.1:5050'])
        
        # By default, use direct master addressing
        # Allow for comma delimited URLs to be passed in via MASTER_URLS
        # environment variable
        master_urls = os.getenv('MESOS_URLS', 'http://127.0.0.1:5050')
        self.client = MesosClient(mesos_urls=master_urls.split(','))
        
        
        secret = os.getenv('SERVICE_SECRET')
        if secret:
            self.client.set_service_account(json.loads(secret))
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
        self.client.stop = True
        self.driver.tearDown()
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

    def run_job(self, mesos_offer):
        offer = mesos_offer.get_offer()
        print(str(offer))
        task = {
            'name': 'sample test',
            'task_id': {'value': uuid.uuid4().hex},
            'agent_id': {'value': offer['agent_id']['value']},
            'resources': [
            {
                'name': 'cpus',
                'type': 'SCALAR',
                'scalar': {'value': 1}
            },
            {
                'name': 'mem',
                'type': 'SCALAR',
                'scalar': {'value': 1000}
            }
            ],
            'command': {'value': 'sleep 30'},
            'container': {
                'type': 'MESOS',
                'mesos': {
                    'image': {
                        'type': 'DOCKER',
                        'docker': {'name': 'debian'}
                    }
                }
            }
        }

        mesos_offer.accept([task])

test_mesos = Test()
