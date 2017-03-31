import logging
import json

import requests

from mesoshttp.core import CoreMesosObject
from mesoshttp.exception import MesosException


class Offer(CoreMesosObject):
    '''
    Wrapper class for Mesos offers
    '''

    def __init__(self, mesos_url, frameworkId, streamId, mesosOffer):
        CoreMesosObject.__init__(self, mesos_url, frameworkId, streamId)
        self.logger = logging.getLogger(__name__)
        self.offer = mesosOffer

    def get_offer(self):
        '''
        Get offer info received from Mesos

        :return: dict
        '''
        return self.offer

    def accept(self, operations):
        '''
        Accept offer with task operations

        :param operations: JSON TaskInfo instances to accept in current offer
        :type operations: list of json TaskInfo
        '''
        if not operations:
            self.logger.debug('Mesos:Accept:no operation to accept')
            return True

        offer_ids = [{'value': self.offer['id']['value']}]
        self.logger.debug('Mesos:ACCEPT Offer ids:' + str(offer_ids))

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Mesos-Stream-Id': self.streamId
        }

        tasks = []
        for operation in operations:
            if 'slave_id' not in operation:
                operation['slave_id'] = {
                    'value': self.offer['agent_id']['value']
                }
            tasks.append(operation)

        message = {
            "framework_id": {"value": self.frameworkId},
            "type": "ACCEPT",
            "accept": {
                "offer_ids": offer_ids,
                "operations": {
                    'type': 'LAUNCH',
                    'launch': {'task_infos': tasks}
                }
            }
        }
        message = json.dumps(message)
        try:
            r = requests.post(
                self.mesos_url + '/api/v1/scheduler',
                message,
                headers=headers
            )
            self.logger.debug('Mesos:Accept:' + str(message))
            self.logger.debug('Mesos:Accept:Anwser:%d:%s' % (r.status_code, r.text))
        except Exception as e:
            raise MesosException(e)
        return True

    def decline(self):
        '''
        Decline offer
        '''
        if not self.offer:
            return
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Mesos-Stream-Id': self.streamId
        }
        offers_decline = {
            "framework_id": {"value": self.frameworkId},
            "type": "DECLINE",
            "decline": {
                "offer_ids": []
            }
        }

        self.logger.debug('Mesos:Decline:Offer:' + self.offer['id']['value'])
        offers_decline['decline']['offer_ids'].append(
            {'value': self.offer['id']['value']}
        )
        try:
            self.r = requests.post(
                self.mesos_url + '/api/v1/scheduler',
                json.dumps(offers_decline),
                headers=headers
            )
        except Exception as e:
            raise MesosException(e)
        return True
