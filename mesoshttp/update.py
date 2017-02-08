import logging
import json

import requests

from mesoshttp.core import CoreMesosObject
from mesoshttp.exception import MesosException


class Update(CoreMesosObject):
    '''
    This class manages Update message from Mesos master
    '''

    def __init__(self, mesos_url, frameworkId, streamId, mesosUpdate):
        CoreMesosObject.__init__(self, mesos_url, frameworkId, streamId)
        self.logger = logging.getLogger(__name__)
        self.mesosUpdate = mesosUpdate

    def ack(self):
        '''
        Acknowledge an update message
        '''
        if 'uuid' not in self.mesosUpdate['status']:
            self.logger.debug('Mesos:Ack:Skip')
            return
        self.logger.debug('Mesos:Update:Status:%s:%s' % (
            self.mesosUpdate['status']['task_id']['value'],
            self.mesosUpdate['status']['state'])
        )
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Mesos-Stream-Id': self.streamId
        }

        acknowledge = {
            "framework_id": {"value": self.frameworkId},
            "type": "ACKNOWLEDGE",
            "acknowledge": {
                "agent_id": self.mesosUpdate['status']['agent_id'],
                "task_id": {
                    "value": self.mesosUpdate['status']['task_id']['value']
                },
                "uuid": self.mesosUpdate['status']['uuid']
            }
        }
        try:
            requests.post(
                self.mesos_url + '/api/v1/scheduler',
                json.dumps(acknowledge), headers=headers
            )
        except Exception as e:
            raise MesosException(e)
