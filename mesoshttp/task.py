import logging


class Operation(object):

    def __init__(self, task_id, offer):
        '''
        :param task_id: unique id of the task in the framework
        :type task_id: str
        :param offer: offer matching the task
        :type offer: mesoshttp.offers.Offer

        '''
        self.logger = logging.getLogger(__name__)
        self.task_id = str(task_id)
        self.agent_id = offer.offer['agent_id']['value']
        self.offer_id = offer.offer['id']['value']
        self.operation = None

    def set_task(self, task):
        '''
        Task is a protobuf object representing a mesos task
        '''
        self.operation = task
