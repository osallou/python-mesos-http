import logging


class CoreMesosObject(object):
    '''
    Internal class to manage driver
    '''

    def __init__(self, mesos_url, frameworkId, streamId):
        self.logger = logging.getLogger(__name__)
        self.mesos_url = mesos_url
        self.streamId = streamId
        self.frameworkId = frameworkId
