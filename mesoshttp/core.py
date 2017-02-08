import logging


class CoreMesosObject(object):

    def __init__(self, mesos_url, frameworkId, streamId):
        self.logger = logging.getLogger(__name__)
        self.mesos_url = mesos_url
        self.streamId = streamId
        self.frameworkId = frameworkId
