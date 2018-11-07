import logging


class CoreMesosObject(object):
    '''
    Internal class to manage driver
    '''

    def __init__(self, mesos_url, frameworkId, streamId, requests_auth=None, verify=True):
        self.logger = logging.getLogger(__name__)
        self.mesos_url = mesos_url
        self.streamId = streamId
        self.frameworkId = frameworkId
        self.requests_auth = requests_auth
        self.verify = verify
