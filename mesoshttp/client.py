import json
import time
import logging
import socket
import sys

import requests
from requests.exceptions import ConnectionError

from mesoshttp.acs import DCOSServiceAuth

from mesoshttp.offers import Offer
from mesoshttp.core import CoreMesosObject
from mesoshttp.exception import MesosException
from mesoshttp.update import Update

from kazoo.client import KazooClient


class MesosClient(object):
    '''
    Entrypoint class to connect framework to Mesos master

    Instance should be started in a separate thread as `MesosClient.register`
    will start a blocking loop with a long connection.
    '''

    WAIT_TIME = 10

    SUBSCRIBED = 'SUBSCRIBED'
    OFFERS = 'OFFERS'
    ERROR = 'ERROR'
    UPDATE = 'UPDATE'
    FAILURE = 'FAILURE'
    RESCIND = 'RESCIND'
    HEARTBEAT = 'HEARTBEAT'

    DISCONNECTED = 'DISCONNECTED'
    RECONNECTED = 'RECONNECTED'

    class SchedulerDriver(CoreMesosObject):
        '''
        Handler to communicate with scheduler

        `MesosClient.SchedulerDriver` instance is available after the
        SUBSCRIBED event with the subscribed event.
        '''
        def __init__(self, mesos_url, frameworkId, streamId, requests_auth=None, verify=True):
            '''
            Create a driver instance related to created framework
            '''
            CoreMesosObject.__init__(self, mesos_url, frameworkId, streamId, requests_auth, verify)
            self.driver = None

        def tearDown(self):
            '''
            Undeclare framework
            '''
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Mesos-Stream-Id': self.streamId
            }
            teardown = {
                "framework_id": {"value": self.frameworkId},
                "type": "TEARDOWN"
            }
            try:
                requests.post(
                    self.mesos_url + '/api/v1/scheduler',
                    json.dumps(teardown),
                    headers=headers,
                    auth=self.requests_auth,
                    verify=self.verify
                )
            except Exception as e:
                self.logger.error('Mesos:Teardown:Error:' + str(e))

        def request(self, requests):
            '''
            Send a REQUEST message

            :param requests: list of resources request [{'agent_id': : XX, 'resources': {}}]
            :type requests: list
            '''
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Mesos-Stream-Id': self.streamId
            }

            revive = {
                "framework_id": {"value": self.frameworkId},
                "type": "REQUEST",
                "requests": requests
            }

            try:
                requests.post(
                    self.mesos_url + '/api/v1/scheduler',
                    json.dumps(revive),
                    headers=headers,
                    auth=self.requests_auth,
                    verify=self.verify
                )
            except Exception as e:
                raise MesosException(e)

        def revive(self):
            '''
            Send REVIVE request
            '''
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Mesos-Stream-Id': self.streamId
            }

            revive = {
                "framework_id": {"value": self.frameworkId},
                "type": "REVIVE"
            }

            try:
                requests.post(
                    self.mesos_url + '/api/v1/scheduler',
                    json.dumps(revive),
                    headers=headers,
                    auth=self.requests_auth,
                    verify=self.verify
                )
            except Exception as e:
                raise MesosException(e)

        def kill(self, agent_id, task_id):
            '''
            Kill specified task

            :param agent_id: slave agent_id
            :type agent_id: str
            :param task_id: task identifier
            :type task_id: str
            '''
            self.logger.debug('Kill task %s' % (str(task_id)))
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Mesos-Stream-Id': self.streamId
            }
            message = {
                "framework_id": {"value": self.frameworkId},
                "type": "KILL",
                "kill": {
                    "task_id": {'value': task_id},
                    "agent_id": {'value': agent_id}
                }
            }
            try:
                requests.post(
                    self.mesos_url + '/api/v1/scheduler',
                    json.dumps(message),
                    headers=headers,
                    auth=self.requests_auth,
                    verify=self.verify
                )
            except Exception as e:
                self.logger.error('Mesos:Kill:Exception:' + str(e))
                raise MesosException(e)
            return True

        def shutdown(self, agent_id, executor_id):
            '''
            Shutdown an executor

            :param agent_id: slave identifier
            :type agent_id: str
            :param executor_id: executor identifier
            :type executor_id: str
            '''
            self.logger.debug('Shutdown executor %s' % (str(executor_id)))
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Mesos-Stream-Id': self.streamId
            }
            message = {
                "framework_id": {"value": self.frameworkId},
                "type": "SHUTDOWN",
                "shutdown": {
                    "executor_id": {'value': executor_id},
                    "agent_id": {'value': agent_id}
                }
            }
            try:
                requests.post(
                    self.mesos_url + '/api/v1/scheduler',
                    json.dumps(message),
                    headers=headers,
                    auth=self.requests_auth,
                    verify=self.verify
                )
            except Exception as e:
                raise MesosException(e)
            return True

        def message(self, agent_id, executor_id, message):
            '''
            Send message to an executor

            :param agent_id: slave identifier
            :type agent_id: str
            :param executor_id: executor identifier
            :type executor_id: str
            :param message: message to send, raw bytes encoded as Base64
            :type message: str
            '''
            self.logger.debug(
                'Send message to executor %s' % (str(executor_id))
            )
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Mesos-Stream-Id': self.streamId
            }
            message = {
                "framework_id": {"value": self.frameworkId},
                "type": "MESSAGE",
                "message": {
                    "executor_id": {'value': executor_id},
                    "agent_id": {'value': agent_id},
                    "data": message
                }
            }
            try:
                requests.post(
                    self.mesos_url + '/api/v1/scheduler',
                    json.dumps(message),
                    headers=headers,
                    auth=self.requests_auth,
                    verify=self.verify
                )
            except Exception as e:
                raise MesosException(e)
            return True

        def reconcile(self, tasks):
            '''
            Reconcile tasks

            :type tasks: list
            :param tasks: list of dict { "agent_id": xx, "task_id": yy }
            '''
            self.logger.debug('Reconcile %s' % (str(tasks)))

            if not tasks:
                return True

            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Mesos-Stream-Id': self.streamId
            }
            message = {
                "framework_id": {"value": self.frameworkId},
                "type": "RECONCILE",
                "reconcile": {
                    "tasks": tasks
                }
            }

            try:
                requests.post(
                    self.mesos_url + '/api/v1/scheduler',
                    json.dumps(message),
                    headers=headers,
                    auth=self.requests_auth,
                    verify=self.verify
                )
            except Exception as e:
                raise MesosException(e)
            return True

    def get_driver(self):
        '''
        Get driver instance to dialog with master

        :return: `MesosClient.SchedulerDriver`
        '''
        if self.driver is None:
            self.driver = MesosClient.SchedulerDriver(
                self.mesos_url,
                frameworkId=self.frameworkId,
                streamId=self.streamId,
                requests_auth=self.requests_auth,
                verify=self.verify
            )
        return self.driver

    def disconnect_framework(self):
        '''
        Stops framework but does not teardown (unregister) the framework

        This will enable framework reconnection and will not kill running jobs
        '''
        self.disconnect = True
        if self.long_pool:
            self.long_pool.connection.close()

    def set_role(self, role_name):
        '''
        Set Mesos role to use by framework

        :param role_name: Mesos role name
        :type role_name: str
        '''
        self.frameworkRole = role_name

    def __init__(
            self,
            mesos_urls,
            frameworkId=None,
            frameworkName='Mesos HTTP framework',
            frameworkUser='root',
            frameworkHostname='',
            frameworkWebUI='',
            max_reconnect=3,
            connection_timeout=None):
        '''
        Create a frameworkId

        :param mesos_urls: list of mesos http endpoints
        :type mesos_urls: list
        :param frameworkId: identifier of the framework, if None, will declare a new framework
        :type frameworkId: str
        :param frameworkName: name of the framework
        :type frameworkName: str
        :param frameworkUser: user to use (will run tasks as user), defaults to root
        :type frameworkUser: str
        :param frameworkHostname: Hostname of the framework
        :type frameworkHostname: str
        :param frameworkWebUI: URL of the framework WebUI
        :type frameworkWebUI: str
        :param max_reconnect: number of reconnection retries when connection fails
        :type max_reconnect: int  defaults to 3
        :param connection_timeout: sets a timeout for scheduler connection loop
        :type connection_timeout: defaults to None (no timeout)
        '''
        self.frameworkId = frameworkId
        self.frameworkName = frameworkName
        self.frameworkHostname = frameworkHostname
        self.frameworkWebUI = frameworkWebUI
        self.frameworkRole = None
        self.frameworkUser = frameworkUser
        self.mesos_urls = mesos_urls
        self.mesos_url_index = 0
        self.max_reconnect = max_reconnect
        self.driver = None
        self.streamId = None
        self.logger = logging.getLogger(__name__)
        self.stop = False
        self.disconnect = False
        self.callbacks = {
            MesosClient.SUBSCRIBED: [],
            MesosClient.OFFERS: [],
            MesosClient.UPDATE: [],
            MesosClient.ERROR: [],
            MesosClient.FAILURE: [],
            MesosClient.RESCIND: [],
            MesosClient.DISCONNECTED: [],
            MesosClient.RECONNECTED: [],
            MesosClient.HEARTBEAT: []
        }

        self.principal = None
        self.secret = None
        self.long_pool = None
        self.failover_timeout = None
        self.connection_timeout = connection_timeout
        self.checkpoint = True
        self.capabilities = []
        self.master_info = None
        self.disconnected = False
        self.requests_auth = None
        self.verify = True

    def set_credentials(self, principal, secret):
        '''
        Set credentials to authenticate with Mesos master

        :param principal: login to use
        :type principal: str
        :param secret: password
        :type secret: str
        '''
        self.principal = principal
        self.secret = secret

    def set_service_account(self, service_secret, verify=False):
        '''
        Set credentials to authenticate with DCOS and Mesos Master

        :param service_secret: Optional DCOS Service account secret. Supersedes principal / secret.
        :type service_secret: dict
        :param verify: validate HTTPS fronted Mesos API using CA root trusts, defaults to False
        :type verify: bool
        '''

        self.requests_auth = DCOSServiceAuth(service_secret)
        self.principal = self.requests_auth.principal

        cert_file = 'dcos-ca.crt'

        if not verify:
            response = requests.get('https://leader.mesos/ca/' + cert_file, verify=False)

            if response.status_code == 200:
                with open(cert_file, 'w') as cert:
                    cert.write(response.text)
        self.verify = cert_file

    def tearDown(self):
        '''
        Unregister and stop scheduler
        '''
        self.stop = True

    def get_master_info(self):
        '''
        Get Mesos master information, return None if not connected

        :return: json formatted info about connected Mesos master
        '''
        return self.master_info

    def on(self, eventName, callback):
        '''
        Register callback for an event.

        Multiple callbacks can be registered for the same event

        :param eventName: name fo the event to register to (`MesosClient.SUBSCRIBED`, etc.)
        :type eventName: str
        :param callback: function to call on event
        :type callback: def
        '''
        if eventName == MesosClient.SUBSCRIBED:
            self.callbacks[MesosClient.SUBSCRIBED].append(callback)
        elif eventName == MesosClient.OFFERS:
            self.callbacks[MesosClient.OFFERS].append(callback)
        elif eventName == MesosClient.UPDATE:
            self.callbacks[MesosClient.UPDATE].append(callback)
        elif eventName == MesosClient.ERROR:
            self.callbacks[MesosClient.ERROR].append(callback)
        elif eventName == MesosClient.FAILURE:
            self.callbacks[MesosClient.FAILURE].append(callback)
        elif eventName == MesosClient.RESCIND:
            self.callbacks[MesosClient.RESCIND].append(callback)
        elif eventName == MesosClient.DISCONNECTED:
            self.callbacks[MesosClient.DISCONNECTED].append(callback)
        elif eventName == MesosClient.RECONNECTED:
            self.callbacks[MesosClient.RECONNECTED].append(callback)
        elif eventName == MesosClient.HEARTBEAT:
            self.callbacks[MesosClient.HEARTBEAT].append(callback)
        else:
            self.logger.error('No event %s' % (eventName))
            return False
        return True

    def __event_offers(self, offers):
        return self.__event_callback(MesosClient.OFFERS, offers)

    def __event_error(self, message):
        return self.__event_callback(MesosClient.ERROR, message)

    def __event_update(self, update):
        return self.__event_callback(MesosClient.UPDATE, update)

    def __event_subscribed(self):
        return self.__event_callback(MesosClient.SUBSCRIBED, self.get_driver())

    def __event_disconnected(self):
        return self.__event_callback(MesosClient.DISCONNECTED, 'mesos master disconnected')

    def __event_reconnected(self):
        return self.__event_callback(MesosClient.RECONNECTED, 'mesos master reconnected')

    def __event_heartbeat(self, heartbeat):
        return self.__event_callback(MesosClient.HEARTBEAT, heartbeat)

    def __event_callback(self, event, message):
        is_ok = True
        if event not in self.callbacks:
            self.logger.debug('No callback for %s: %s' % (event, str(message)))
            return is_ok
        for callback in self.callbacks[event]:
            try:
                self.logger.debug(
                    'Callback %s on %s' % (event, callback.__name__)
                )
                callback(message)
            except Exception as e:
                is_ok = False
                self.logger.exception(
                    'Error in %s callback: %s' % (event, str(e))
                )
        return is_ok

    def register(self):
        '''
        Register framework, return False if could not connect, else will open a permanent HTTP connection.

        Creates an infinite loop on a permanent connection to Mesos master to receive messages.
        On message, callbacks will be called.
        '''
        res = False
        attempt = 0
        while not self.stop and not self.disconnect:
            try:
                attempt += 1
                self.driver = None
                self.mesos_url_index = 0
                self.logger.info("try to register")
                res = self.__register()
            except requests.exceptions.ConnectionError as e:
                self.logger.error('http connection error: ' + str(e))
                self.__event_disconnected()
                self.disconnected = True
            except socket.timeout as e:
                self.logger.error('http connection timeout: ' + str(e))
                self.__event_disconnected()
                self.disconnected = True
            except requests.exceptions.ChunkedEncodingError as e:
                self.logger.error('http connection error: ' + str(e))
                self.__event_disconnected()
                self.disconnected = True
            except:
                self.logger.exception('Unexpected error with mesos connection')
                self.__event_disconnected()
                self.disconnected = True
            if not self.stop and not self.disconnect:
                if not res:
                    self.logger.error('Failed to register, retrying...')
                if attempt >= self.max_reconnect:
                    break
                time.sleep(MesosClient.WAIT_TIME)
        else:
            self.logger.error('All connection tries failed')
        return res

    def set_failover_timeout(self, timeout):
        '''
        Sets failover timeout value

        :param timeout: define framework failover timeout, in seconds
        :type timeout: int
        '''
        self.failover_timeout = timeout

    def set_checkpoint(self, do_checkpoint):
        '''
        Sets framework checkpoint value

        :param do_checkpoint: de/activate checkpoint in framework
        :type do_checkpoint: bool
        '''
        self.checkpoint = do_checkpoint

    def add_capability(self, capability):
        '''
        Adds a framwork capability

        :param capability: caapbility name
        :type capability: str
        '''
        self.capabilities.append({'type': capability})

    def __zk_detect(self, zk_url, prefix='/mesos'):
        '''
        Try to get master url info from zookeeper

        :param zk_url: ip/port to reach zookeeper
        :type zk_url: str
        :param prefix: prefix to search for in zookeeper
        :type prefix: str
        '''
        mesos_master = None
        mesos_prefix = prefix
        if not prefix.startswith('/'):
            mesos_prefix = '/' + prefix
        zk = KazooClient(zk_url)
        zk.start()
        childs = zk.get_children(mesos_prefix)
        for child in childs:
            if not child.startswith('json'):
                continue
            (data, zk_mesos) = zk.get('/mesos/' + child)
            if sys.version_info.major == 3:
                data = data.decode("utf-8")
            master_info = json.loads(data)
            if 'pid' in master_info and master_info['pid'].startswith('master@'):
                mesos_master = master_info['pid'].replace('master@', '')

                try:
                    requests.get('http://' + mesos_master)
                    mesos_master = 'http://' + mesos_master
                # If we get connection closed, assume we are in strict mode and set https protocol
                except ConnectionError:
                    mesos_master = 'https://' + mesos_master

                break
        zk.stop()
        self.logger.debug('Zookeeper mesos master: %s' % (str(mesos_master)))
        return mesos_master

    def __register(self):
        python_version = sys.version_info.major
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        subscribe = {
            "type": "SUBSCRIBE",
            "subscribe": {
                "framework_info": {
                    "user": self.frameworkUser,
                    "name": self.frameworkName,
                    "hostname": self.frameworkHostname,
                    "webui_url": self.frameworkWebUI
                }
            }
        }

        if self.frameworkRole:
            subscribe['subscribe']['framework_info']['role'] = self.frameworkRole

        if self.capabilities:
            subscribe['subscribe']['framework_info']['capabilities'] = self.capabilities

        if self.failover_timeout:
            subscribe['subscribe']['framework_info']['failover_timeout'] = self.failover_timeout

        if self.principal:
            subscribe['subscribe']['framework_info']['principal'] = self.principal
            if self.secret:
                credentials = [
                    {'principal': self.principal, 'secret': self.secret}
                ]
                subscribe['subscribe']['credentials'] = credentials

        if self.frameworkId:
            subscribe['subscribe']['framework_info']['id'] = {
                'value': self.frameworkId
            }
            subscribe['framework_id'] = {'value': self.frameworkId}
        ok = False
        self.long_pool = None
        while (not ok) and self.mesos_url_index < len(self.mesos_urls):
            try:
                self.mesos_url = self.mesos_urls[self.mesos_url_index]
                if self.mesos_url.startswith('zk://'):
                    self.logger.debug('Use zookeeper url, try to detect master')
                    zk_info = self.mesos_url.replace('zk://', '').split('/')
                    zk_url = self.__zk_detect(zk_info[0], '/'.join(zk_info[1:]))
                    if zk_url is None:
                        raise Exception('Could not detect master in zookeeper')
                    self.mesos_url = zk_url
                self.logger.warn(
                    'Try to connect to master: %s' % (self.mesos_url)
                )

                if self.connection_timeout is not None:
                    self.logger.debug("connection timeout set")
                    self.long_pool = requests.post(
                        self.mesos_url + '/api/v1/scheduler',
                        json.dumps(subscribe),
                        stream=True,
                        headers=headers,
                        verify=self.verify,
                        auth=self.requests_auth,
                        timeout=self.connection_timeout
                    )
                else:
                    self.long_pool = requests.post(
                        self.mesos_url + '/api/v1/scheduler',
                        json.dumps(subscribe),
                        stream=True,
                        headers=headers,
                        verify=self.verify,
                        auth=self.requests_auth
                    )

                self.logger.debug("Subscribe HTTP answer: " + str(self.long_pool.status_code))
                if self.long_pool.status_code == 307:
                    # Not leader, reconnect to leader
                    self.logger.info("Not master, connect to " + self.long_pool.headers['Location'])
                    if 'Location' in self.long_pool.headers:
                        self.mesos_url = self.long_pool.headers['Location']
                        self.long_pool = requests.post(
                            self.mesos_url + '/api/v1/scheduler',
                            json.dumps(subscribe),
                            stream=True,
                            headers=headers,
                            auth=self.requests_auth,
                            verify=self.verify
                        )
                ok = True
                self.mesos_url_index = 0
            except Exception as e:
                self.mesos_url_index += 1
                self.logger.exception('Mesos:Subscribe:Failed for %s: %s' % (self.mesos_url, str(e)))

        if self.long_pool is not None:
            if not self.long_pool.status_code == 200:
                self.logger.error(
                    'Mesos:Subscribe:Error: ' + str(self.long_pool.text)
                )
                return False
        else:
            self.logger.error(
                'Mesos:Subscribe:Error: Failed to connect to a mesos master'
            )
            return False

        self.streamId = self.long_pool.headers['Mesos-Stream-Id']

        if self.disconnected:
            self.__event_reconnected()
            self.disconnected = False

        first_line = True
        for line in self.long_pool.iter_lines():
            if self.stop or self.disconnect:
                if self.stop and self.driver:
                    self.driver.tearDown()
                break
            # filter out keep-alive new lines
            if first_line:
                count_bytes = int(line)
                first_line = False
                continue
            else:
                if python_version == 3:
                    line = line.decode('UTF-8')
                body = json.loads(line[:count_bytes])
                self.logger.debug('Mesos:Event:%s' % (str(body['type'])))
                self.logger.debug('Mesos:Message:' + str(body))
                if body['type'] == 'SUBSCRIBED':
                    self.frameworkId = body['subscribed']['framework_id']['value']
                    self.logger.info(
                        'Mesos:Subscribe:Framework-Id:' + self.frameworkId
                    )
                    self.logger.info(
                        'Mesos:Subscribe:Stream-Id:' + self.streamId
                    )
                    if 'master_info' in body['subscribed']:
                        self.master_info = body['subscribed']['master_info']
                    self.__event_subscribed()
                elif body['type'] == 'OFFERS':
                    mesos_offers = body['offers']['offers']
                    offers = []
                    for mesos_offer in mesos_offers:
                        offers.append(
                            Offer(
                                self.mesos_url,
                                frameworkId=self.frameworkId,
                                streamId=self.streamId,
                                mesosOffer=mesos_offer,
                                requests_auth=self.requests_auth,
                                verify=self.verify
                            )
                        )
                    self.__event_offers(offers)
                elif body['type'] == 'UPDATE':
                    mesos_update = body['update']
                    update_event = Update(
                        self.mesos_url,
                        frameworkId=self.frameworkId,
                        streamId=self.streamId,
                        mesosUpdate=mesos_update,
                        requests_auth=self.requests_auth,
                        verify=self.verify 
                    )
                    update_event.ack()
                    self.__event_update(mesos_update)
                elif body['type'] == 'ERROR':
                    self.logger.error('Mesos:Error:' + body['error']['message'])
                    self.__event_error(body['error']['message'])
                elif body['type'] == 'RESCIND':
                    self.__event_callback(body['type'], body['rescind'])
                elif body['type'] == 'MESSAGE':
                    self.__event_callback(body['type'], body['message'])
                elif body['type'] == 'FAILURE':
                    self.__event_callback(body['type'], body['failure'])
                elif body['type'] == 'HEARTBEAT':
                    self.logger.debug('Mesos:Heartbeat')
                    self.__event_heartbeat(body['type'])
                else:
                    self.logger.warn(
                        '%s event no yet implemented' % (str(body['type']))
                    )

                if line[count_bytes:]:
                    count_bytes = int(line[count_bytes:])
        return True

    def combine_offers(self, offers, operations, options=None):
        '''
        Accept offers with task operations

        :param offers: offers to be accepted
        :type offers: list
        :param operations: JSON TaskInfo instances to accept
        :type operations: list of json TaskInfo
        :param options: optional filters
        :type options: JSON filters instances

        This method does not check if the operations are valid
        JSON TaskInfo instances and if conform with the offers.
        '''
        if not operations:
            self.logger.debug('Mesos:Accept:no operation to accept')
            return True

        if not offers:
            self.logger.debug('Mesos:Accept:no offers to accept')
            return True

        ids = [o.get_offer()['id']['value'] for o in offers]
        offer_ids = [{'value': oid} for oid in ids]
        self.logger.debug('Mesos:COMBINE Offer ids:' + ','.join(ids))

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Mesos-Stream-Id': self.streamId
        }

        message = {
            "framework_id": {"value": self.frameworkId},
            "type": "ACCEPT",
            "accept": {
                "offer_ids": offer_ids,
                "operations": [{
                    'type': 'LAUNCH',
                    'launch': {'task_infos': operations}
                }]
            }
        }
        if options and options.get('filters'):
            message["accept"]["filters"] = options.get('filters')

        message = json.dumps(message)
        try:
            r = requests.post(
                self.mesos_url + '/api/v1/scheduler',
                message,
                headers=headers,
                auth=self.requests_auth,
                verify=self.verify
            )
            self.logger.debug('Mesos:Accept:' + str(message))
            self.logger.debug('Mesos:Accept:Anwser:%d:%s' % (r.status_code, r.text))
        except Exception as e:
            raise MesosException(e)
        return True
