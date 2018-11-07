from time import time
from datetime import timedelta

import jwt
import requests
from requests.auth import AuthBase

from mesoshttp.exception import ACSException


class DCOSServiceAuth(AuthBase):
    """Attaches a token to Request object that will allow requests to be made through DCOS Admin Router."""

    def __init__(self, secret, verify=False):
        """Take a DCOS service account secret and breaks out components needed for login flow

        :param secret: dict that must include uid, private_key, scheme and login_endpoint keys
        :param verify: `False` or path to a PEM encoded trust bundle
        :return: :class:`DCOSServiceAuth`
        """

        # Service account
        self._user = secret['uid']
        self._key = secret['private_key']
        self._scheme = secret['scheme']
        self._acs_endpoint = secret['login_endpoint']
        self._verify = verify

        self._expiration = time()
        self._token = None

    @property
    def principal(self):
        """Get the service account user value which will match the principal value that must be set in Mesos

        :return: service account user name
        """
        return self._user

    @property
    def token(self):
        """Get the authentication token for making API calls through DCOS AdminRouter

        :return: token used for authentication
        """

        if time() > self._expiration or self._token is None:
            self._acs_login()

        return self._token

    def _generate_token(self, uid, private_key, scheme='RS256', expiry_seconds=180):
        """Generate a JWT for ACS login

        Updates the instance variable to track expiration

        :param private_key: Service accounts PEM encoded private key
        :param scheme: algorithm used to encode JWT
        :param expiry_seconds: how many seconds authentication token will be good for
        :return: login token
        """
        expire_time = time() + float(timedelta(seconds=expiry_seconds).seconds)
        token = jwt.encode({'exp': expire_time, 'uid': uid}, private_key, algorithm=scheme)
        self._expiration = expire_time
        return token

    def _acs_login(self):
        """Login to ACS and set an authentication token on the instance
        """

        payload = {'uid': self._user, 'token': self._generate_token(self._user, self._key, self._scheme)}
        response = requests.post(self._acs_endpoint, json=payload, verify=self._verify)

        if response.status_code != 200:
            raise ACSException('Unable to authenticate against DCOS ACS: {} {}'.format(response.status_code,
                                                                                       response.text))
        self._token = response.json()['token']

    def __call__(self, r):
        r.headers['Authorization'] = 'token={}'.format(self.token)
        return r

