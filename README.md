# Python mesos scheduler HTTP library

This is a Python library to create a Mesos scheduler.

It removes the need to use/install Mesos python bindings and makes use of the HTTP API (native bindings are not updated anymore with new features)

MesosClient is the main entrypoint that connects to the Mesos master. You can then register for callbacks on events.
On subscribe, you get a driver instance which you can use to send messages to master on current framework (kill, etc.)

MesosClient must be executed in a separate thread as it keeps a loop pooling connection with the master.

Submitted tasks should be in JSON format (according to mesos.proto).

See sample/test.py for example.

Callbacks will "block" the mesos message treatment, so they should be short, or messages should be forwarded to a queue in an other thread/process where longer tasks will handle messages.

# DCOS EE Strict

Additions have been made to support login with ACS to allow access through to the master
in a Strict security posture. The `sample/test.py` script is configured to accept the `SERVICE_SECRET` environment variable, which can be created and passed to the 
service using the script given below:

```
dcos security org service-accounts keypair service-account-private.pem service-account-public.pem
dcos security org service-accounts create -p service-account-public.pem -d "Service account for Scale data processing framework" service-account
dcos security secrets create-sa-secret --strict service-account-private.pem service-account service-account-secret

# Test with SUPERUSER perms on user
dcos security org users grant service-account dcos:superuser full

# Build image to test - update name in 2 commands below and marathon.json to your personal Docker Hub account name
docker build -t gisjedi/python-mesos-http -f Dockerfile-test-framework .
docker push gisjedi/python-mesos-http

# Deploy app consuming image to marathon
dcos marathon app add sample/marathon.json

# Once success has been confirmed, more granular permissions should be applied
dcos security org users revoke service-account dcos:superuser full
dcos security org users grant service-account dcos:mesos:agent:task create
dcos security org users grant service-account dcos:mesos:agent:task:app_id create
dcos security org users grant service-account dcos:mesos:agent:task:user:nobody create
dcos security org users grant service-account dcos:mesos:master:framework create
dcos security org users grant service-account dcos:mesos:master:reservation create
dcos security org users grant service-account dcos:mesos:master:reservation delete
dcos security org users grant service-account dcos:mesos:master:reservation:principal:service_account delete
dcos security org users grant service-account dcos:mesos:master:task create
dcos security org users grant service-account dcos:mesos:master:task:app_id create
dcos security org users grant service-account dcos:mesos:master:task:user:nobody create
dcos security org users grant service-account dcos:mesos:master:volume create
dcos security org users grant service-account dcos:mesos:master:volume delete
dcos security org users grant service-account dcos:mesos:master:volume:principal:service_account delete
```

# About

This library does not implement all options of mesos.proto and manages schedulers only (not executors). Implemented features should be enough to implement a scheduler, if something is missing, please ask, or contribute ;-)

# Install

    pip install mesoshttp

# Documentation

[![Documentation Status](https://readthedocs.org/projects/osalloupython-mesos-http/badge/?version=latest)](http://osalloupython-mesos-http.readthedocs.io/en/latest/?badge=latest)
