# Python mesos scheduler HTTP library

This is a Python library to create a Mesos scheduler.

It removes the need to use/install Mesos python bindings and makes use of the HTTP API (native bindings are not updated anymore with new features)

MesosClient is the main entrypoint that connects to the Mesos master. You can then register for callbacks on events.
On subscribe, you get a driver instance which you can use to send messages to master on current framework (kill, etc.)

MesosClient must be executed in a separate thread as it keeps a loop pooling connection with the master.

Submitted tasks should be in JSON format (according to mesos.proto).

See sample/test.py for example.

Callbacks will "block" the mesos message treatment, so they should be short, or messages should be forwarded to a queue in an other thread/process where longer tasks will handle messages.

# About

This library does not implement all options of mesos.proto and manages schedulers only (not executors). Implemented features should be enough to implement a scheduler, if something is missing, please ask, or contribute ;-)

# Documentation

[![Documentation Status](https://readthedocs.org/projects/osalloupython-mesos-http/badge/?version=latest)](http://osalloupython-mesos-http.readthedocs.io/en/latest/?badge=latest)
