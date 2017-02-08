# Python mesos scheduler HTTP library

This is a Python library to create a Mesos scheduler. It includes the mesos.proto and compiled mesos.proto python modules.

It removes the need to use/install Mesos python bindings and makes use of the HTTP API (native bindings are not updated anymore with new features)

MesosClient is the main entrypoint that connects to the Mesos master. You can then register for callbacks on events.
On subscribe, you get a driver instance which you can use to send messages to master on current framework (kill, etc.)

MesosClient must be executed in a separate thread as it keeps a loop pooling connection with the master.

Submitted tasks should be in Protobuf format, library will convert them to JSON format. Tasks can also be directly provided in JSON, up to you to define the TaskInfo in JSON format...

See sample/test.py for example.

# Documentation

[![Documentation Status](https://readthedocs.org/projects/osalloupython-mesos-http/badge/?version=latest)](http://osalloupython-mesos-http.readthedocs.io/en/latest/?badge=latest)



# Status

Do not use for production or at your own risks....
