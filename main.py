#!/usr/bin/env python
"""
This module contain the entry for running the stellar framework.
"""

# pylint: disable=no-member, invalid-name

import Queue
import json
import os
import sys
import threading
import monitor
import time

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

import mesos_scheduler

from flask import request, render_template
from flask import Flask

# TODO(nnielsen): Use GLOG for debug logging (and leaving log lines in place).
# TODO(nnielsen): Limit/control fan-out per executor
# TODO(nnielsen): Introduce built-in chaos-monkey (tasks and executors dies after X minutes).

record_queue = Queue.Queue()
m = monitor.Monitor(record_queue)


# Path                                        Description
# /                                           Main hosted UI
# /cluster                                    Aggregate cluster statistics
# /framework/<framework id>/                  Aggregate statistics for framework
# /framework/<framework id>/<executor id>/    Aggregate statistics for executor of framework
app = Flask('stellar')


@app.route('/')
def ui():
    """
    / endpoint hosting the Web UI.

    :return: HTML rendered from templates/slack.html
    """
    return render_template("slack.html")


@app.route('/cluster')
def cluster():
    """
    /cluster endpoint hosting the statistics for the Web UI and external tooling.

    :return: JSON of usage statistics samples.
    """
    limit = int(request.args.get('limit', 1))
    print "Requesting %d samples" % limit
    return json.dumps(m.cluster(limit))


# TODO(nnielsen): Parse role from command line arguments.
# TODO(nnielsen): Parse secret and principal from command line arguments.
# TODO(nnielsen): Make Flask port configurable
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: %s master" % sys.argv[0]
        sys.exit(1)

    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = "default"
    executor.command.value = "python mesos_executor.py"
    executor.name = "Stellar Executor"

    # TODO(nnielsen): Run executor from docker hub instead.
    url = executor.command.uris.add()
    url.value = os.path.abspath("mesos_executor.py")
    url = executor.command.uris.add()
    url.value = os.path.abspath("json_helper.py")
    url = executor.command.uris.add()
    url.value = os.path.abspath("metrics.py")

    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""
    framework.name = "Stellar"
    framework.checkpoint = True

    # TODO(nnielsen): Get from ip from host.
    framework.webui_url = "http://127.0.1.1:5000/"

    # TODO(nnielsen): Reregister with existing FrameworkID.

    #
    # Start Mesos scheduler (backed by it's own thread).
    #
    if os.getenv("MESOS_AUTHENTICATE"):
        print "Enabling authentication for the framework"

        if not os.getenv("DEFAULT_PRINCIPAL"):
            print "Expecting authentication principal in the environment"
            sys.exit(1)

        credential = mesos_pb2.Credential()
        credential.principal = os.getenv("DEFAULT_PRINCIPAL")

        if os.getenv("DEFAULT_SECRET"):
            credential.secret = os.getenv("DEFAULT_SECRET")

        framework.principal = os.getenv("DEFAULT_PRINCIPAL")

        driver = mesos.native.MesosSchedulerDriver(
            mesos_scheduler.MesosScheduler(executor, record_queue),
            framework,
            sys.argv[1],
            credential)
    else:
        driver = mesos.native.MesosSchedulerDriver(
            mesos_scheduler.MesosScheduler(executor, record_queue),
            framework,
            sys.argv[1])

    #
    # Start monitor thread
    #
    m.start()

    #
    # Host stats endpoints
    #
    def run_flask():
        """
        Entry to HTTP server thread.
        """

        app.run(host='0.0.0.0')

    http_thread = threading.Thread(target=run_flask)
    http_thread.daemon = True
    http_thread.start()

    status = 0 if driver.start() == mesos_pb2.DRIVER_STOPPED else 1

    # Block main thread.
    while True:
        time.sleep(1)

    # Ensure that the driver process terminates.
    driver.stop()

    # TODO(nnielsen): Signal stop() to monitor thread.
    # TODO(nnielsen): Signal stop() to HTTP thread.

    m.join(2000)

    http_thread.join(2000)

    sys.exit(status)
