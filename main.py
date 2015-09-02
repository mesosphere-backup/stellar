#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import Queue
import json
import os
import sys
import threading
import monitor

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

import mesos_scheduler

from flask import request
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
    return 'Stellar'


@app.route('/cluster')
def cluster():
    limit = int(request.args.get('limit', 1))
    return json.dumps(m.cluster(limit))


# TODO(nnielsen): Parse role from command line arguments.
# TODO(nnielsen): Parse secret and principal from command line arguments.
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: %s master" % sys.argv[0]
        sys.exit(1)

    executor = mesos_pb2.ExecutorInfo()
    executor.executor_id.value = "default"
    executor.command.value = "python collect.py"
    executor.name = "Stellar Executor"

    # TODO(nnielsen): Run executor from docker hub instead.
    url = executor.command.uris.add()
    url.value = "/home/vagrant/stellar/collect.py"
    url = executor.command.uris.add()
    url.value = "/home/vagrant/stellar/json_helper.py"
    url = executor.command.uris.add()
    url.value = "/home/vagrant/stellar/metrics.py"

    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""
    framework.name = "Stellar"
    framework.checkpoint = True
    # TODO(nnielsen): Reregister with existing FrameworkID.

    #
    # Start Mesos scheduler (backed by it's own thread).
    #
    if os.getenv("MESOS_AUTHENTICATE"):
        print "Enabling authentication for the framework"

        if not os.getenv("DEFAULT_PRINCIPAL"):
            print "Expecting authentication principal in the environment"
            sys.exit(1);

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
        app.run(host='0.0.0.0')
    http_thread = threading.Thread(target=run_flask)
    http_thread.start()

    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1

    # Ensure that the driver process terminates.
    driver.stop()

    m.join()

    http_thread.join()

    sys.exit(status)
