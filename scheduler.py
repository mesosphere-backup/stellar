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
import time
import urllib
import uuid

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

from flask import Flask
app = Flask('stellar')

TASK_CPUS = 0.1
TASK_MEM = 128

# TODO(nnielsen): Limit/control fanout per executor
# TODO(nnielsen): Introduce built-in chaos-monkey (tasks and executors dies after X minutes).

def json_from_url(url):
    while True:
        try:
            response = urllib.urlopen(url)
            data = response.read()
            return json.loads(data)
        except IOError:
            print "Could not load %s: retrying in one second" % url
            time.sleep(1)
            continue

class Slave:
    def __init__(self, hostname):
        self.id = str(uuid.uuid4())
        self.hostname = hostname

# TODO(nnielsen): Run Stellar scheduler in it's own thread.
class Scheduler:
    def __init__(self):
        self.master_info = None

        # target i.e. which slaves _should_ be monitored
        self.targets = {}

        # changes (additions / removal) Which monitor tasks should be start or removed.
        self.monitor = {}
        self.staging = {}
        self.unmonitor = {}

        # current i.e. which slaves are currently monitored
        self.current = {}

    def update(self, master_info = None):
        """
        Get new node list from master.
        If master_info is set (during registration and reregistration), a new master url will be set.
        """
        if master_info is not None:
            self.master_info = master_info

        state_endpoint = "http://" + self.master_info.hostname + ":" + str(self.master_info.port) + "/state.json"

        state_json = json_from_url(state_endpoint)

        new_targets = []
        for slave in state_json['slaves']:
            new_targets.append(slave['pid'].split('@')[1])

        inactive_slaves = self.targets
        for new_target in new_targets:
            if new_target not in self.targets:
                slave = Slave(new_target)
                self.monitor[slave.id] = slave
                self.targets[slave.hostname] = slave
                del inactive_slaves[slave.hostname]

        if len(inactive_slaves) > 0:
            print "%d slaves to be unmonitored" % len(inactive_slaves)
            for inactive_slave in inactive_slaves:
                self.unmonitor[inactive_slave.id] = inactive_slave


# TODO(nnielsen): Share queue between monitor and flask thread.
class Monitor:
    def __init__(self, record_queue, request_queue):
        # TODO(nnielsen): Keep samples for max 1hr
        # TODO(nnielsen): Maintain circular buffer for samples.
        self.record_queue = record_queue
        self.request_queue = request_queue

    def start(self):
        def run_task():
            # TODO(nnielsen): Use real select(). This can easily be a bottleneck.
            default_timeout = 0.1
            max_timeout = 2
            timeout = default_timeout
            while True:
                try:
                    record = self.record_queue.get_nowait()
                    timeout = default_timeout
                    print record
                except Queue.Empty:
                    timeout = max(timeout * 2, max_timeout)

                time.sleep(timeout)


        self.thread = threading.Thread(target=run_task)
        self.thread.start()

    def join(self):
        self.thread.join()

    def cluster(self):
        pass

    def slave(self, slave_id):
        pass

    def framework(self, framework_id):
        pass

    def executor(self, framework_id, task_id):
        pass


class MesosScheduler(mesos.interface.Scheduler):
    def __init__(self, executor, record_queue):
        self.executor = executor
        self.scheduler = Scheduler()
        self.record_queue = record_queue

    def registered(self, driver, frameworkId, masterInfo):
        # TODO(nnielsen): Persist in zookeeper
        print "Registered with framework ID %s" % frameworkId.value
        self.scheduler.update(masterInfo)

    def resourceOffers(self, driver, offers):
        for offer in offers:
            tasks = []
            offerCpus = 0
            offerMem = 0
            for resource in offer.resources:
                if resource.name == "cpus":
                    offerCpus += resource.scalar.value
                elif resource.name == "mem":
                    offerMem += resource.scalar.value

            print "Received offer %s with cpus: %s and mem: %s" % (offer.id.value, offerCpus, offerMem)

            remainingCpus = offerCpus
            remainingMem = offerMem

            # TODO(nnielsen): Decline unused offers.

            monitored_slaves = []
            slaves = self.scheduler.monitor
            for slave_id, slave in slaves.iteritems():
                if remainingCpus >= TASK_CPUS and remainingMem >= TASK_MEM:
                    monitored_slaves.append(slave.id)
                    self.scheduler.staging = slave

                    print "Launching task %s using offer %s" % (slave.id, offer.id.value)

                    task = mesos_pb2.TaskInfo()
                    task.task_id.value = slave.id
                    task.slave_id.value = offer.slave_id.value
                    task.name = "Monitor %s" % slave.hostname
                    task.executor.MergeFrom(self.executor)

                    cpus = task.resources.add()
                    cpus.name = "cpus"
                    cpus.type = mesos_pb2.Value.SCALAR
                    cpus.scalar.value = TASK_CPUS

                    mem = task.resources.add()
                    mem.name = "mem"
                    mem.type = mesos_pb2.Value.SCALAR
                    mem.scalar.value = TASK_MEM

                    task.data = json.dumps({'slave_location': slave.hostname})

                    tasks.append(task)

                    remainingCpus -= TASK_CPUS
                    remainingMem -= TASK_MEM

            for monitored_slave in monitored_slaves:
                del self.scheduler.monitor[monitored_slave]

            operation = mesos_pb2.Offer.Operation()
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            operation.launch.task_infos.extend(tasks)

            driver.acceptOffers([offer.id], [operation])

    def statusUpdate(self, driver, update):
        print "Task %s is in state %s" % (update.task_id.value, mesos_pb2.TaskState.Name(update.state))

        if update.state == mesos_pb2.TASK_RUNNING:
            if update.data is not None:
                self.record_queue.put(update.data)

        # TODO(nnielsen): Update node list
        if update.state == mesos_pb2.TASK_FINISHED:
            pass

        if update.state == mesos_pb2.TASK_LOST or \
           update.state == mesos_pb2.TASK_KILLED or \
           update.state == mesos_pb2.TASK_FAILED:
            # TODO(nnielsen): Reschedule monitor task
            print "Aborting because task %s is in unexpected state %s with message '%s'" \
                % (update.task_id.value, mesos_pb2.TaskState.Name(update.state), update.message)

# Path                                        Description
# /                                           Main hosted UI
# /cluster                                    Aggregate cluster statistics
# /framework/<framework id>/                  Aggregate statistics for framework
# /framework/<framework id>/<executor id>/    Aggregate statistics for executor of framework
@app.route('/')
def ui():
    return 'Stellar'

@app.route('/cluster')
def cluster():
    cluster_stats = None
    return json.dumps({})

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

    url = executor.command.uris.add()
    url.value = "/home/vagrant/stellar/collect.py"

    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""
    framework.name = "Stellar"
    framework.checkpoint = True

    record_queue = Queue.Queue()

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
            MesosScheduler(executor, record_queue),
            framework,
            sys.argv[1],
            credential)
    else:
        driver = mesos.native.MesosSchedulerDriver(
            MesosScheduler(executor, record_queue),
            framework,
            sys.argv[1])

    monitor = Monitor(record_queue, None)
    monitor.start()

    def run_flask():
        app.run(host='0.0.0.0')
    http_thread = threading.Thread(target=run_flask)
    http_thread.start()

    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1

    # Ensure that the driver process terminates.
    driver.stop()

    monitor.join()

    http_thread.join()

    sys.exit(status)
