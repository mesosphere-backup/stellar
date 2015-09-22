import uuid
import json_helper

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native


class Slave:
    def __init__(self, hostname):
        self.id = str(uuid.uuid4())
        self.hostname = hostname


# TODO(nnielsen): Run Stellar scheduler in it's own thread.
# TODO(nnielsen): Introduce print_stats() method which prints counts, last stats etc. for logging
# TODO(nnielsen): Introduce stats() method which return recent metrics
# TODO(nnielsen): Introduce heart beats from the executors.
class Scheduler:
    def __init__(self):
        self.master_info = None

        # target i.e. which slaves _should_ be monitored
        self.targets = {}

        # TODO(nnielsen): Introduce set of slaves _being_ unmonitored. Message may be lost during
        # master fail-over and scheduler should retry after timeout.
        # changes (additions / removal) Which monitor tasks should be start or removed.
        self.monitor = {}
        self.staging = {}
        self.unmonitor = {}

        # current i.e. which slaves are currently monitored
        self.current = {}

    # TODO(nnielsen): Verify slave removal.
    def update(self, master_info=None, state_json=None):
        """
        Get new node list from master.
        If master_info is set (during registration and reregistration), a new master url will be set.
        """
        if master_info is not None:
            self.master_info = master_info

        # For testing, allow caller to give state_json.
        if state_json is None:
            state_endpoint = "http://" + self.master_info.hostname + ":" + str(self.master_info.port) + "/state.json"
            state_json = json_helper.from_url(state_endpoint)

        # Get node list
        new_targets = []
        for slave in state_json['slaves']:
            new_targets.append(slave['pid'].split('@')[1])

        # Make copy of current targets, to identify deactivated slaves
        inactive_slaves = self.targets

        for new_target in new_targets:
            if new_target not in self.targets:
                slave = Slave(new_target)

                # TODO(nnielsen): Persist map id -> host to zookeeper.

                self.monitor[slave.id] = slave
                self.targets[slave.hostname] = slave
                del inactive_slaves[slave.hostname]

        if len(inactive_slaves) > 0:
            print "%d slaves to be unmonitored" % len(inactive_slaves)
            for inactive_slave in inactive_slaves:
                # TODO(nnielsen): Remove from monitor queue as well.
                self.unmonitor[inactive_slave.id] = inactive_slave

    def status_update(self, slave_id, state):
        print "########## Status Update ##########"
        print "Slaves to monitor:        %d" % len(self.monitor)
        print "Slaves staging:           %d" % len(self.staging)
        print "Slaves monitored:         %d" % len(self.current)
        print "Slaves to unmonitor:      %d" % len(self.unmonitor)
        print "###################################"

        # If task is staging (synthesized by the mesos scheduler loop to represent 'launched but not yet started',
        # we move slave from 'monitor' to 'staging' list. If the launch fails, we need to move the slave back to the
        # 'monitor' list.
        if state == mesos_pb2.TASK_STAGING:
            if slave_id not in self.monitor:
                print "WARNING: Received TASK_STAGING for non-monitored task"
            else:
                print "Task %s is now staging" % slave_id
                slave = self.monitor[slave_id]
                self.staging[slave_id] = slave
                del self.monitor[slave_id]

        if state == mesos_pb2.TASK_RUNNING:
            if slave_id in self.staging:
                print "Task %s is now monitoring" % slave_id
                slave = self.staging[slave_id]
                del self.staging[slave_id]
                self.current[slave_id] = slave

        if state == mesos_pb2.TASK_LOST or state == mesos_pb2.TASK_FAILED or state == mesos_pb2.TASK_ERROR:
            if slave_id in self.current:
                print "Lost task %s: rescheduling for monitoring" % slave_id
                slave = self.current[slave_id]
                del self.current[slave_id]
                self.monitor[slave_id] = slave

        if state == mesos_pb2.TASK_KILLED:
            if slave_id not in self.unmonitor:
                if slave_id in self.current:
                    print "Unintentional kill of task %s: rescheduling" % slave_id
                    slave = self.current[slave_id]
                    del self.current[slave_id]
                    self.monitor[slave_id] = slave

                elif slave_id in self.staging:
                    print "Unintentional kill of task %s: rescheduling" % slave_id
                    slave = self.staging[slave_id]
                    del self.staging[slave_id]
                    self.monitor[slave_id] = slave
