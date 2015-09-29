"""
This module contain the main scheduler in Stellar (not the Mesos Scheduler, but the component
which gets Agent lists from the current active master and generate tasks to monitor new Agents
or killTask requests to stop monitoring inactive Agents.
"""

# pylint: disable=unused-import

import uuid
import json_helper
import copy

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native


class Agent(object):
    """
    Simple container to generate an unique id for the Agent (for the task id) and mapping it
    to the hostname and port to monitor.
    """
    def __init__(self, hostname):
        self.task_id = str(uuid.uuid4())
        self.hostname = hostname


# TODO(nnielsen): Run Stellar scheduler in it's own thread.
# TODO(nnielsen): Introduce print_stats() method which prints counts, last stats etc. for logging
# TODO(nnielsen): Introduce stats() method which return recent metrics
# TODO(nnielsen): Introduce heart beats from the executors.
# TODO(nnielsen): Introduce way to encode the tracking_tasks as state machines themselves, and have
#                 them move between Queues.
class Scheduler(object):
    """
    Scheduler in Stellar (not the Mesos Scheduler, but the component which gets Agent lists from
    the current active master and generate tasks to monitor new Agents or killTask requests to stop
    monitoring inactive Agents.
    """

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

        # Running i.e. which slaves are currently monitored
        self.running = {}

    def update(self, master_info=None, state_json=None):
        """
        Get new node list from master.
        If master_info is set (during registration and reregistration),
        a new master url will be set.
        """
        if master_info is not None:
            self.master_info = master_info

        # We have no way to update; abort
        if state_json is None and self.master_info is None:
            return

        # For testing, allow caller to give state_json.
        if state_json is None and self.master_info is not None:
            state_endpoint = "http://" + self.master_info.hostname + ":" + \
                str(self.master_info.port) + "/state.json"

            state_json = json_helper.from_url(state_endpoint)

        # Get node list.
        new_targets = []
        for slave in state_json['slaves']:
            new_targets.append(slave['pid'].split('@')[1])

        print "New targets: %s" % new_targets

        # Make copy of current targets, to identify deactivated slaves
        # TODO(nnielsen): Find lighter weight way of doing this.
        inactive_slaves = copy.deepcopy(self.targets)

        for new_target in new_targets:
            if new_target not in self.targets:
                slave = Agent(new_target)

                # TODO(nnielsen): Persist map id -> host to zookeeper.

                self.monitor[slave.task_id] = slave
                self.targets[new_target] = slave

            if new_target in inactive_slaves:
                print "Don't remove %s" % new_target
                del inactive_slaves[new_target]

        if len(inactive_slaves) > 0:
            print "%d slaves to be unmonitored" % len(inactive_slaves)
            for inactive_slave, slave in inactive_slaves.iteritems():
                print "inactive_slave: %s" % inactive_slave
                # TODO(nnielsen): Remove from monitor queue as well.
                self.unmonitor[slave.task_id] = inactive_slave

                if slave.task_id in self.monitor:
                    # Don't try to schedule for monitoring, if we decided slave is gone.
                    del self.monitor[slave.task_id]

                    # And no longer a target.
                    if slave.hostname in self.targets:
                        del self.targets[slave.hostname]

        self.stats()

    def status_update(self, slave_id, state):
        """
        If task is staging (synthesized by the mesos scheduler loop to represent
        'launched but not yet started',
        we move slave from 'monitor' to 'staging' list. If the launch fails,
        we need to move the slave back to the 'monitor' list.
        """

        print "Received %s in state %d %d" % (slave_id, state, mesos_pb2.TASK_LOST)

        if state == mesos_pb2.TASK_STAGING:
            if slave_id not in self.monitor:
                print "WARNING: Received TASK_STAGING for non-monitored task"
            else:
                print "Task %s is now staging" % slave_id
                slave = self.monitor[slave_id]
                self.staging[slave_id] = slave
                del self.monitor[slave_id]

        elif state == mesos_pb2.TASK_RUNNING:
            if slave_id in self.staging:
                print "Task %s is now monitoring" % slave_id
                slave = self.staging[slave_id]
                del self.staging[slave_id]
                self.running[slave_id] = slave

        elif (state == mesos_pb2.TASK_LOST) or \
             (state == mesos_pb2.TASK_FAILED) or \
             (state == mesos_pb2.TASK_ERROR):

            print "Lost task %s: rescheduling for monitoring" % slave_id

            if slave_id in self.unmonitor:
                del self.unmonitor[slave_id]

                # Don't restart: we intended to kill task.
                if slave_id in self.running:
                    del self.running[slave_id]

                elif slave_id in self.staging:
                    del self.staging[slave_id]

            elif slave_id in self.running:
                slave = self.running[slave_id]
                del self.running[slave_id]
                self.monitor[slave_id] = slave

            elif slave_id in self.staging:
                slave = self.staging[slave_id]
                del self.staging[slave_id]
                self.monitor[slave_id] = slave
            else:
                print "WARNING: Won't restart task. Task not found."

        elif state == mesos_pb2.TASK_KILLED:
            if slave_id not in self.unmonitor:
                if slave_id in self.running:
                    print "Unintentional kill of task %s: rescheduling" % slave_id
                    slave = self.running[slave_id]
                    del self.running[slave_id]
                    self.monitor[slave_id] = slave

                elif slave_id in self.staging:
                    print "Unintentional kill of task %s: rescheduling" % slave_id
                    slave = self.staging[slave_id]
                    del self.staging[slave_id]
                    self.monitor[slave_id] = slave
            else:
                if slave_id in self.running:
                    del self.running[slave_id]

                elif slave_id in self.staging:
                    del self.staging[slave_id]

                del self.unmonitor[slave_id]
        else:
            print "Warning: unhandled task state"

        self.stats()

    def stats(self):
        """
        Prints out statistics about agent queues.
        """
        # TODO(nnielsen): Update stats/metrics object instead.
        print "########## Status Update ##########"
        print "Slaves to monitor:        %d" % len(self.monitor)
        print "Slaves staging:           %d" % len(self.staging)
        print "Slaves monitored:         %d" % len(self.running)
        print "Slaves to unmonitor:      %d" % len(self.unmonitor)
        print "###################################"
