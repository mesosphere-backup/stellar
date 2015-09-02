import uuid
import json_helper


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

        # TODO(nnielsen): Introduce set of slaves _being_ unmonitored. Message may be lost during
        # master fail-over and scheduler should retry after timeout.
        # changes (additions / removal) Which monitor tasks should be start or removed.
        self.monitor = {}
        self.staging = {}
        self.unmonitor = {}

        # current i.e. which slaves are currently monitored
        self.current = {}

    def update(self, master_info=None):
        """
        Get new node list from master.
        If master_info is set (during registration and reregistration), a new master url will be set.
        """
        if master_info is not None:
            self.master_info = master_info

        state_endpoint = "http://" + self.master_info.hostname + ":" + str(self.master_info.port) + "/state.json"

        state_json = json_helper.from_url(state_endpoint)

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
