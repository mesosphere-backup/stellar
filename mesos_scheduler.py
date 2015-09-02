import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

import scheduler
import json

TASK_CPUS = 0.1
TASK_MEM = 128


class MesosScheduler(mesos.interface.Scheduler):
    def __init__(self, executor, record_queue):
        self.executor = executor
        self.scheduler = scheduler.Scheduler()
        self.record_queue = record_queue

    def registered(self, driver, frameworkId, masterInfo):
        # TODO(nnielsen): Persist in zookeeper
        print "Registered with framework ID %s" % frameworkId.value
        self.scheduler.update(masterInfo)

    def resourceOffers(self, driver, offers):
        for offer in offers:
            tasks = []
            offer_cpus = 0
            offer_mem = 0
            for resource in offer.resources:
                if resource.name == "cpus":
                    offer_cpus += resource.scalar.value
                elif resource.name == "mem":
                    offer_mem += resource.scalar.value

            print "Received offer %s with cpus: %s and mem: %s" % (offer.id.value, offer_cpus, offer_mem)

            remaining_cpus = offer_cpus
            remaining_mem = offer_mem

            monitored_slaves = []
            slaves = self.scheduler.monitor

            for slave_id, slave in slaves.iteritems():
                if remaining_cpus >= TASK_CPUS and remaining_mem >= TASK_MEM:
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

                    remaining_cpus -= TASK_CPUS
                    remaining_mem -= TASK_MEM

            for monitored_slave in monitored_slaves:
                del self.scheduler.monitor[monitored_slave]

            print "Slaves to monitor: %d" % len(slaves)
            print "Launching %d tasks" % len(tasks)

            operation = mesos_pb2.Offer.Operation()
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            operation.launch.task_infos.extend(tasks)

            driver.acceptOffers([offer.id], [operation])

    def statusUpdate(self, driver, update):
        print "Task %s is in state %s" % (update.task_id.value, mesos_pb2.TaskState.Name(update.state))

        if update.state == mesos_pb2.TASK_RUNNING:
            if update.data is not None and 'timestamp' in update.data:
                self.record_queue.put(json.loads(update.data))

        # TODO(nnielsen): Periodically check for new/deactivated slaves with monitor.update()

        if update.state == mesos_pb2.TASK_FINISHED:
            # TODO(nnielsen): Remove slave from current list.
            scheduler.update()

        if update.state == mesos_pb2.TASK_LOST or \
           update.state == mesos_pb2.TASK_KILLED or \
           update.state == mesos_pb2.TASK_FAILED:
            # TODO(nnielsen): Reschedule monitor task
            print "Aborting because task %s is in unexpected state %s with message '%s'" \
                % (update.task_id.value, mesos_pb2.TaskState.Name(update.state), update.message)
