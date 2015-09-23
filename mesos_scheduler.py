import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

import scheduler
import json
import time
import threading

TASK_CPUS = 0.1
TASK_MEM = 128


class MesosScheduler(mesos.interface.Scheduler):
    def __init__(self, executor, record_queue):
        self.executor = executor
        self.scheduler = scheduler.Scheduler()
        self.record_queue = record_queue
        self.stored_driver = None

        # TODO(nnielsen): Reduce scope of lock
        self.scheduler_lock = threading.Lock()

        # TODO(nnielsen): Make 5s interval configurable.
        # Periodically run reconciliation and node updates.
        def stop_unmonitored_tasks():
            print "Reloading slaves @ %s" % time.ctime()

            self.scheduler_lock.acquire()

            self.scheduler.update()

            if self.stored_driver is not None:
                unmonitor = self.scheduler.unmonitor
                for slave in unmonitor:
                    print "Killing task %s: monitoring no longer needed" % slave.id
                    self.stored_driver.killTask(slave.id)

            self.scheduler_lock.release()

            threading.Timer(5, stop_unmonitored_tasks).start()

        stop_unmonitored_tasks()

    def registered(self, driver, framework_id, master_info):
        self.scheduler_lock.acquire()

        if self.stored_driver is None:
            self.stored_driver = driver

        # TODO(nnielsen): Persist in zookeeper
        print "Registered with framework ID %s" % framework_id.value
        self.scheduler.update(master_info)

        self.scheduler_lock.release()

    def resourceOffers(self, driver, offers):
        self.scheduler_lock.acquire()

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

            # Update slave state (move from monitor -> staging list) in a subsequent step, as we want to avoid
            # deleting items from the list we are iterating.
            for monitored_slave in monitored_slaves:
                self.scheduler.status_update(monitored_slave, mesos_pb2.TASK_STAGING)

            operation = mesos_pb2.Offer.Operation()
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            operation.launch.task_infos.extend(tasks)

            driver.acceptOffers([offer.id], [operation])

        self.scheduler_lock.release()

    def statusUpdate(self, driver, update):
        self.scheduler_lock.acquire()
        print "Task %s is in state %s" % (update.task_id.value, mesos_pb2.TaskState.Name(update.state))

        self.scheduler.status_update(update.task_id.value, update.state)

        # Pump samples through monitor.
        if update.state == mesos_pb2.TASK_RUNNING:
            if update.data is not None and 'timestamp' in update.data:
                # TODO(nnielsen): Write up JSON Schema for status update data.
                self.record_queue.put(json.loads(update.data))

        self.scheduler_lock.release()