"""
This module contains the Mesos side of the Stellar scheduler.
It packs monitoring tasks from the Stellar scheduler into the incoming offers and periodically
polls the "unmonitor" list and issues kill tasks to stop those tasks.
"""

# pylint: disable=no-member

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

import scheduler
import json
import time
import threading

TASK_CPUS = 0.1
TASK_MEM = 128


def new_task(executor, offer, slave):
    """
    Generates a new task info.

    :param executor: Shred Executor Info
    :param offer: Offer to launch on
    :param slave: Slave to monitor
    :return:
    """
    task = mesos_pb2.TaskInfo()
    task.task_id.value = slave.task_id
    task.slave_id.value = offer.slave_id.value
    task.name = "Monitor %s" % slave.hostname
    task.executor.MergeFrom(executor)

    cpus = task.resources.add()
    cpus.name = "cpus"
    cpus.type = mesos_pb2.Value.SCALAR
    cpus.scalar.value = TASK_CPUS

    mem = task.resources.add()
    mem.name = "mem"
    mem.type = mesos_pb2.Value.SCALAR
    mem.scalar.value = TASK_MEM

    task.data = json.dumps({'slave_location': slave.hostname})

    return task


class MesosScheduler(mesos.interface.Scheduler):
    """
    Implementation of Mesos scheduler which functions gets called by the backing sched code in
    libmesos.
    """

    def __init__(self, executor, record_queue):
        """
        Scheduler constructor.

        :param executor:        We want to run multiple monitoring tasks per executor and thus need
                                to share the exact executor info.
        :param record_queue:    Queue where received samples gets published.
        """

        self.executor = executor
        self.scheduler = scheduler.Scheduler()
        self.record_queue = record_queue
        self.stored_driver = None

        # TODO(nnielsen): Reduce scope of lock
        self.scheduler_lock = threading.Lock()

        # TODO(nnielsen): Make 1s interval configurable.
        # Periodically run reconciliation and node updates.
        def cleanup_task():
            """
            Runs cleanup tasks i.e. Kill tasks for Agents which have disappeared, reconcile running
            task states and get new node lists from the master.
            """

            print "Reloading slaves @ %s" % time.ctime()

            self.scheduler_lock.acquire()

            self.scheduler.update()

            if self.stored_driver is not None:
                unmonitor = self.scheduler.unmonitor
                for slave_id, _ in unmonitor.iteritems():
                    task_id = mesos_pb2.TaskID()
                    task_id.value = slave_id

                    if slave_id in self.scheduler.running or slave_id in self.scheduler.staging:
                        print "Killing task %s" % task_id.value
                        self.stored_driver.killTask(task_id)

                    # TODO(nnielsen): Introduce retry for task killing.

            self.scheduler_lock.release()

            threading.Timer(1, cleanup_task).start()

        cleanup_task()

    def registered(self, driver, framework_id, master_info):
        """
        This function is called when the scheduler has registered with the master.
        Only at this point do we load the node list and start feeding the scheduler loop.

        :param driver: Scheduler driver.
        :param framework_id: Assigned framework id.
        :param master_info: Master info of the attached master.
        """
        self.scheduler_lock.acquire()

        if self.stored_driver is None:
            self.stored_driver = driver

        # TODO(nnielsen): Persist in zookeeper
        print "Registered with framework ID %s" % framework_id.value
        self.scheduler.update(master_info)

        self.scheduler_lock.release()

    def launch_tasks(self, offer):
        """
        Determins how many tasks (if any) to launch on offer.

        :param offer: Offer to consider
        :return: List of tasks to launch on offer
        """

        remaining_cpus = 0
        remaining_mem = 0
        for resource in offer.resources:
            if resource.name == "cpus":
                remaining_cpus += resource.scalar.value
            elif resource.name == "mem":
                remaining_mem += resource.scalar.value

        monitored_slaves = []
        slaves = self.scheduler.monitor

        tasks = []

        for _, slave in slaves.iteritems():
            if remaining_cpus >= TASK_CPUS and remaining_mem >= TASK_MEM:
                # TODO(nnielsen): Make sure we don't exceed the/a maximum fanout.

                monitored_slaves.append(slave.task_id)

                print "Launching task %s using offer %s" % (slave.task_id, offer.id.value)

                task = new_task(self.executor, offer, slave)

                tasks.append(task)

                remaining_cpus -= TASK_CPUS
                remaining_mem -= TASK_MEM

        # Update slave state (move from monitor -> staging list) in a subsequent step, as we
        # want to avoid deleting items from the list we are iterating.
        for monitored_slave in monitored_slaves:
            self.scheduler.status_update(monitored_slave, mesos_pb2.TASK_STAGING)

        return tasks

    def resourceOffers(self, driver, offers):
        """
        This function is called when the allocator has formed a set of resource offers for the
        scheduler. The main decision logic for whether to accept or decline the offer is done here.

        :param driver: Scheduler driver.
        :param offers: Available resource offers.
        """
        self.scheduler_lock.acquire()

        for offer in offers:
            tasks = self.launch_tasks(offer)

            operation = mesos_pb2.Offer.Operation()
            operation.type = mesos_pb2.Offer.Operation.LAUNCH
            operation.launch.task_infos.extend(tasks)

            driver.acceptOffers([offer.id], [operation])

        self.scheduler_lock.release()

    def statusUpdate(self, driver, update):
        """
        This is the second part of the scheduling loop. As task launching and killing is done in
        an async fashion, we have to keep track of our desired state and make appropriate
        corrections (relaunch, stop monitoring, etc) based on the new knowledge.

        :param driver: Scheduler driver.
        :param update: The update describing the task state. This will also include sample data.
        """
        self.scheduler_lock.acquire()
        print "Task %s is in state %s" % \
              (update.task_id.value, mesos_pb2.TaskState.Name(update.state))

        self.scheduler.status_update(update.task_id.value, update.state)

        # Pump samples through monitor.
        if update.state == mesos_pb2.TASK_RUNNING:
            if update.data is not None and 'timestamp' in update.data:
                # TODO(nnielsen): Write up JSON Schema for status update data.
                self.record_queue.put(json.loads(update.data))

        self.scheduler_lock.release()
