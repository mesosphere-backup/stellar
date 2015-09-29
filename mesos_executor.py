"""
This module contains the Stellar Executor and code for monitoring tasks.
"""

# pylint: disable=no-member, invalid-name

import sys
import threading
import time

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

import metrics


# TODO(nnielsen): Factor out data processing into helper class for testing.
class MesosExecutor(mesos.interface.Executor):
    """
    Executor managing tasks to load container statistics periodically.
    """

    def launchTask(self, executor_driver, task):
        """
        This function is called by the exec code from libmesos when the executor has registered
        successfully with the Agent and tasks can be launched.

        :param executor_driver: MesosExecutorDriver
        :param task: The task to launch
        :return: None
        """

        thread = threading.Thread(
            target=metrics.run_task, args=[executor_driver, task])
        thread.start()

if __name__ == "__main__":
    time.sleep(1)
    print "Starting Stellar executor"
    driver_status = mesos.native.MesosExecutorDriver(MesosExecutor())
    sys.exit(0 if driver_status.run() == mesos_pb2.DRIVER_STOPPED else 1)
