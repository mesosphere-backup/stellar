from unittest import TestCase

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

import scheduler
import unittest


class TestScheduler(TestCase):
    def test_task_launch(self):
        s = scheduler.Scheduler()

        state_data = {
            'slaves': [
                {
                    'id': '1',
                    'pid': 'slave@foo-01'
                }
            ]
        }
        s.update(None, state_data)

        self.assertEqual(len(s.monitor), 1)
        self.assertEqual(len(s.targets), 1)
        self.assertEqual(len(s.staging), 0)
        self.assertEqual(len(s.running), 0)

        # Mimic that we launched the task
        s.status_update(s.targets['foo-01'].id, mesos_pb2.TASK_STAGING)

        # Should now be in staging queue
        self.assertEqual(len(s.monitor), 0)
        self.assertEqual(len(s.staging), 1)
        self.assertEqual(len(s.running), 0)

        # Mimic that task is running
        s.status_update(s.targets['foo-01'].id, mesos_pb2.TASK_RUNNING)

        self.assertEqual(len(s.monitor), 0)
        self.assertEqual(len(s.staging), 0)
        self.assertEqual(len(s.running), 1)

    def test_task_failure(self):
        print "Task failure test"

        s = scheduler.Scheduler()

        state_data = {
            'slaves': [
                {
                    'id': '1',
                    'pid': 'slave@foo-01'
                }
            ]
        }
        s.update(None, state_data)

        self.assertEqual(len(s.monitor), 1)
        self.assertEqual(len(s.targets), 1)
        self.assertEqual(len(s.staging), 0)
        self.assertEqual(len(s.running), 0)

        # Mimic that we launched the task
        s.status_update(s.targets['foo-01'].id, mesos_pb2.TASK_STAGING)

        # Should now be in staging queue
        self.assertEqual(len(s.monitor), 0)
        self.assertEqual(len(s.staging), 1)
        self.assertEqual(len(s.running), 0)

        # Mimic that task failed. Verify that it is back in monitor queue.
        s.status_update(s.targets['foo-01'].id, mesos_pb2.TASK_LOST)

        self.assertEqual(len(s.monitor), 1)
        self.assertEqual(len(s.staging), 0)
        self.assertEqual(len(s.running), 0)

    def test_multiple_tasks(self):
        s = scheduler.Scheduler()

        state_data = {
            'slaves': [
                {
                    'id': '1',
                    'pid': 'slave@foo-01'
                },
                {
                    'id': '2',
                    'pid': 'slave@foo-02'
                }
            ]
        }
        s.update(None, state_data)

        self.assertEqual(len(s.monitor), 2)
        self.assertEqual(len(s.targets), 2)
        self.assertEqual(len(s.staging), 0)
        self.assertEqual(len(s.running), 0)

        # Mimic that we launched the task
        s.status_update(s.targets['foo-01'].id, mesos_pb2.TASK_STAGING)

        # Should now be in staging queue
        self.assertEqual(len(s.monitor), 1)
        self.assertEqual(len(s.staging), 1)
        self.assertEqual(len(s.running), 0)

        # Mimic that we launched the task
        s.status_update(s.targets['foo-02'].id, mesos_pb2.TASK_STAGING)

        # Should now be in staging queue
        self.assertEqual(len(s.monitor), 0)
        self.assertEqual(len(s.staging), 2)
        self.assertEqual(len(s.running), 0)

        s.status_update(s.targets['foo-01'].id, mesos_pb2.TASK_RUNNING)
        self.assertEqual(len(s.monitor), 0)
        self.assertEqual(len(s.staging), 1)
        self.assertEqual(len(s.running), 1)

        s.status_update(s.targets['foo-02'].id, mesos_pb2.TASK_RUNNING)
        self.assertEqual(len(s.monitor), 0)
        self.assertEqual(len(s.staging), 0)
        self.assertEqual(len(s.running), 2)

    def test_unmonitor_slave(self):
        s = scheduler.Scheduler()

        state_data = {
            'slaves': [
                {
                    'id': '1',
                    'pid': 'slave@foo-01'
                },
                {
                    'id': '2',
                    'pid': 'slave@foo-02'
                }
            ]
        }
        s.update(None, state_data)

        self.assertEqual(len(s.monitor), 2)
        self.assertEqual(len(s.targets), 2)
        self.assertEqual(len(s.staging), 0)
        self.assertEqual(len(s.running), 0)

        # Mimic that we launched the task
        s.status_update(s.targets['foo-01'].id, mesos_pb2.TASK_STAGING)

        # Should now be in staging queue
        self.assertEqual(len(s.monitor), 1)
        self.assertEqual(len(s.staging), 1)
        self.assertEqual(len(s.running), 0)

        # Mimic that we launched the task
        s.status_update(s.targets['foo-02'].id, mesos_pb2.TASK_STAGING)

        # Should now be in staging queue
        self.assertEqual(len(s.monitor), 0)
        self.assertEqual(len(s.staging), 2)
        self.assertEqual(len(s.running), 0)

        s.status_update(s.targets['foo-01'].id, mesos_pb2.TASK_RUNNING)
        self.assertEqual(len(s.monitor), 0)
        self.assertEqual(len(s.staging), 1)
        self.assertEqual(len(s.running), 1)

        s.status_update(s.targets['foo-02'].id, mesos_pb2.TASK_RUNNING)
        self.assertEqual(len(s.monitor), 0)
        self.assertEqual(len(s.staging), 0)
        self.assertEqual(len(s.running), 2)

        state_data = {
            'slaves': [
                {
                    'id': '2',
                    'pid': 'slave@foo-02'
                }
            ]
        }
        s.update(None, state_data)

        self.assertEqual(len(s.monitor), 0)
        self.assertEqual(len(s.staging), 0)
        self.assertEqual(len(s.running), 2)
        self.assertEqual(len(s.unmonitor), 1)

        # Mimic killing of task.
        s.status_update(s.targets['foo-01'].id, mesos_pb2.TASK_KILLED)
        self.assertEqual(len(s.monitor), 0)
        self.assertEqual(len(s.staging), 0)
        self.assertEqual(len(s.running), 1)
        self.assertEqual(len(s.unmonitor), 0)

    def test_selfmonitored_slave_lost(self):
        s = scheduler.Scheduler()

        state_data = {
            'slaves': [
                {
                    'id': '1',
                    'pid': 'slave@foo-01'
                }
            ]
        }
        s.update(None, state_data)

        self.assertEqual(len(s.monitor), 1)
        self.assertEqual(len(s.targets), 1)
        self.assertEqual(len(s.staging), 0)
        self.assertEqual(len(s.running), 0)

        # Mimic that we launched the task
        s.status_update(s.targets['foo-01'].id, mesos_pb2.TASK_STAGING)

        # Should now be in staging queue
        self.assertEqual(len(s.monitor), 0)
        self.assertEqual(len(s.staging), 1)
        self.assertEqual(len(s.running), 0)

        s.status_update(s.targets['foo-01'].id, mesos_pb2.TASK_RUNNING)
        self.assertEqual(len(s.monitor), 0)
        self.assertEqual(len(s.staging), 0)
        self.assertEqual(len(s.running), 1)

        state_data = {
            'slaves': []
        }
        s.update(None, state_data)

        self.assertEqual(len(s.monitor), 0)
        self.assertEqual(len(s.staging), 0)
        self.assertEqual(len(s.running), 1)
        self.assertEqual(len(s.unmonitor), 1)

        # Slave is gone - but so is the task that was running on it.
        s.status_update(s.targets['foo-01'].id, mesos_pb2.TASK_LOST)
        self.assertEqual(len(s.monitor), 0)
        self.assertEqual(len(s.staging), 0)
        self.assertEqual(len(s.running), 0)
        self.assertEqual(len(s.unmonitor), 0)

    def test_unknown_slave_gone(self):
        print "Unknown slave gone"

        s = scheduler.Scheduler()

        state_data = {
            'slaves': [
                {
                    'id': '1',
                    'pid': 'slave@foo-01'
                }
            ]
        }
        s.update(None, state_data)

        self.assertEqual(len(s.monitor), 1)
        self.assertEqual(len(s.targets), 1)
        self.assertEqual(len(s.staging), 0)
        self.assertEqual(len(s.running), 0)

        state_data = {
            'slaves': []
        }
        s.update(None, state_data)

        # Should now be in staging queue
        self.assertEqual(len(s.monitor), 0)
        self.assertEqual(len(s.targets), 0)
        self.assertEqual(len(s.staging), 0)
        self.assertEqual(len(s.running), 0)


if __name__ == '__main__':
    unittest.main()
