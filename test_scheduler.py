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
        self.assertEqual(len(s.current), 0)

        # Mimic that we launched the task
        s.status_update(s.targets['foo-01'].id, mesos_pb2.TASK_STAGING)

        # Should now be in staging queue
        self.assertEqual(len(s.monitor), 0)
        self.assertEqual(len(s.staging), 1)
        self.assertEqual(len(s.current), 0)

        # Mimic that task is running
        s.status_update(s.targets['foo-01'].id, mesos_pb2.TASK_RUNNING)

        self.assertEqual(len(s.monitor), 0)
        self.assertEqual(len(s.staging), 0)
        self.assertEqual(len(s.current), 1)

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
        self.assertEqual(len(s.current), 0)

        # Mimic that we launched the task
        s.status_update(s.targets['foo-01'].id, mesos_pb2.TASK_STAGING)

        # Should now be in staging queue
        self.assertEqual(len(s.monitor), 0)
        self.assertEqual(len(s.staging), 1)
        self.assertEqual(len(s.current), 0)

        # Mimic that task failed. Verify that it is back in monitor queue.
        s.status_update(s.targets['foo-01'].id, mesos_pb2.TASK_LOST)

        self.assertEqual(len(s.monitor), 1)
        self.assertEqual(len(s.staging), 0)
        self.assertEqual(len(s.current), 0)

if __name__ == '__main__':
    unittest.main()
