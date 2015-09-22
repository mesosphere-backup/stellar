from unittest import TestCase

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

import scheduler
import unittest


class TestScheduler(TestCase):
    def test_foobar(self):
        s = scheduler.Scheduler()

        master_info = mesos_pb2.MasterInfo()
        s.update(master_info)

        self.assertEqual(0, 1)


if __name__ == '__main__':
    unittest.main()
