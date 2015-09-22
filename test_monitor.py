from unittest import TestCase

import monitor
import Queue
import time


class TestMonitor(TestCase):
    def test_malformed_sample(self):
        record_queue = Queue.Queue()
        m = monitor.Monitor(record_queue)
        m.start()

        # Try to insert incorrect sample.
        record_queue.put([{
            'foobar': 'baz'
        }])

        m.stop()
        m.join()

    def test_samples(self):
        record_queue = Queue.Queue()
        m = monitor.Monitor(record_queue, 6, 10)
        m.start()

        # Get base for timestamps
        base = time.time()
        framework_id = "foobarbaz"
        executor_id = "foobarbaz"
        cpu_allocation_slack = 4
        cpu_usage_slack = 0.5
        cpu_usage = 0.5
        mem_allocation_slack = 45
        mem_usage_slack = 32
        mem_usage = 512

        for i in range(1200):
            record_queue.put([{
                'timestamp': base + i,
                'framework_id': framework_id,
                'executor_id': executor_id,
                'cpu_allocation_slack': cpu_allocation_slack,
                'cpu_usage_slack': cpu_usage_slack,
                'cpu_usage': cpu_usage,
                'mem_allocation_slack': mem_allocation_slack,
                'mem_usage_slack': mem_usage_slack,
                'mem_usage': mem_usage
            }])

        sample_min1 = m.cluster()

        self.assertEqual(len(sample_min1), 1)

        # All samples are identical and average should be the input samples.
        self.assertEqual(sample_min1[0]['cpu_allocation_slack'], cpu_allocation_slack)
        self.assertEqual(sample_min1[0]['cpu_usage_slack'], cpu_usage_slack)
        self.assertEqual(sample_min1[0]['cpu_usage'], cpu_usage)
        self.assertEqual(sample_min1[0]['mem_allocation_slack'], mem_allocation_slack)
        self.assertEqual(sample_min1[0]['mem_usage_slack'], mem_usage_slack)
        self.assertEqual(sample_min1[0]['mem_usage'], mem_usage)

        m.stop()
        m.join()