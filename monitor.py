import threading
import Queue
import json


# TODO(nnielsen): Make this an 'Aggregate' instead (with count, min, max,
# etc).
class Average:
    def __init__(self):
        self.count = 0
        self.sum = 0.0
        self.min = None
        self.max = None

    def add(self, value):
        self.sum += value
        self.count += 1

    def compute(self):
        if self.count is 0:
            return 0
        return float(self.sum) / float(self.count)


def validate_record(record):
    if 'timestamp' not in record:
        return False

    if not isinstance(record['timestamp'], float):
        return False

    if 'framework_id' not in record:
        return False

    if 'executor_id' not in record:
        return False

    if 'cpu_allocation_slack' not in record:
        return False

    if 'cpu_usage_slack' not in record:
        return False

    if 'cpu_usage' not in record:
        return False

    if 'mem_allocation_slack' not in record:
        return False

    if 'mem_usage_slack' not in record:
        return False

    if 'mem_usage' not in record:
        return False

    if 'timestamp' not in record:
        return False

    return True


# TODO(nnielsen): Introduce print_stats() method which prints counts, last stats etc. for logging
# TODO(nnielsen): Introduce stats() method which return recent metrics
class Monitor:
    def __init__(self, record_queue, bucket_size=5, sample_limits=120):
        self.record_queue = record_queue
        self.stats_lock = threading.Lock()

        # Bucket size is 60 seconds
        self.bucket_size = bucket_size

        # Keep samples for max 1hr
        self.sample_limits = sample_limits

        # TODO(nnielsen): This bucket is prone to clock skew i.e. a single slave can invalidate the
        # averages if their times are more than one minutes apart. Make a small (5 minutes, for
        # example) window to cope with this.
        self.cluster_start = 0
        self.cluster_current = []

        # 1 min averages
        self.cluster_current_index = 0
        self.cluster_avgs = []

        for i in range(self.sample_limits):
            self.cluster_avgs.append(None)

        self.thread = None

        # Cause monitor thread to eventually stop.
        self.stop_status = False
        self.stop_lock = threading.Lock()

        # Metrics
        self.metrics = {
            '/samples/count': 0,
            '/rollover/count': 0
        }

    def start(self):
        def run_task():
            iteration = 0

            while True:
                # Check for stop request every 10 iteration.
                iteration += 1
                if iteration > 2:
                    iteration = 0

                    # Get copy of stop
                    self.stop_lock.acquire()
                    status = self.stop_status
                    self.stop_lock.release()

                    if status is True:
                        print "Stopping monitor!"
                        return

                try:
                    records = self.record_queue.get(True, 1)
                except Queue.Empty:
                    continue

                for record in records:
                    if not validate_record(record):
                        print "Skipping sample: malformed %s" % json.dumps(record)
                        continue

                    ts = record['timestamp']
                    current_minute = int(ts / self.bucket_size)

                    if self.cluster_start == 0:

                        self.stats_lock.acquire()
                        self.cluster_start = current_minute
                        self.stats_lock.release()

                    elif self.cluster_start < current_minute:
                        # Clear current minute average and update average list
                        # Compute new aggregates
                        frameworks = {}
                        for sample in self.cluster_current:
                            framework_id = sample['framework_id']
                            executor_id = sample['executor_id']

                            if framework_id not in frameworks:
                                frameworks[framework_id] = {}

                            if executor_id not in frameworks[framework_id]:
                                frameworks[framework_id][executor_id] = []

                            frameworks[framework_id][executor_id].append(sample)

                        self.stats_lock.acquire()

                        cluster_cpu_allocation_slack = 0.0
                        cluster_cpu_usage_slack = 0.0
                        cluster_cpu_usage = 0.0
                        cluster_mem_allocation_slack = 0.0
                        cluster_mem_usage_slack = 0.0
                        cluster_mem_usage = 0.0

                        # TODO(nnielsen): Compute and store framework and executor aggregates.
                        for framework_id, framework in frameworks.iteritems():
                            # TODO(nnielsen): Wrap 'Average' or 'Aggregate' for set of metrics.
                            framework_cpu_allocation_slack = Average()
                            framework_cpu_usage_slack = Average()
                            framework_cpu_usage = Average()
                            framework_mem_allocation_slack = Average()
                            framework_mem_usage_slack = Average()
                            framework_mem_usage = Average()

                            for executor_id, executor in framework.iteritems():
                                for sample in executor:
                                    # Add samples to corresponding aggregates
                                    framework_cpu_allocation_slack.add(sample['cpu_allocation_slack'])
                                    framework_cpu_usage_slack.add(sample['cpu_usage_slack'])
                                    framework_cpu_usage.add(sample['cpu_usage'])
                                    framework_mem_allocation_slack.add(sample['mem_allocation_slack'])
                                    framework_mem_usage_slack.add(sample['mem_usage_slack'])
                                    framework_mem_usage.add(sample['mem_usage'])

                            cluster_cpu_allocation_slack += framework_cpu_allocation_slack.compute()
                            cluster_cpu_usage_slack += framework_cpu_usage_slack.compute()
                            cluster_cpu_usage += framework_cpu_usage.compute()
                            cluster_mem_allocation_slack += framework_mem_allocation_slack.compute()
                            cluster_mem_usage_slack += framework_mem_usage_slack.compute()
                            cluster_mem_usage += framework_mem_usage.compute()

                        self.cluster_avgs[self.cluster_current_index] = {
                            'cpu_allocation_slack': cluster_cpu_allocation_slack,
                            'cpu_usage_slack': cluster_cpu_usage_slack,
                            'cpu_usage':       cluster_cpu_usage,
                            'mem_allocation_slack': cluster_mem_allocation_slack,
                            'mem_usage_slack': cluster_mem_usage_slack,
                            'mem_usage':       cluster_mem_usage,
                            'timestamp':       self.cluster_start
                        }

                        self.cluster_start = current_minute

                        self.cluster_current_index += 1

                        # Roll over.
                        self.cluster_current_index %= self.sample_limits

                        self.stats_lock.release()

                        # Reset current 1s samples
                        self.cluster_current = []

                    elif self.cluster_start > current_minute:
                        # Skip sample, already rolled over.
                        print "Warning: skipping sample due to previous roll over"
                        continue

                    # Add to current bucket
                    self.cluster_current.append(record)

        self.thread = threading.Thread(target=run_task)
        self.thread.start()

    def join(self):
        self.thread.join()

    def stop(self):
        self.stop_lock.acquire()
        self.stop_status = True
        self.stop_lock.release()

    def cluster(self, minutes=1):
        samples = []
        self.stats_lock.acquire()

        # Make sure we don't exceed the sample size.
        limit = min(minutes, self.sample_limits)

        # self.cluster_current_index points to current sample (which is currently being built). We
        # need to subtract one to get previous (not in progress) aggregate.
        start = self.cluster_current_index - 1
        if start < 0:
            start = 0

        for i in range(start, start + limit):
            index = i % self.sample_limits
            sample = self.cluster_avgs[index]

            if sample is not None:
                samples.append(sample)

        self.stats_lock.release()

        return samples

