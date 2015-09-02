import threading


# TODO(nnielsen): Make this an 'Aggregate' instead (with count, min, max,
# etc).
class Average:
    def __init__(self):
        self.count = 0
        self.sum = 0.0

    def add(self, value):
        self.sum += value
        self.count += 1

    def compute(self):
        if self.count is 0:
            return 0
        return float(self.sum) / float(self.count)


# TODO(nnielsen): Introduce stats() method which prints counts, last stats etc. for logging
class Monitor:
    def __init__(self, record_queue):
        # TODO(nnielsen): Maintain circular buffer for samples.
        self.record_queue = record_queue
        self.stats_lock = threading.Lock()

        # Bucket size is 60 seconds
        self.bucket_size = 60

        # Keep samples for max 1hr
        self.sample_limits = 60

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

    def start(self):
        def run_task():
            while True:
                records = self.record_queue.get()

                for record in records:
                    ts = record['timestamp']
                    current_minute = int(ts / self.bucket_size)

                    if self.cluster_start < current_minute:
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

                        cluster_cpu_slack = Average()
                        cluster_cpu_usage = Average()
                        cluster_mem_slack = Average()
                        cluster_mem_usage = Average()

                        # TODO(nnielsen): Compute and store framework and executor aggregates.
                        for framework_id, framework in frameworks.iteritems():
                            for executor_id, executor in framework.iteritems():
                                for sample in executor:
                                    # Add samples to corresponding aggregates
                                    cluster_cpu_slack.add(sample['cpu_slack'])
                                    cluster_cpu_usage.add(sample['cpu_usage'])
                                    cluster_mem_slack.add(sample['mem_slack'])
                                    cluster_mem_usage.add(sample['mem_usage'])

                        # TODO(nnielsen): Due to the post processing, we end up with a zero sample
                        # (all metrics are zero).
                        self.cluster_avgs[self.cluster_current_index] = {
                            'cpu_slack': cluster_cpu_slack.compute(),
                            'cpu_usage': cluster_cpu_usage.compute(),
                            'mem_slack': cluster_mem_slack.compute(),
                            'mem_usage': cluster_mem_usage.compute(),
                            'timestamp': self.cluster_start
                        }

                        self.cluster_start = current_minute

                        self.cluster_current_index += 1

                        # Roll over.
                        self.cluster_current_index %= self.sample_limits

                        self.stats_lock.release()

                        # Reset current 1s samples
                        self.cluster_current = []

                    if self.cluster_start > current_minute:
                        # Skip sample, already rolled over.
                        print "Warning: skipping sample due to previous roll over"
                        continue

                    # Add to current bucket
                    self.cluster_current.append(record)

        self.thread = threading.Thread(target=run_task)
        self.thread.start()

    def join(self):
        self.thread.join()

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

