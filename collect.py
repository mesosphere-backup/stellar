import argparse
import json
import requests
import sys
import threading
import time

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

class StellarExecutor(mesos.interface.Executor):
    def launchTask(self, driver, task):
        print "launchTask(%s)" % task

        def run_task():
            print "Running task %s" % task.task_id.value
            update = mesos_pb2.TaskStatus()
            update.task_id.value = task.task_id.value
            update.state = mesos_pb2.TASK_RUNNING
            driver.sendStatusUpdate(update)

            def fail(message):
                update = mesos_pb2.TaskStatus()
                update.task_id.value = task.task_id.value
                update.state = mesos_pb2.TASK_FAILED
                update.message = message
                driver.sendStatusUpdate(update)

            # Validate task.data
            task_json = None
            try:
                if task.data is None:
                    return fail('Data field not set for task; cannot monitor slave')
             
                task_json = json.loads(task.data)

                if 'slave_location' not in task_json:
                    return fail('slave_location not found in task json')

                if 'monitor_path' not in task_json:
                    return fail('monitor_path not found in task json')
            except:
                return fail('Data field could not be parsed for task; cannot monitor slave')

            slave_location = task_json['slave_location']
            monitor_path = task_json['monitor_path']
            monitor_endpoint = 'http://%s/%s' % (slave_location, args.slave_monitor)

            # One second sample rate.
            sample_rate = 1

            samples = {}
            sample_count = 0

            # Sample loop.
            while True:
                # Poor mans GC: We loose one sample per framework every 10.000 iterations.
                sample_count += 1
                if sample_count > 10000 == 0:
                    print "Cleaning samples..."
                    sample_count = 0
                    samples = {}

                stellar_samples = []

                # TODO(nnielsen): If slave is unreachable after a certain number of retries, send TASK_FAILED and abort.
                # Collect the latest resource usage statistics.
                for sample in json_from_url(monitor_endpoint):
                    print 'Collecting sample at \'%s\'' % monitor_endpoint
                    if 'statistics' in sample and 'timestamp' not in sample['statistics']:
                        sample['statistics']['timestamp'] = time.time()

                    # Validate sample
                    if validate_statistics_sample(sample) == False:
                        print "Warning: partial sample %s" % sample
                        continue

                    framework_id = sample['framework_id']
                    executor_id = sample['executor_id']

                    if framework_id not in samples:
                        samples[framework_id] = {}

                    if executor_id not in samples[framework_id]:
                        samples[framework_id][executor_id] = None

                    if samples[framework_id][executor_id] is not None:
                        # We need two samples to compute the cpu usage.
                        prev = samples[framework_id][executor_id]

                        interval = sample['statistics']['timestamp'] - prev['statistics']['timestamp']

                        user_time = sample['statistics']['cpus_user_time_secs'] - prev['statistics']['cpus_user_time_secs']
                        system_time = sample['statistics']['cpus_system_time_secs'] - prev['statistics'][
                            'cpus_system_time_secs']
                        cpu_usage = (user_time + system_time) / interval

                        # Compute slack CPU.
                        cpu_slack = sample['statistics']['cpus_limit'] - cpu_usage

                        # Compute slack memory.
                        mem_slack = sample['statistics']['mem_limit_bytes'] - sample['statistics']['mem_rss_bytes']

                        stellar_samples.append({'cpu_slack': cpu_slack, 'mem_slack': mem_slack })

                    samples[framework_id][executor_id] = sample

                # Send samples if collected.
                if stellar_samples is not '':
                    json_out = json.dumps(stellar_samples)
                    update = mesos_pb2.TaskStatus()
                    update.task_id.value = task.task_id.value
                    update.state = mesos_pb2.TASK_RUNNING
                    update.data = json_out
                    driver.sendStatusUpdate(update)

                time.sleep(sample_rate)

        thread = threading.Thread(target=run_task)
        thread.start()

if __name__ == "__main__":
    time.sleep(1)
    print "Starting Stellar executor"
    sys.exit(0)
    # driver = mesos.native.MesosExecutorDriver(StellarExecutor())
    # sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)

def validate_statistics_sample(sample):
    if 'framework_id' not in sample:
        print 'Framework ID not found in sample'
        return False

    if 'executor_id' not in sample:
        print 'Executor ID not found in sample'
        return False

    if 'statistics' not in sample:
        print 'statistics not found in sample'
        return False

    if 'timestamp' not in sample['statistics']:
        print 'timestamp not found in sample'
        return False

    if 'cpus_user_time_secs' not in sample['statistics']:
        print 'cpu user time not found in sample'
        return False

    if 'cpus_system_time_secs' not in sample['statistics']:
        print 'cpu system time not found in sample'
        return False

    return True


def json_from_url(url):
    # TODO(nnielsen): Introduce exp backoff and limits.
    # TODO(nnielsen): Throw exception when limit is reached.
    while True:
        try:
            response = urllib.urlopen(url)
            data = response.read()
            return json.loads(data)
        except IOError:
            print "Could not load %s: retrying in one second" % url
            time.sleep(1)
            continue
