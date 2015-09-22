import json
import json_helper
import sys
import threading
import time

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native
import metrics


# TODO(nnielsen): Factor out data processing into helper class for testing.
class StellarExecutor(mesos.interface.Executor):
    def launchTask(self, driver, task):
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

            print "Validating task.data..."

            # Validate task.data
            try:
                if task.data is None:
                    return fail('Data field not set for task; cannot monitor slave')
             
                task_json = json.loads(task.data)

                if 'slave_location' not in task_json:
                    return fail('slave_location not found in task json')
            except:
                return fail('Data field could not be parsed for task; cannot monitor slave')

            print "Task.data validated"

            print task_json

            slave_location = task_json['slave_location']
            monitor_endpoint = 'http://%s/monitor/statistics.json' % slave_location
            state_endpoint = 'http://%s/state.json' % slave_location
            metrics_endpoint = 'http://%s/metrics/snapshot' % slave_location

            slave_state = json_helper.from_url(state_endpoint)
            slave_id = slave_state['id']

            print "Resolved slave id: %s" % slave_id

            # One second sample rate.
            sample_rate = 1

            samples = {}
            sample_count = 0

            print "Start sample loop..."

            # Sample loop.
            while True:
                # Poor mans GC: We loose one sample per framework every 10.000 iterations.
                sample_count += 1
                if sample_count > 10000 == 0:
                    print "Cleaning samples..."
                    sample_count = 0
                    samples = {}

                stellar_samples = []

                print "Collecting sample for %s" % monitor_endpoint

                # Compute slave global allocation slacks.
                metrics_snapshot = json_helper.from_url(metrics_endpoint)
                cpus_total = metrics_snapshot['slave/cpus_total']
                cpus_used = metrics_snapshot['slave/cpus_used']
                cpus_allocation_slack = cpus_total - cpus_used

                mem_total = metrics_snapshot['slave/mem_total']
                mem_used = metrics_snapshot['slave/mem_used']
                mem_allocation_slack = mem_total - mem_used

                # TODO(nnielsen): If slave is unreachable after a certain number of retries, send TASK_FAILED and abort.
                # Collect the latest resource usage statistics.
                # TODO(nnielsen): Make sample rate configurable.
                # TODO(nnielsen): Batch samples.
                # TODO(nnielsen): We can adjust sample rate based on time of previous request.
                for sample in json_helper.from_url(monitor_endpoint):
                    print 'Collecting sample at \'%s\'' % monitor_endpoint
                    if 'statistics' in sample and 'timestamp' not in sample['statistics']:
                        sample['statistics']['timestamp'] = time.time()

                    # Validate sample
                    if not metrics.validate_statistics_sample(sample):
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

                        user_time = sample['statistics']['cpus_user_time_secs'] - \
                            prev['statistics']['cpus_user_time_secs']

                        system_time = sample['statistics']['cpus_system_time_secs'] - \
                            prev['statistics']['cpus_system_time_secs']

                        cpu_usage = float(user_time + system_time) / float(interval)

                        # Compute slack CPU.
                        cpu_slack = sample['statistics']['cpus_limit'] - cpu_usage

                        # Compute slack memory.
                        mem_usage = sample['statistics']['mem_rss_bytes']
                        mem_slack = sample['statistics']['mem_limit_bytes'] - mem_usage

                        # TODO(nnielsen): Hang off task id's for this executor.
                        stellar_samples.append({
                            'slave_id': slave_id,
                            'framework_id': framework_id,
                            'executor_id': executor_id,
                            'cpu_usage_slack': cpu_slack,
                            'cpu_usage': cpu_usage,
                            'cpu_allocation_slack': cpus_allocation_slack,
                            'mem_usage_slack': mem_slack,
                            'mem_usage': mem_usage,
                            'mem_allocation_slack': mem_allocation_slack,
                            'timestamp': sample['statistics']['timestamp']
                        })

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
    driver = mesos.native.MesosExecutorDriver(StellarExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
