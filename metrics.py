"""
This module contains the logic to grab usage data, process it and send it back to the scheduler.
"""

# pylint: disable=no-member, unused-import

import json
import json_helper
import time

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native


def validate_statistics_sample(sample):
    """
    :param sample:
    :return: True if sample includes necessary fields. False otherwise.
    """
    status = True
    if 'framework_id' not in sample:
        print 'Framework ID not found in sample'
        status = False

    if 'executor_id' not in sample:
        print 'Executor ID not found in sample'
        status = False

    if 'statistics' not in sample:
        print 'statistics not found in sample'
        status = False

    if 'timestamp' not in sample['statistics']:
        print 'timestamp not found in sample'
        status = False

    if 'cpus_user_time_secs' not in sample['statistics']:
        print 'cpu user time not found in sample'
        status = False

    if 'cpus_system_time_secs' not in sample['statistics']:
        print 'cpu system time not found in sample'
        status = False

    return status


def validate_task_info(executor_driver, task):
    """
    Validates TaskInfo and returns task.data as a JSON Object.

    :param executor_driver:
    :param task:
    :return: Task.data
    """
    print "Validating task.data..."

    # Validate task.data
    try:
        if task.data is None:
            return fail(
                'Data field not set for task; cannot monitor slave', executor_driver, task)

        task_json = json.loads(task.data)

        if 'slave_location' not in task_json:
            return fail('slave_location not found in task json', executor_driver, task)
    except TypeError:
        return fail(
            'Data field could not be parsed for task; cannot monitor slave',
            executor_driver,
            task)

    return task_json


def fail(message, executor_driver, task):
    """
    Helper function to send failure status update to the framework.

    :param message: Message string to send to the framework.
    :param executor_driver: Executor driver.
    :param task: Failed task.
    :return: None
    """
    update = mesos_pb2.TaskStatus()
    update.task_id.value = task.task_id.value
    update.state = mesos_pb2.TASK_FAILED
    update.message = message
    executor_driver.sendStatusUpdate(update)


def running(executor_driver, task, json_out=None):
    """
    Helper function to send 'task running' status updates, optionally with json data
    hanging off it.

    :param executor_driver: Executor driver.
    :param json_out: JSON data to hang off status update.
    :return:
    """
    update = mesos_pb2.TaskStatus()
    update.task_id.value = task.task_id.value
    update.state = mesos_pb2.TASK_RUNNING

    if json_out is not None:
        update.data = json_out

    executor_driver.sendStatusUpdate(update)


def resolve_slave_id(slave_location):
    """
    Helper to look up slave id from slave endpoint.

    :param slave_location: Address of slave (for example, localhost:5051).
    :return: ID of slave.
    """
    state_endpoint = 'http://%s/state.json' % slave_location
    slave_state = json_helper.from_url(state_endpoint)
    slave_id = slave_state['id']
    print "Resolved slave id: %s" % slave_id

    return slave_id


def process_sample(prev, sample):
    """
    Returns a new Stellar sample from two samples of container statistics.

    :param prev: Previous container statistics (from /monitor/statistics)
    :param sample: Current container statistics (from /monitor/statistics)
    :return: Stellar sample.
    """
    framework_id = sample['framework_id']
    executor_id = sample['executor_id']

    interval = sample['statistics']['timestamp'] - \
        prev['statistics']['timestamp']

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
    return {
        'framework_id': framework_id,
        'executor_id': executor_id,
        'cpu_usage_slack': cpu_slack,
        'cpu_usage': cpu_usage,
        'mem_usage_slack': mem_slack,
        'mem_usage': mem_usage,
        'timestamp': sample['statistics']['timestamp']
    }


def run_task(executor_driver, task):
    """
    Entry for collector thread.

    :return: False on failure
    """
    print "Running task %s" % task.task_id.value
    running(executor_driver, task)

    slave_location = validate_task_info(executor_driver, task)['slave_location']

    slave_id = resolve_slave_id(slave_location)

    monitor_endpoint = 'http://%s/monitor/statistics.json' % slave_location

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

        # Compute slave global allocation slacks.
        metrics_snapshot = json_helper.from_url('http://%s/metrics/snapshot' % slave_location)

        cpus_allocation_slack = metrics_snapshot['slave/cpus_total'] - \
            metrics_snapshot['slave/cpus_used']

        mem_allocation_slack = metrics_snapshot['slave/mem_total'] - \
            metrics_snapshot['slave/mem_used']

        # TODO(nnielsen): If slave is unreachable after a certain number of retries,
        #                 send TASK_FAILED and abort.
        # Collect the latest resource usage statistics.
        # TODO(nnielsen): Make sample rate configurable.
        # TODO(nnielsen): Batch samples.
        # TODO(nnielsen): We can adjust sample rate based on time of previous request.
        for sample in json_helper.from_url(monitor_endpoint):
            print 'Collecting sample at \'%s\'' % monitor_endpoint
            if 'statistics' in sample and 'timestamp' not in sample['statistics']:
                sample['statistics']['timestamp'] = time.time()

            # Validate sample
            if not validate_statistics_sample(sample):
                print "Warning: partial sample %s" % sample
                continue

            framework_id = sample['framework_id']
            executor_id = sample['executor_id']

            # Initialize 2-level deep map of framework -> executor -> sample.
            if framework_id not in samples:
                samples[framework_id] = {}

            if executor_id not in samples[framework_id]:
                samples[framework_id][executor_id] = None

            if samples[framework_id][executor_id] is not None:
                # We need two samples to compute the cpu usage.
                stellar_sample = process_sample(samples[framework_id][executor_id], sample)

                # Add global metrics.
                stellar_sample['slave_id'] = slave_id
                stellar_sample['cpu_allocation_slack'] = cpus_allocation_slack
                stellar_sample['mem_allocation_slack'] = mem_allocation_slack

                stellar_samples.append(stellar_sample)

            # Save current sample for next sample processing.
            samples[framework_id][executor_id] = sample

        # Send samples if collected.
        if len(stellar_samples) > 0:
            running(executor_driver, task, json.dumps(stellar_samples))

        # One second sample rate.
        time.sleep(1)
