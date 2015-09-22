def validate_statistics_sample(sample):
    """

    :rtype : Boolean
    """
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