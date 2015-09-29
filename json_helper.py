"""
This module contains helpers to grab and parse JSON from Mesos agents.
"""

import json
import urllib
import time


def from_url(url):
    """
    Function loads data from url and parse JSON.

    Function blocks and retries if data cannot be loaded.

    :param url: Url to load
    :return: JSON object
    """
    while True:
        try:
            response = urllib.urlopen(url)
            data = response.read()
            return json.loads(data)
        except IOError:
            print "Could not load %s: retrying in one second" % url
            time.sleep(1)
            continue
