import json
import urllib

def from_url(url):
    while True:
        try:
            response = urllib.urlopen(url)
            data = response.read()
            return json.loads(data)
        except IOError:
            print "Could not load %s: retrying in one second" % url
            time.sleep(1)
            continue

