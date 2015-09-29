FROM ubuntu

RUN mkdir -p /usr/local/stellar
ADD . /usr/local/stellar

WORKDIR /usr/local/stellar

# Install dependencies
RUN apt-get -qy update
RUN apt-get -qy install python wget python-setuptools libsasl2-modules libsasl2-dev subversion libcurl4-openssl-dev python-pip

ADD http://downloads.mesosphere.io/master/ubuntu/14.04/mesos-0.23.0-py2.7-linux-x86_64.egg .
RUN easy_install mesos-0.23.0-py2.7-linux-x86_64.egg

RUN pip install Flask

# Run tests
RUN python test_monitor.py
RUN python test_scheduler.py

