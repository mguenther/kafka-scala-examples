FROM phusion/baseimage:latest

MAINTAINER Markus Günther <markus.guenther@gmail.com>

RUN apt-get update && \
    apt-get install -y zookeeper supervisor dnsutils && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean

ADD supervisord.conf /etc/supervisor/conf.d/

EXPOSE 2181 2888 3888

CMD ["/sbin/my_init", "supervisord"]