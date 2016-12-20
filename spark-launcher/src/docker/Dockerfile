FROM ubuntu:trusty

MAINTAINER Chris Caspanello <ccaspanello@gmail.com>

RUN apt-get -y update
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --force-yes software-properties-common python-software-properties
RUN apt-add-repository -y ppa:webupd8team/java
RUN apt-get -y update
RUN /bin/echo debconf shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
RUN DEBIAN_FRONTEND=noninteractive apt-get -y install oracle-java8-installer oracle-java8-set-default

RUN apt-get -y install curl
RUN curl -s http://d3kbcqa49mib13.cloudfront.net/spark-1.3.0-bin-hadoop2.4.tgz | tar -xz -C /usr/local/
RUN cd /usr/local && ln -s spark-1.3.0-bin-hadoop2.4 spark
#ADD sbin/start-master.sh /start-master.sh
#ADD scripts/start-worker /start-worker.sh
#ADD scripts/spark-shell.sh  /spark-shell.sh
#ADD scripts/spark-defaults.conf /spark-defaults.conf
#ADD scripts/remove_alias.sh /remove_alias.sh
ENV SPARK_HOME /usr/local/spark

CMD bash -c '/usr/local/spark/sbin/start-master.sh';'bash'