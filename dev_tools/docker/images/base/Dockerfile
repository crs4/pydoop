#----------------------------------------------------
#
# A basic java machine with java, basic services and iv6 disabled
#----------------------------------------------------
FROM debian:latest

#----------------------------------------------------
# Install java and basic services
RUN echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" | tee /etc/apt/sources.list.d/webupd8team-java.list && \
    echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu trusty main" | tee -a /etc/apt/sources.list.d/webupd8team-java.list && \
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys EEA14886 && \
    apt-get update && \
    echo yes | apt-get install -y --force-yes oracle-java8-installer && \
    apt-get install -y \
    apt-utils \
    openssh-server \
    python \
    python-pip \
    wget

ENV JAVA_HOME /usr/lib/jvm/java-8-oracle
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> /etc/profile.d/java.sh

#----------------------------------------------------
# disable ipv6
RUN echo "net.ipv6.conf.all.disable_ipv6=1"     >> /etc/sysctl.conf && \
    echo "net.ipv6.conf.default.disable_ipv6=1" >> /etc/sysctl.conf && \
    echo "net.ipv6.conf.lo.disable_ipv6=1"      >>  /etc/sysctl.conf

#----------------------------------------------------
# add default unprivileged user (Alfred E. Neuman, "What? Me worry?")
ENV UNPRIV_USER aen
RUN useradd -m ${UNPRIV_USER} -s /bin/bash && \
    echo "${UNPRIV_USER}:hadoop" | chpasswd
    
RUN mkdir -p /root/.ssh && \
    ssh-keygen -t dsa -P '' -f /root/.ssh/id_dsa && \
    cat /root/.ssh/id_dsa.pub >> /root/.ssh/authorized_keys