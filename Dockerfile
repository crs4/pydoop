FROM crs4/hadoop
MAINTAINER simone.leo@crs4.it

RUN yum install https://centos7.iuscommunity.org/ius-release.rpm

# needed only to run examples: zip, wheel
RUN yum install \
    gcc \
    gcc-c++ \
    python-devel \
    python-pip \
    python36u-devel \
    python36u-pip \
    zip
RUN ln -rs /usr/bin/python3.6 /usr/bin/python3 && \
    ln -rs /usr/bin/pip3.6 /usr/bin/pip3

ENV HADOOP_HOME /opt/hadoop
ENV LC_ALL en_US.UTF-8
ENV LANG en_US.UTF-8

COPY . /build/pydoop
WORKDIR /build/pydoop

RUN source /etc/profile && for v in 2 3; do \
      pip${v} install --upgrade pip && \
      pip${v} install --upgrade -r requirements.txt && \
      python${v} setup.py build && \
      python${v} setup.py install --skip-build; \
    done
