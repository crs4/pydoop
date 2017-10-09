FROM crs4/hadoop

# needed only to run examples: zip, wheel
RUN yum -y -q install \
    gcc \
    gcc-c++ \
    python-devel \
    python-pip \
    zip
RUN pip install --upgrade pip && pip install --upgrade \
    avro \
    setuptools \
    wheel

ENV HADOOP_HOME /opt/hadoop
ENV LC_ALL en_US.UTF-8
ENV LANG en_US.UTF-8

COPY . /build/pydoop
WORKDIR /build/pydoop

RUN source /etc/profile && \
    python setup.py build && \
    python setup.py install --skip-build
