ARG hadoop_version=3.2.0
ARG python_version=3.6

FROM crs4/pydoop-base:${hadoop_version}-${python_version}

COPY . /build/pydoop
WORKDIR /build/pydoop

RUN ${PYTHON} -m pip install --no-cache-dir --upgrade -r requirements.txt \
    && ${PYTHON} setup.py sdist \
    && ${PYTHON} -m pip install --pre dist/pydoop-$(cat VERSION).tar.gz
