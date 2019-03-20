ARG hadoop_version=3.2.0
ARG python_version=3.6

FROM crs4/pydoop-client-base:${hadoop_version}-${python_version}

COPY . /build/pydoop
WORKDIR /build/pydoop

RUN ${PYTHON} -m pip install --no-cache-dir --upgrade -r requirements.txt \
    && ${PYTHON} setup.py build \
    && ${PYTHON} setup.py install --skip-build \
    && ${PYTHON} setup.py clean
