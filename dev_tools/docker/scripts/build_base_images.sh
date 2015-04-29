#!/bin/bash

current_path=$(cd $(dirname ${BASH_SOURCE}); pwd; cd - >/dev/null)
images_path="${current_path}/../images"

echo "Building crs4_pydoop/base image (path: ${images_path}/base)"
docker build -t crs4_pydoop/base	${images_path}/base

echo "Building crs4_pydoop/client image (path: ${images_path}/client)"
docker build -t crs4_pydoop/client ${images_path}/client
