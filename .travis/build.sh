#!/usr/bin/env bash

[[ -z ${DOCKER_IMAGE_NAME} ]] && DOCKER_IMAGE_NAME=blobs-http-reverse-proxy

./mvnw --batch-mode install -B -V

cd agent/reverse-proxy && docker build -t ${DOCKER_IMAGE_NAME} -f Dockerfile . && cd ../..
