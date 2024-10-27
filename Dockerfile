FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y build-essential redis-server libprotobuf-dev \
        protobuf-compiler

COPY . /usr/src/app
WORKDIR /usr/src/app

# Set default command to bash so the container doesn’t exit immediately
CMD ["/bin/bash"]
