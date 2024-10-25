FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y build-essential checkinstall wget zlib1g-dev \
        redis-server libprotobuf-dev protobuf-compiler

COPY . .

# Set default command to bash so the container doesn’t exit immediately
CMD ["/bin/bash"]
