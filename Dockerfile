FROM ubuntu:22.04

# Default build threads to 1; max is defined in Docker config (run `nproc` in Docker container)
ARG BUILD_THREADS
ENV THREADS_ARG=${BUILD_THREADS:-1}

RUN apt-get update && \
    apt-get install -y build-essential checkinstall wget zlib1g-dev \
        redis-server libprotobuf-dev protobuf-compiler

COPY . .

# Set default command to bash so the container doesn’t exit immediately
CMD ["/bin/bash"]
