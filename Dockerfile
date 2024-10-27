FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y build-essential redis-server libprotobuf-dev \
        protobuf-compiler git

CMD ["/bin/bash"]
