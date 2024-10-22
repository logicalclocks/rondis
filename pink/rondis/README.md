# Rondis

## Running unit tests via Docker

In case you are developing inside a Docker container and you wish to run unit tests against a RonDB cluster, you can choose between

1. RonDB running as part of MTR tests
2. RonDB running as a separate Docker Compose cluster

In case you decide for the latter, you can spin up a cluster using [rondb-docker](https://github.com/logicalclocks/rondb-docker). Make sure when running it to have at least one open API slot with no Hostname specified. When a Docker Compose cluster is running, the container you are running from has to be added to the network of this cluster. Do so by running the following commands:

```bash
docker network list  # Search for the network name of the Docker Compose cluster
docker network connect <cluster network> <development container id>
```

Once this is done, the Rondis server can be started up using:
```bash
./pink/rondis/rondis 1186 mgmd_1
```

whereby, `mgmd_1` is the container name of the first Management server.
