The `env` folder is for all files that relate to setting up a development or CI environment.

# Docker tips.

```
# Build a Docker image named pgnp from our Dockerfile.
docker build --tag pgnp --file ./cmudb/env/Dockerfile .

# Run the Docker image in the pgnp_instance container.
docker run --detach --interactive --tty --name pgnp_instance pgnp

# Connect to the Docker container pgnp_instance.
docker exec --interactive --tty pgnp_instance /bin/bash

# Shut down the container AND DELETE ALL THE DATA ON IT.
docker container rm --force --volumes pgnp_instance
```