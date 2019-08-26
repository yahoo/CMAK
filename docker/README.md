## How to use

### Build docker image

For docker users, you can build your own docker image

``` sh
docker build -t kafka-manager:2.0.0.2 .
```

The Dockerfile provides two parameters for customizing the **kafka-manager version** and **JVM options**:

``` sh
docker build --build-arg VERSION=2.0.0.1 --build-arg JAVA_OPTS="-XX:+PrintGCDetails -XX:+PrintGCApplicationStoppedTime" -t kafka-manager:2.0.0.1
```

### Run with docker

After building the docker image, you can use this image to run the container. 
**All environment variables** supported by kafka-manager can be set with the `-e` option:

``` sh
docker run -dt --name kafka-manager \
            -p 9000:9000 \
            -e ZK_HOSTS=172.16.0.110:2181,172.16.0.111:2181,172.16.0.112:2181 \
            kafka-manager:2.0.0.2
```

### Run with docker-compose

This is a docker-compose configuration example, docker compose will automatically read
the `.env` file in the same directory as an environment variable.

Directory Structure

``` sh
kafka-manager
├── docker-compose.yaml
└── .env
```

docker-compose.yaml

``` yaml
version: '3.7'
services:
  kafka-manager:
    image: kafka-manager:2.0.0.2
    restart: always
    ports:
      - 9000:9000
    environment:
      - ZK_HOSTS
```

.env

``` sh
ZK_HOSTS=172.16.0.110:2181,172.16.0.111:2181,172.16.0.112:2181
```

run with docker-compose

``` sh
cd kafka-manager
docker-compose up -d
```
