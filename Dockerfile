FROM ubuntu:16.04

ENV version=1.3.0.8 \
    src_dir=/usr/local/src/kafka-manager

RUN apt-get update && \
    apt-get install -y openjdk-8-jdk curl unzip && \
    apt-get clean

ADD . /usr/local/src/kafka-manager
WORKDIR /usr/local/src/kafka-manager

# build
RUN mkdir -p ~/.sbt/0.13 && \
    mv .docker.sbt ~/.sbt/0.13/local.sbt && \
    ./sbt clean dist

# install
RUN unzip $src_dir/target/universal/kafka-manager-$version.zip -d /usr/local && \
    rm -rfv $src_dir
WORKDIR /usr/local/kafka-manager-$version