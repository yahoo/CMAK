FROM alpine:3.4

RUN apk update && \
    apk add bash openssl wget openjdk8 nodejs openjdk8-jre

COPY . /tmp/kafka-manager

RUN cd /tmp/kafka-manager && \
    ./sbt clean dist && \
    unzip /tmp/kafka-manager/target/universal/kafka-manager-1.3.2.1.zip -d /tmp && \
    mv /tmp/kafka-manager-1.3.2.1 /app

WORKDIR /app

EXPOSE 9000

ENTRYPOINT ["/app/bin/kafka-manager"]
