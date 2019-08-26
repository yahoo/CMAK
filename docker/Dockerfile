FROM openjdk:8u222-jdk-slim AS builder

# allow users to modify kafka-manger version with the --build-arg option when building on their own
ARG VERSION=2.0.0.2

ENV VERSION ${VERSION}
ENV DOWNLOAD_URL https://github.com/yahoo/kafka-manager/archive/${VERSION}.tar.gz

RUN set -ex \
    && apt update \
    && apt install tar unzip curl fakeroot -y \
    && curl -sSL ${DOWNLOAD_URL} > km.tar.gz \
    && tar -zxf km.tar.gz \
    && (cd kafka-manager-${VERSION} \
    && ./sbt clean dist \
    && mv target/universal/kafka-manager-${VERSION}.zip /)

FROM openjdk:8u222-jdk-slim AS dist

LABEL maintainer="mritd <mritd@linux.com>"

ARG VERSION=2.0.0.2

# allow user-defined JVM options
ARG JAVA_OPTS="-XX:+PrintFlagsFinal"

ENV VERSION ${VERSION} 
ENV KAFKA_MANAGER_HOME /usr/local/kafka-manager
ENV PATH ${PATH}:${KAFKA_MANAGER_HOME}/bin

ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LANGUAGE en_US:en
ENV JAVA_OPTS ${JAVA_OPTS}

# use tini tools to solve problems like zombie processes
# when a user forgets the --init option with docker or runs this image in kubernetes, tini handles it automatically
ENV TINI_VERSION v0.18.0
ENV TINI_DOWNLOAD_URL https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-amd64

# some general JVM options
# use UnlockExperimentalVMOptions、UseCGroupMemoryLimitForHeap to support Docker CPU and memory limits 
# refs https://blogs.oracle.com/java-platform-group/java-se-support-for-docker-cpu-and-memory-limits
ENV JAVA_OPTS   -Djava.security.egd=file:/dev/./urandom \
                -XX:+UnlockExperimentalVMOptions \
                -XX:+UseCGroupMemoryLimitForHeap \
                ${JAVA_OPTS}

RUN set -ex \
    && apt update -y \
    && apt install tzdata locales curl -y \
    && apt upgrade -y \
    && curl -sSL ${TINI_DOWNLOAD_URL} > /usr/bin/tini \
    && chmod +x /usr/bin/tini \
    && locale-gen --purge en_US.UTF-8 zh_CN.UTF-8 \
    && localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 \
    && echo 'LANG="en_US.UTF-8"' > /etc/default/locale \
    && echo 'LANGUAGE="en_US:en"' >> /etc/default/locale \
    && apt remove curl -y \
    && apt autoremove -y \
    && apt autoclean -y \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /kafka-manager-${VERSION}.zip /kafka-manager-${VERSION}.zip

RUN set -ex \
    && apt update -y \
    && apt install unzip \
    && unzip -q kafka-manager-${VERSION}.zip \
    && mv kafka-manager-${VERSION} ${KAFKA_MANAGER_HOME} \
    && apt remove unzip -y \
    && apt autoremove -y \
    && apt autoclean -y \
    && rm -f /kafka-manager-${VERSION}.zip

WORKDIR ${KAFKA_MANAGER_HOME}

EXPOSE 9000

ENTRYPOINT ["tini","--"]

CMD ["./bin/kafka-manager"]
