CMAK (Cluster Manager for Apache Kafka, previously known as Kafka Manager)
=============

CMAK (previously known as Kafka Manager) is a tool for managing [Apache Kafka](http://kafka.apache.org) clusters.
_See below for details about the name change._

CMAK supports the following:

 - Manage multiple clusters
 - Easy inspection of cluster state (topics, consumers, offsets, brokers, replica distribution, partition distribution)
 - Run preferred replica election
 - Generate partition assignments with option to select brokers to use
 - Run reassignment of partition (based on generated assignments)
 - Create a topic with optional topic configs (0.8.1.1 has different configs than 0.8.2+)
 - Delete topic (only supported on 0.8.2+ and remember set delete.topic.enable=true in broker config)
 - Topic list now indicates topics marked for deletion (only supported on 0.8.2+)
 - Batch generate partition assignments for multiple topics with option to select brokers to use
 - Batch run reassignment of partition for multiple topics
 - Add partitions to existing topic
 - Update config for existing topic
 - Optionally enable JMX polling for broker level and topic level metrics.
 - Optionally filter out consumers that do not have ids/ owners/ & offsets/ directories in zookeeper.

Cluster Management

![cluster](/img/cluster.png)

***

Topic List

![topic](/img/topic-list.png)

***

Topic View

![topic](/img/topic.png)

***

Consumer List View

![consumer](/img/consumer-list.png)

***

Consumed Topic View

![consumer](/img/consumed-topic.png)

***

Broker List

![broker](/img/broker-list.png)

***

Broker View

![broker](/img/broker.png)

***

Requirements
------------

1. [Kafka 0.8.*.* or 0.9.*.* or 0.10.*.* or 0.11.*.*](http://kafka.apache.org/downloads.html)
2. Java 8+

Configuration
-------------

The minimum configuration is the zookeeper hosts which are to be used for CMAK (pka kafka manager) state.
This can be found in the application.conf file in conf directory.  The same file will be packaged
in the distribution zip file; you may modify settings after unzipping the file on the desired server.

    kafka-manager.zkhosts="my.zookeeper.host.com:2181"

You can specify multiple zookeeper hosts by comma delimiting them, like so:

    kafka-manager.zkhosts="my.zookeeper.host.com:2181,other.zookeeper.host.com:2181"

Alternatively, use the environment variable `ZK_HOSTS` if you don't want to hardcode any values.

    ZK_HOSTS="my.zookeeper.host.com:2181"

You can optionally enable/disable the following functionality by modifying the default list in application.conf :

    application.features=["KMClusterManagerFeature","KMTopicManagerFeature","KMPreferredReplicaElectionFeature","KMReassignPartitionsFeature"]

 - KMClusterManagerFeature - allows adding, updating, deleting cluster from CMAK (pka Kafka Manager)
 - KMTopicManagerFeature - allows adding, updating, deleting topic from a Kafka cluster
 - KMPreferredReplicaElectionFeature - allows running of preferred replica election for a Kafka cluster
 - KMReassignPartitionsFeature - allows generating partition assignments and reassigning partitions

Consider setting these parameters for larger clusters with jmx enabled :

 - kafka-manager.broker-view-thread-pool-size=< 3 * number_of_brokers>
 - kafka-manager.broker-view-max-queue-size=< 3 * total # of partitions across all topics>
 - kafka-manager.broker-view-update-seconds=< kafka-manager.broker-view-max-queue-size / (10 * number_of_brokers) >

Here is an example for a kafka cluster with 10 brokers, 100 topics, with each topic having 10 partitions giving 1000 total partitions with JMX enabled :

 - kafka-manager.broker-view-thread-pool-size=30
 - kafka-manager.broker-view-max-queue-size=3000
 - kafka-manager.broker-view-update-seconds=30

The follow control consumer offset cache's thread pool and queue :

 - kafka-manager.offset-cache-thread-pool-size=< default is # of processors>
 - kafka-manager.offset-cache-max-queue-size=< default is 1000>
 - kafka-manager.kafka-admin-client-thread-pool-size=< default is # of processors>
 - kafka-manager.kafka-admin-client-max-queue-size=< default is 1000>

You should increase the above for large # of consumers with consumer polling enabled.  Though it mainly affects ZK based consumer polling.

Kafka managed consumer offset is now consumed by KafkaManagedOffsetCache from the "__consumer_offsets" topic.  Note, this has not been tested with large number of offsets being tracked.  There is a single thread per cluster consuming this topic so it may not be able to keep up on large # of offsets being pushed to the topic.

### Authenticating a User with LDAP
Warning, you need to have SSL configured with CMAK (pka Kafka Manager) to ensure your credentials aren't passed unencrypted.
Authenticating a User with LDAP is possible by passing the user credentials with the Authorization header.
LDAP authentication is done on first visit, if successful, a cookie is set.
On next request, the cookie value is compared with credentials from Authorization header.
LDAP support is through the basic authentication filter.

1. Configure basic authentication
- basicAuthentication.enabled=true
- basicAuthentication.realm=< basic authentication realm>

2. Encryption parameters (optional, otherwise randomly generated on startup) :
- basicAuthentication.salt="some-hex-string-representing-byte-array"
- basicAuthentication.iv="some-hex-string-representing-byte-array"
- basicAuthentication.secret="my-secret-string"

3. Configure LDAP/LDAPS authentication
- basicAuthentication.ldap.enabled=< Boolean flag to enable/disable ldap authentication >
- basicAuthentication.ldap.server=< fqdn of LDAP server>
- basicAuthentication.ldap.port=< port of LDAP server>
- basicAuthentication.ldap.username=< LDAP search username>
- basicAuthentication.ldap.password=< LDAP search password>
- basicAuthentication.ldap.search-base-dn=< LDAP search base>
- basicAuthentication.ldap.search-filter=< LDAP search filter>
- basicAuthentication.ldap.connection-pool-size=< number of connection to LDAP server>
- basicAuthentication.ldap.ssl=< Boolean flag to enable/disable LDAPS>

4. (Optional) Limit access to a specific LDAP Group
- basicAuthentication.ldap.group-filter=< LDAP group filter>

#### Example (Online LDAP Test Server):

- basicAuthentication.ldap.enabled=true
- basicAuthentication.ldap.server="ldap.forumsys.com"
- basicAuthentication.ldap.port=389
- basicAuthentication.ldap.username="cn=read-only-admin,dc=example,dc=com"
- basicAuthentication.ldap.password="password"
- basicAuthentication.ldap.search-base-dn="dc=example,dc=com"
- basicAuthentication.ldap.search-filter="(uid=$capturedLogin$)"
- basicAuthentication.ldap.group-filter="cn=allowed-group,ou=groups,dc=example,dc=com"
- basicAuthentication.ldap.connection-pool-size=10
- basicAuthentication.ldap.ssl=false


Deployment
----------

The command below will create a zip file which can be used to deploy the application.

    ./sbt clean dist

Please refer to play framework documentation on [production deployment/configuration](https://www.playframework.com/documentation/2.4.x/ProductionConfiguration).

If java is not in your path, or you need to build against a specific java version,
please use the following (the example assumes oracle java8):

    $ PATH=/usr/local/oracle-java-8/bin:$PATH \
      JAVA_HOME=/usr/local/oracle-java-8 \
      /path/to/sbt -java-home /usr/local/oracle-java-8 clean dist

This ensures that the 'java' and 'javac' binaries in your path are first looked up in the
oracle java8 release. Next, for all downstream tools that only listen to JAVA_HOME, it points
them to the oracle java8 location. Lastly, it tells sbt to use the oracle java8 location as
well.

Starting the service
--------------------

After extracting the produced zipfile, and changing the working directory to it, you can
run the service like this:

    $ bin/kafka-manager

By default, it will choose port 9000. This is overridable, as is the location of the
configuration file. For example:

    $ bin/kafka-manager -Dconfig.file=/path/to/application.conf -Dhttp.port=8080

Again, if java is not in your path, or you need to run against a different version of java,
add the -java-home option as follows:

    $ bin/kafka-manager -java-home /usr/local/oracle-java-8

Starting the service with Security
----------------------------------

To add JAAS configuration for SASL, add the config file location at start:

    $ bin/kafka-manager -Djava.security.auth.login.config=/path/to/my-jaas.conf

NOTE: Make sure the user running CMAK (pka kafka manager) has read permissions on the jaas config file


Packaging
---------

If you'd like to create a Debian or RPM package instead, you can run one of:

    sbt debian:packageBin

    sbt rpm:packageBin

Credits
-------

Logo/favicon used is from [Apache Kafka](http://kafka.apache.org) project and a registered trademark of the Apache Software Foundation.

Most of the utils code has been adapted to work with [Apache Curator](http://curator.apache.org) from [Apache Kafka](http://kafka.apache.org).

Name and Management
-------

CMAK was renamed from its previous name due to [this issue](https://github.com/yahoo/kafka-manager/issues/713). CMAK is designed to be used with Apache Kafka and is offered to support the needs of the Kafka community. This project is currently managed by employees at Verizon Media and the community who supports this project. 

License
-------

Licensed under the terms of the Apache License 2.0. See accompanying LICENSE file for terms.
