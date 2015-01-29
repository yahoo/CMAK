Kafka Manager
=============

A tool for managing [Apache Kafka](http://kafka.apache.org).

It supports the following :

 - Manage multiple clusters
 - Easy inspection of cluster state (topics, brokers, replica distribution, partition distribution)
 - Run preferred replica election
 - Generate partition assignments (based on current state of cluster)
 - Run reassignment of partition (based on generated assignments)

Cluster Management

![cluster](/img/cluster.png)

***

Topic View

![topic](/img/topic.png)

***

Broker View

![broker](/img/broker.png)

***

Requirements
------------

1. Kafka 0.8.1.1 or 0.8.2-beta
2. sbt 0.13.x
3. Java 7+

Configuration
-------------

The minimum configuration is the zookeeper hosts which are to be used for kafka manager state.
This can be found in the application.conf file in conf directory.  The same file will be packaged
in the distribution zip file; you may modify settings after unzipping the file on the desired server.

    kafka-manager.zkhosts="my.zookeeper.host.com:2181"


Deployment
----------

The command below will create a zip file which can be used to deploy the application.

    sbt clean dist

Please refer to play framework documentation on production deployment.

Credits
-------

Logo/favicon used is from [Apache Kafka](http://kafka.apache.org).

Most of the utils code has been adapted to work with [Apache Curator](http://curator.apache.org) from [Apache Kafka](http://kafka.apache.org).


License
-------

Apache Licensed. See accompanying LICENSE file.

