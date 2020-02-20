/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager.utils

import kafka.manager.utils.zero81.{PreferredLeaderElectionErrors, PreferredReplicaLeaderElectionCommand}
import org.apache.kafka.common.TopicPartition

/**
 * @author hiral
 */
class TestPreferredReplicaLeaderElection extends CuratorAwareTest {
  import PreferredLeaderElectionErrors._

  test("preferred replica leader election with empty set") {
    checkError[ElectionSetEmptyOnWrite] {
      withCurator { curator =>
        PreferredReplicaLeaderElectionCommand.writePreferredReplicaElectionData(curator,Set.empty)
      }
    }
  }

  test("preferred replica leader election") {
    withCurator { curator =>
      val set = Set(new TopicPartition("mytopic",1),new TopicPartition("mytopic",2),new TopicPartition("mytopic",3))
      PreferredReplicaLeaderElectionCommand.writePreferredReplicaElectionData(curator,set)
      val json: String = curator.getData.forPath(ZkUtils.PreferredReplicaLeaderElectionPath)
      assert(json == "{\"version\":1,\"partitions\":[{\"topic\":\"mytopic\",\"partition\":1},{\"topic\":\"mytopic\",\"partition\":2},{\"topic\":\"mytopic\",\"partition\":3}]}")
    }
  }

  test("preferred replica leader election already running") {
    checkError[ElectionAlreadyInProgress] {
      withCurator { curator =>
        val set = Set(new TopicPartition("mytopic", 1), new TopicPartition("mytopic", 2), new TopicPartition("mytopic", 3))
        PreferredReplicaLeaderElectionCommand.writePreferredReplicaElectionData(curator, set)
        val json: String = curator.getData.forPath(ZkUtils.PreferredReplicaLeaderElectionPath)
        assert(json == "{\"version\":1,\"partitions\":[{\"topic\":\"mytopic\",\"partition\":1},{\"topic\":\"mytopic\",\"partition\":2},{\"topic\":\"mytopic\",\"partition\":3}]}")
      }
    }
  }
}
