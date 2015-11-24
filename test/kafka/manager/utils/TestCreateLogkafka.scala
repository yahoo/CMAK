/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager.utils

import java.util.Properties

import LogkafkaErrors._
import kafka.manager.ActorModel.{LogkafkaIdentity}
import kafka.manager.features.ClusterFeatures
import kafka.manager.{ClusterContext, ClusterConfig, Kafka_0_8_2_0}
import org.apache.zookeeper.data.Stat
import scala.concurrent.Future
/**
 * @author zheolong
 */
class TestCreateLogkafka extends CuratorAwareTest {

  import logkafka82.LogkafkaConfigErrors._ 
  
  private[this] val adminUtils  = new LogkafkaAdminUtils(Kafka_0_8_2_0)
  private[this] val defaultClusterConfig = ClusterConfig("test","0.8.2.0","localhost:2818",100,false,true)
  private[this] val defaultClusterContext = ClusterContext(ClusterFeatures.from(defaultClusterConfig), defaultClusterConfig)
  private[this] val createLogkafkaHostname = "km-unit-test-logkafka-hostname"
  private[this] val createLogkafkaLogPath = "/km-unit-test-logkafka-logpath"
  private[this] val createLogkafkaTopic = "km-unit-test-logkafka-topic"
  private[this] val createLogkafkaRegexFilterPattern = "km-unit-test-logkafka-regex-filter-pattern"
  private[this] val createLogkafkaInvalidHostnames = List("x.x..x.x", ".x.x.x.x", "x.x.x.x.")

  test("create logkafka with empty hostname") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    checkError[HostnameEmpty] {
      withCurator { curator =>
        adminUtils.createLogkafka(curator, "", createLogkafkaLogPath, config)
      }
    }
  }

  test("create logkafka with invalid hostname") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    withCurator { curator =>
      checkError[HostnameIsLocalhost] {
        adminUtils.createLogkafka(curator, "localhost", createLogkafkaLogPath, config)
      }
      createLogkafkaInvalidHostnames foreach { invalidHostname =>
        checkError[InvalidHostname] {
          adminUtils.createLogkafka(curator, invalidHostname, createLogkafkaLogPath, config)
        }
      }
    }
  }

  test("create logkafka with hostname too long") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    checkError[InvalidHostnameLength] {
      withCurator { curator =>
        adminUtils.createLogkafka(curator, "adfasfdsafsfasdfsadfasfsdfasffsdfsadfsdfsdfsfasdfdsfdsafasdfsfdsafasdfdsfdsafsdfdsafasdfsdafasdfadsfdsfsdafsdfsadfdsfasfdfasfsdafsdfdsfdsfasdfdsfsdfsadfsdfasdfdsafasdfsadfdfdsfdsfsfsfdsfdsfdssafsdfdsafadfasdfsdafsdfasdffasfdfadsfasdfasfadfafsdfasfdssafffffffffffdsadfsafdasdfsafsfsfsdfafs", createLogkafkaLogPath, config)
      }
    }
  }

  test("create logkafka with bad chars in hostname") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    checkError[IllegalCharacterInName] {
      withCurator { curator =>
        adminUtils.createLogkafka(curator, "bad!Hostname!", createLogkafkaLogPath, config)
      }
    }
  }

  test("create logkafka with empty log path") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    checkError[LogPathEmpty] {
      withCurator { curator =>
        adminUtils.createLogkafka(curator, createLogkafkaHostname, "", config)
      }
    }
  }

  test("create logkafka with invalid log path") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    withCurator { curator =>
      checkError[LogPathNotAbsolute] {
        adminUtils.createLogkafka(curator, createLogkafkaHostname, "a/b/c", config)
      }
    }
  }

  test("create logkafka with log path too long") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    checkError[InvalidLogPathLength] {
      withCurator { curator =>
        adminUtils.createLogkafka(curator, createLogkafkaHostname, "/adfasfdsafsfasdfsadfasfsdfasffsdfsadfsdfsdfsfasdfdsfdsafasdfsfdsafasdfdsfdsafsdfdsafasdfsdafasdfadsfdsfsdafsdfsadfdsfasfdfasfsdafsdfdsfdsfasdfdsfsdfsadfsdfasdfdsafasdfsadfdfdsfdsfsfsfdsfdsfdssafsdfdsafadfasdfsdafsdfasdffasfdfadsfasdfasfadfafsdfasfdssafffffffffffdsadfsafdasdfsafsfsfsdfafs", config)
      }
    }
  }

  test("create logkafka with bad chars in log path") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    checkError[IllegalCharacterInPath] {
      withCurator { curator =>
        adminUtils.createLogkafka(curator, createLogkafkaHostname, "/bad?Log Path*", config)
      }
    }
  }

  test("create logkafka with invalid regex filter pattern") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    config.put(kafka.manager.utils.logkafka82.LogConfig.RegexFilterPatternProp, "{")
    withCurator { curator =>
      checkError[InvalidRegexFilterPattern] {
        adminUtils.createLogkafka(curator, createLogkafkaHostname, createLogkafkaLogPath, config)
      }
    }
  }

  test("create logkafka with regex filter pattern too long") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    config.put(kafka.manager.utils.logkafka82.LogConfig.RegexFilterPatternProp, "adfasfdsafsfasdfsadfasfsdfasffsdfsadfsdfsdfsfasdfdsfdsafasdfsfdsafasdfdsfdsafsdfdsafasdfsdafasdfadsfdsfsdafsdfsadfdsfasfdfasfsdafsdfdsfdsfasdfdsfsdfsadfsdfasdfdsafasdfsadfdfdsfdsfsfsfdsfdsfdssafsdfdsafadfasdfsdafsdfasdffasfdfadsfasdfasfadfafsdfasfdssafffffffffffdsadfsafdasdfsafsfsfsdfafs")
    checkError[InvalidRegexFilterPatternLength] {
      withCurator { curator =>
        adminUtils.createLogkafka(curator, createLogkafkaHostname, createLogkafkaLogPath, config)
      }
    }
  }
}
