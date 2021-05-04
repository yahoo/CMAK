/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
package kafka.manager.utils

import kafka.manager.BaseTest
import kafka.manager.features.ClusterFeatures
import kafka.manager.model.{ClusterConfig, ClusterContext, Kafka_0_8_2_0}
import kafka.manager.utils.LogkafkaErrors._

import java.util.Properties
/**
 * @author zheolong
 */
class TestCreateLogkafka extends CuratorAwareTest with BaseTest {

  import logkafka82.LogkafkaConfigErrors._ 
  
  private[this] val adminUtils  = new LogkafkaAdminUtils(Kafka_0_8_2_0)
  private[this] val defaultClusterConfig = ClusterConfig("test","0.8.2.0","localhost:2818",100,false, pollConsumers = true, filterConsumers = true, jmxUser = None, jmxPass = None, jmxSsl = false, tuning = Option(defaultTuning), securityProtocol = "PLAINTEXT", saslMechanism = None, jaasConfig = None)
  private[this] val defaultClusterContext = ClusterContext(ClusterFeatures.from(defaultClusterConfig), defaultClusterConfig)
  private[this] val createLogkafkaLogkafkaId = "km-unit-test-logkafka-logkafka_id"
  private[this] val createLogkafkaLogPath = "/km-unit-test-logkafka-logpath"
  private[this] val createLogkafkaTopic = "km-unit-test-logkafka-topic"
  private[this] val createLogkafkaRegexFilterPattern = "km-unit-test-logkafka-regex-filter-pattern"
  private[this] val createLogkafkaInvalidLogkafkaIds = List(".", "..")
  private[this] val createLogkafkaInvalidLineDelimiters = List("-1", "256")

  test("create logkafka with empty logkafka id") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    checkError[LogkafkaIdEmpty] {
      withCurator { curator =>
        adminUtils.createLogkafka(curator, "", createLogkafkaLogPath, config)
      }
    }
  }

  test("create logkafka with invalid logkafka id") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    withCurator { curator =>
      createLogkafkaInvalidLogkafkaIds foreach { invalidLogkafkaId =>
        checkError[InvalidLogkafkaId] {
          adminUtils.createLogkafka(curator, invalidLogkafkaId, createLogkafkaLogPath, config)
        }
      }
    }
  }

  test("create logkafka with logkafka id too long") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    checkError[InvalidLogkafkaIdLength] {
      withCurator { curator =>
        adminUtils.createLogkafka(curator, "adfasfdsafsfasdfsadfasfsdfasffsdfsadfsdfsdfsfasdfdsfdsafasdfsfdsafasdfdsfdsafsdfdsafasdfsdafasdfadsfdsfsdafsdfsadfdsfasfdfasfsdafsdfdsfdsfasdfdsfsdfsadfsdfasdfdsafasdfsadfdfdsfdsfsfsfdsfdsfdssafsdfdsafadfasdfsdafsdfasdffasfdfadsfasdfasfadfafsdfasfdssafffffffffffdsadfsafdasdfsafsfsfsdfafs", createLogkafkaLogPath, config)
      }
    }
  }

  test("create logkafka with bad chars in logkafka id") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    checkError[IllegalCharacterInLogkafkaId] {
      withCurator { curator =>
        adminUtils.createLogkafka(curator, "bad!LogkafkaId!", createLogkafkaLogPath, config)
      }
    }
  }

  test("create logkafka with empty log path") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    checkError[LogPathEmpty] {
      withCurator { curator =>
        adminUtils.createLogkafka(curator, createLogkafkaLogkafkaId, "", config)
      }
    }
  }

  test("create logkafka with invalid log path") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    withCurator { curator =>
      checkError[LogPathNotAbsolute] {
        adminUtils.createLogkafka(curator, createLogkafkaLogkafkaId, "a/b/c", config)
      }
    }
  }

  test("create logkafka with log path too long") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    checkError[InvalidLogPathLength] {
      withCurator { curator =>
        adminUtils.createLogkafka(curator, createLogkafkaLogkafkaId, "/adfasfdsafsfasdfsadfasfsdfasffsdfsadfsdfsdfsfasdfdsfdsafasdfsfdsafasdfdsfdsafsdfdsafasdfsdafasdfadsfdsfsdafsdfsadfdsfasfdfasfsdafsdfdsfdsfasdfdsfsdfsadfsdfasdfdsafasdfsadfdfdsfdsfsfsfdsfdsfdssafsdfdsafadfasdfsdafsdfasdffasfdfadsfasdfasfadfafsdfasfdssafffffffffffdsadfsafdasdfsafsfsfsdfafs", config)
      }
    }
  }

  test("create logkafka with bad chars in log path") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    checkError[IllegalCharacterInPath] {
      withCurator { curator =>
        adminUtils.createLogkafka(curator, createLogkafkaLogkafkaId, "/bad?Log Path*", config)
      }
    }
  }

  test("create logkafka with invalid regex filter pattern") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    config.put(kafka.manager.utils.logkafka82.LogConfig.RegexFilterPatternProp, "{")
    withCurator { curator =>
      checkError[InvalidRegexFilterPattern] {
        adminUtils.createLogkafka(curator, createLogkafkaLogkafkaId, createLogkafkaLogPath, config)
      }
    }
  }

  test("create logkafka with regex filter pattern too long") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    config.put(kafka.manager.utils.logkafka82.LogConfig.RegexFilterPatternProp, "adfasfdsafsfasdfsadfasfsdfasffsdfsadfsdfsdfsfasdfdsfdsafasdfsfdsafasdfdsfdsafsdfdsafasdfsdafasdfadsfdsfsdafsdfsadfdsfasfdfasfsdafsdfdsfdsfasdfdsfsdfsadfsdfasdfdsafasdfsadfdfdsfdsfsfsfdsfdsfdssafsdfdsafadfasdfsdafsdfasdffasfdfadsfasdfasfadfafsdfasfdssafffffffffffdsadfsafdasdfsafsfsfsdfafs")
    checkError[InvalidRegexFilterPatternLength] {
      withCurator { curator =>
        adminUtils.createLogkafka(curator, createLogkafkaLogkafkaId, createLogkafkaLogPath, config)
      }
    }
  }

  test("create logkafka with invalid line delimiter") {
    val config = new Properties()
    config.put(kafka.manager.utils.logkafka82.LogConfig.TopicProp, createLogkafkaTopic)
    withCurator { curator =>
      createLogkafkaInvalidLineDelimiters foreach { invalidLineDelimiter =>
        config.put(kafka.manager.utils.logkafka82.LogConfig.LineDelimiterProp, invalidLineDelimiter)
        checkError[InvalidLineDelimiter] {
          adminUtils.createLogkafka(curator, createLogkafkaLogkafkaId, createLogkafkaLogPath, config)
        }
      }
    }
  }
}
