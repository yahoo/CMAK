import com.typesafe.sbt.SbtNativePackager._
import com.typesafe.sbt.SbtNativePackager.autoImport._
import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import com.typesafe.sbt.packager.docker.DockerPlugin

/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
name := """kafka-manager"""

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.5"

scalacOptions ++= Seq("-Xlint","-Xfatal-warnings","-deprecation","-feature","-language:implicitConversions","-language:postfixOps")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.7",
  "org.webjars" %% "webjars-play" % "2.3.0-2",
  "org.webjars" % "bootstrap" % "3.3.1",
  "org.webjars" % "jquery" % "2.1.1",
  "org.webjars" % "backbonejs" % "1.1.2-2",
  "org.webjars" % "underscorejs" % "1.7.0",
  "org.webjars" % "dustjs-linkedin" % "2.4.0-1",
  "org.apache.curator" % "curator-framework" % "2.7.0" exclude("log4j","log4j") force(),
  "org.apache.curator" % "curator-recipes" % "2.7.0" exclude("log4j","log4j") force(),
  "org.json4s" %% "json4s-jackson" % "3.2.11",
  "org.json4s" %% "json4s-scalaz" % "3.2.11",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.7",
  "com.adrianhurt" %% "play-bootstrap3" % "0.1.1",
  "org.apache.kafka" %% "kafka" % "0.8.2-beta" % "test",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.apache.curator" % "curator-test" % "2.7.0" % "test" force()
)

net.virtualvoid.sbt.graph.Plugin.graphSettings

LessKeys.compress in Assets := true

pipelineStages := Seq(digest, gzip)

includeFilter in (Assets, LessKeys.less) := "*.less"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

enablePlugins(JavaAppPackaging) 

enablePlugins(DockerPlugin)

packageName := packageName.value

version := version.value

dockerBaseImage := "zenphu/jzmq:latest"

dockerExposedPorts := Seq(9000, 9443)

dockerExposedVolumes := Seq("/opt/docker/logs")

dockerEntrypoint := Seq("./bin/kafka-manager")
