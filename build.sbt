/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
name := """kafka-manager"""

/* For packaging purposes, -SNAPSHOT MUST contain a digit */
version := "1.3.3.14"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-Xlint:-missing-interpolator","-Xfatal-warnings","-deprecation","-feature","-language:implicitConversions","-language:postfixOps","-Xmax-classfile-name","240")

// From https://www.playframework.com/documentation/2.3.x/ProductionDist
assemblyMergeStrategy in assembly := {
  case "logger.xml" => MergeStrategy.first
  case "play/core/server/ServerWithStop.class" => MergeStrategy.first
  case other => (assemblyMergeStrategy in assembly).value(other)
}

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.14",
  "com.typesafe.akka" %% "akka-slf4j" % "2.3.14",
  "com.google.code.findbugs" % "jsr305" % "2.0.1",
  "org.webjars" %% "webjars-play" % "2.4.0-2",
  "org.webjars" % "bootstrap" % "3.3.5",
  "org.webjars" % "jquery" % "2.1.4",
  "org.webjars" % "backbonejs" % "1.2.3",
  "org.webjars" % "underscorejs" % "1.8.3",
  "org.webjars" % "dustjs-linkedin" % "2.6.1-1",
  "org.apache.curator" % "curator-framework" % "2.10.0" exclude("log4j","log4j") exclude("org.slf4j", "slf4j-log4j12") force(),
  "org.apache.curator" % "curator-recipes" % "2.10.0" exclude("log4j","log4j") exclude("org.slf4j", "slf4j-log4j12") force(),
  "org.json4s" %% "json4s-jackson" % "3.4.0",
  "org.json4s" %% "json4s-scalaz" % "3.4.0",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.12",
  "com.adrianhurt" %% "play-bootstrap3" % "0.4.5-P24",
  "org.clapper" %% "grizzled-slf4j" % "1.0.2",
  "org.apache.kafka" %% "kafka" % "0.10.0.1" exclude("log4j","log4j") exclude("org.slf4j", "slf4j-log4j12") force(),
  "com.beachape" %% "enumeratum" % "1.4.4",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.apache.curator" % "curator-test" % "2.10.0" % "test",
  "org.mockito" % "mockito-core" % "1.10.19" % "test",
  "com.yammer.metrics" % "metrics-core" % "2.2.0" force()
)

routesGenerator := InjectedRoutesGenerator

LessKeys.compress in Assets := true

pipelineStages := Seq(digest, gzip)

includeFilter in (Assets, LessKeys.less) := "*.less"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

coverageExcludedPackages := "<empty>;controllers.*;views.*;models.*"

/*
 * Allow packaging as part of the build
 */
enablePlugins(SbtNativePackager)

/* Debian Settings - to create, run as:
   $ sbt debian:packageBin

   See here for details:
   http://www.scala-sbt.org/sbt-native-packager/formats/debian.html
*/

maintainer := "Yahoo <yahoo@example.com>"
packageSummary := "A tool for managing Apache Kafka"
packageDescription := "A tool for managing Apache Kafka"

/* End Debian Settings */


/* RPM Settings - to create, run as:
   $ sbt rpm:packageBin

   See here for details:
   http://www.scala-sbt.org/sbt-native-packager/formats/rpm.html
*/

rpmRelease := "1"
rpmVendor := "yahoo"
rpmUrl := Some("https://github.com/yahoo/kafka-manager")
rpmLicense := Some("Apache")
rpmGroup := Some("kafka-manager")

/* End RPM Settings */
