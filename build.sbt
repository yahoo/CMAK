/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
name := """cmak"""

/* For packaging purposes, -SNAPSHOT MUST contain a digit */
version := "3.0.0.4"

scalaVersion := "2.12.10"

scalacOptions ++= Seq("-Xlint:-missing-interpolator","-Xfatal-warnings","-deprecation","-feature","-language:implicitConversions","-language:postfixOps","-Xmax-classfile-name","240")

// From https://www.playframework.com/documentation/2.3.x/ProductionDist
assemblyMergeStrategy in assembly := {
  case "play/reference-overrides.conf" => MergeStrategy.first
  case "logger.xml" => MergeStrategy.first
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first
  case "module-info.class" => MergeStrategy.first
  case "play/core/server/ServerWithStop.class" => MergeStrategy.first
  case other => (assemblyMergeStrategy in assembly).value(other)
}

val akkaVersion = "2.6.2"
val curatorVersion = "4.2.0"
val json4sVersion = "3.6.7"
val kafkaVersion = "2.4.0"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.google.code.findbugs" % "jsr305" % "3.0.2",
  "org.webjars" %% "webjars-play" % "2.8.0",
  "org.webjars" % "bootstrap" % "4.4.1",
  "org.webjars" % "jquery" % "3.4.0",
  "org.webjars" % "backbonejs" % "1.3.3",
  "org.webjars" % "underscorejs" % "1.9.0",
  "org.webjars" % "dustjs-linkedin" % "2.7.2",
  "org.webjars" % "octicons" % "4.3.0",
  "org.apache.curator" % "curator-framework" % curatorVersion exclude("log4j","log4j") exclude("org.slf4j", "slf4j-log4j12") force(),
  "org.apache.curator" % "curator-recipes" % curatorVersion exclude("log4j","log4j") exclude("org.slf4j", "slf4j-log4j12") force(),
  "org.json4s" %% "json4s-jackson" % json4sVersion,
  "org.json4s" %% "json4s-scalaz" % json4sVersion,
  "org.slf4j" % "log4j-over-slf4j" % "1.7.30",
  "com.adrianhurt" %% "play-bootstrap" % "1.5.1-P27-B3" exclude("com.typesafe.play", "*"),
  "org.clapper" %% "grizzled-slf4j" % "1.3.3",
  "org.apache.kafka" %% "kafka" % kafkaVersion exclude("log4j","log4j") exclude("org.slf4j", "slf4j-log4j12") force(),
  "org.apache.kafka" % "kafka-streams" % kafkaVersion,
  "com.beachape" %% "enumeratum" % "1.5.14",
  "com.github.ben-manes.caffeine" % "caffeine" % "2.8.0",
  "com.typesafe.play" %% "play-logback" % "2.8.0",
  "org.scalatest" %% "scalatest" % "3.1.0" % "test",
  "org.scalatestplus.play" %% "scalatestplus-play" % "4.0.3" % "test",
  "org.apache.curator" % "curator-test" % curatorVersion % "test",
  "org.mockito" % "mockito-core" % "3.2.4" % "test",
  "com.yammer.metrics" % "metrics-core" % "2.2.0" force(),
  "com.unboundid" % "unboundid-ldapsdk" % "4.0.14"
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
/*
 * Enable systemd as systemloader
 */

enablePlugins(SystemdPlugin)


enablePlugins(sbtdocker.DockerPlugin)
dockerfile in docker := {
  val zipFile: File = dist.value

  new Dockerfile {
    from("openjdk:11-jre-slim")
    runRaw("apt-get update && apt-get install -y --no-install-recommends unzip")
    add(zipFile, file("/opt/cmak.zip"))
    workDir("/opt")
    run("unzip", "cmak.zip")
    run("rm", "-f", "cmak.zip")

    expose(9000)

    cmd(s"cmak-${version.value}/bin/cmak")
  }
}

imageNames in docker := Seq(
  ImageName(
    s"${name.value}:${version.value}"
  )
)

buildOptions in docker := BuildOptions(
  pullBaseImage = BuildOptions.Pull.Always
)

/*
 * Start service as user root
 */

daemonUser in Linux := "root"

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
rpmUrl := Some("https://github.com/yahoo/cmak")
rpmLicense := Some("Apache")
rpmGroup := Some("cmak")

/* End RPM Settings */
