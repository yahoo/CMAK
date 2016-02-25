/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
name := """kafka-manager"""

scalaVersion := "2.11.7"

scalacOptions ++= Seq("-Xlint:-missing-interpolator","-Xfatal-warnings","-deprecation","-feature","-language:implicitConversions","-language:postfixOps")

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
  "org.apache.curator" % "curator-framework" % "2.9.1" exclude("log4j","log4j") exclude("org.slf4j", "slf4j-log4j12") force(),
  "org.apache.curator" % "curator-recipes" % "2.9.1" exclude("log4j","log4j") exclude("org.slf4j", "slf4j-log4j12") force(),
  "org.json4s" %% "json4s-jackson" % "3.2.11",
  "org.json4s" %% "json4s-scalaz" % "3.2.11",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.12",
  "com.adrianhurt" %% "play-bootstrap3" % "0.4.5-P24",
  "org.clapper" %% "grizzled-slf4j" % "1.0.2",
  "org.apache.kafka" %% "kafka" % "0.8.2.2" exclude("log4j","log4j") exclude("org.slf4j", "slf4j-log4j12") force(),
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.apache.curator" % "curator-test" % "2.9.1" % "test",
  "org.mockito" % "mockito-core" % "1.10.19" % "test",
  "com.yammer.metrics" % "metrics-core" % "2.2.0" force()
)

val DevelopementFactoryUrl="http://udd-phenix.edc.carrefour.com/nexus/content"

publishTo <<= (isSnapshot) apply Packaging.phenixRepo

//if (credentialFile.exists())
credentials += Credentials(Packaging.credentialFile)

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


enablePlugins(RpmPlugin, RpmDeployPlugin)

/* RPM Settings - to create, run as:
   $ sbt rpm:packageBin

   See here for details:
   http://www.scala-sbt.org/sbt-native-packager/formats/rpm.htmlsbt depl
*/


com.typesafe.sbt.packager.rpm.RpmPlugin.projectSettings

com.typesafe.sbt.packager.rpm.RpmPlugin.globalSettings

com.typesafe.sbt.packager.rpm.RpmPlugin.buildSettings

packageSummary := "Kafka Manager"
packageDescription := "Kafka Manager"
com.typesafe.sbt.packager.Keys.maintainer in Rpm := "Carrefour"
rpmVendor in Rpm := "Carrefour"
packageArchitecture := "noarch"
rpmDistribution := Some("CentOS")
rpmRelease <<= (version, baseDirectory) apply Packaging.rpmReleaseVersion
version in Rpm <<=  (version) apply Packaging.makeVersion
rpmLicense in Rpm := Some("Apache 2.0")
rpmGroup in Rpm := Some("phenix")
rpmBrpJavaRepackJars := true //see https://github.com/sbt/sbt-native-packager/issues/327
rpmRequirements += "jdk >= 1.8.0"
publishLocal <<= publishLocal.dependsOn(publishLocal in config("rpm"))
publish <<= publish.dependsOn(publish in config("rpm"))

daemonGroup in Linux := "phenix"

rpmPre in Rpm := Some(s"getent group ${(daemonGroup in Linux).value}  >/dev/null 2>&1 || groupadd -r ${(daemonGroup in Linux).value}; getent passwd ${(daemonUser in Linux).value}  >/dev/null 2>&1 || useradd -r -g ${(daemonGroup in Linux).value} ${(daemonUser in Linux).value} && mkdir /var/run/$name && chown -R ${(daemonUser in Linux).value}:${(daemonGroup in Linux).value} /var/run/$name")
/* End RPM Settings */