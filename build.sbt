/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
name := """cmak"""

/* For packaging purposes, -SNAPSHOT MUST contain a digit */
version := "3.0.0.6"

scalaVersion := "2.12.13"

scalacOptions ++= Seq("-Xlint:-missing-interpolator", "-Xfatal-warnings", "-deprecation", "-feature", "-language:implicitConversions", "-language:postfixOps", "-Xmax-classfile-name", "240")

// From https://www.playframework.com/documentation/2.8.x/Deploying
assembly / assemblyMergeStrategy := {
  case referenceOverrides if referenceOverrides.contains("reference-overrides.conf") =>
    // Keep the content for all reference-overrides.conf files
    MergeStrategy.concat
  case "logger.xml" => MergeStrategy.first
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first
  case "module-info.class" => MergeStrategy.first
   case PathList("scala", "annotation", xs@_*) => MergeStrategy.first
  case other => (assembly / assemblyMergeStrategy).value(other)
}

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.5.19",
  "com.typesafe.akka" %% "akka-slf4j" % "2.5.19",
  "com.google.code.findbugs" % "jsr305" % "3.0.2",
  "org.webjars" %% "webjars-play" % "2.6.3",
  "org.webjars" % "bootstrap" % "4.3.1",
  "org.webjars" % "jquery" % "3.3.1-2",
  "org.webjars" % "backbonejs" % "1.3.3",
  "org.webjars" % "underscorejs" % "1.9.0",
  "org.webjars" % "dustjs-linkedin" % "2.7.2",
  "org.webjars" % "octicons" % "4.3.0",
  "org.apache.curator" % "curator-framework" % "2.12.0" exclude("log4j", "log4j") exclude("org.slf4j", "slf4j-log4j12") force(),
  "org.apache.curator" % "curator-recipes" % "2.12.0" exclude("log4j", "log4j") exclude("org.slf4j", "slf4j-log4j12") force(),
  "commons-codec" % "commons-codec" % "1.15",
  "org.json4s" %% "json4s-jackson" % "3.6.5",
  "org.json4s" %% "json4s-scalaz" % "3.6.5",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.25",
  "com.adrianhurt" %% "play-bootstrap" % "1.6.1-P28-B4" exclude("com.typesafe.play", "*"),
  "org.clapper" %% "grizzled-slf4j" % "1.3.3",
  "org.apache.kafka" %% "kafka" % "2.8.0" exclude("log4j", "log4j") exclude("org.slf4j", "slf4j-log4j12") force(),
  "org.apache.kafka" % "kafka-streams" % "2.8.0",
  "com.beachape" %% "enumeratum" % "1.5.13",
  "com.github.ben-manes.caffeine" % "caffeine" % "2.6.2",
  "com.typesafe.play" %% "play-logback" % "2.8.8",
  "org.scalatest" %% "scalatest" % "3.1.4" % "test",
  "org.scalatestplus.play" %% "scalatestplus-play" % "5.1.0" % "test",
  "org.apache.curator" % "curator-test" % "2.12.0" % "test",
  "org.scalatestplus" % "mockito-3-4_2.12" % "3.2.8.0" % "test",
  "com.yammer.metrics" % "metrics-core" % "2.2.0" force(),
  "com.unboundid" % "unboundid-ldapsdk" % "4.0.9"
)

routesGenerator := InjectedRoutesGenerator

Assets / LessKeys.compress := true

pipelineStages := Seq(digest, gzip)

Assets / LessKeys.less / includeFilter := "*.less"

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
docker / dockerfile := {
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

docker / imageNames := Seq(
  ImageName(
    s"${name.value}:${version.value}"
  )
)

docker / buildOptions := BuildOptions(
  pullBaseImage = BuildOptions.Pull.Always
)

/*
 * Start service as user root
 */

Linux / daemonUser := "root"

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

import com.typesafe.sbt.packager.rpm.RpmPlugin.autoImport.RpmConstants._

Rpm / maintainerScripts := maintainerScriptsAppend((Rpm / maintainerScripts).value)(
  Pre -> "%define _binary_payload w9.xzdio"
)

/* End RPM Settings */
