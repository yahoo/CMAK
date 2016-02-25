resolvers += "phenix releases" at "http://udd-phenix.edc.carrefour.com/nexus/content/repositories/releases"
resolvers += "phenix snapshots" at "http://udd-phenix.edc.carrefour.com/nexus/content/repositories/snapshots"

addSbtPlugin("com.carrefour.phenix" % "phenix-build" % "0.10.0" )

// Use the Play sbt plugin for Play projects
addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.4.4")

addSbtPlugin("com.typesafe.sbt" % "sbt-jshint" % "1.0.3")

addSbtPlugin("com.typesafe.sbt" % "sbt-digest" % "1.1.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-gzip" % "1.0.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-less" % "1.1.1")

//addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.1")

// Support packaging plugins
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.0.5")

//resolvers += Classpaths.sbtPluginReleases

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.3.3")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.0.0")
