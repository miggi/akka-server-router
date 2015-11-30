name := "akka-load-balancer"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" % "akka-actor_2.11" % "2.4.1",
  "com.typesafe.akka" % "akka-remote_2.11" % "2.4.1",
  "com.h2database" % "h2" % "1.4.190",
  "commons-codec" % "commons-codec" % "1.10"
)
