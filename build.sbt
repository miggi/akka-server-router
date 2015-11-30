name := "akka-load-balancer"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" % "akka-actor_2.11" % "2.3.14",
  "com.typesafe.akka" % "akka-remote_2.11" % "2.3.14",
  "com.h2database" % "h2" % "1.4.190",
  "commons-codec" % "commons-codec" % "1.10"
)

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")
