name := "ScalaKafkaStreamsDemo"

version := "1.0"

scalaVersion := "2.12.1"

libraryDependencies += "javax.ws.rs" % "javax.ws.rs-api" % "2.1.1" artifacts(Artifact("javax.ws.rs-api", "jar", "jar"))
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.1.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.1.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.1.0"
libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "2.1.0"

//TEST
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test
libraryDependencies += "org.apache.kafka" % "kafka-streams-test-utils" % "2.1.0" % Test

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.3"



