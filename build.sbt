name := "SparkTestLab"

organization := "com.diegomagalhaes"

version := "1.0"

scalaVersion := "2.10.4"

scalacOptions += "-deprecation"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.1" ,
  "com.diegomagalhaes" %% "scalanginxaccesslogparser" % "1.2-SNAPSHOT",
  "ch.qos.logback" % "slf4j-log4j12" % "1.7.10"
)

resolvers ++= Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)

initialCommands := "import com.diegomagalhaes.sparktestlab._"