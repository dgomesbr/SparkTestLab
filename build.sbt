name := "SparkTestLab"

organization := "com.diegomagalhaes"

version := "1.0"

scalaVersion := "2.10.4"

scalacOptions += "-deprecation"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.5.2",
  "com.diegomagalhaes" %% "scalanginxaccesslogparser" % "1.3-SNAPSHOT",
  "com.github.tototoshi" % "scala-csv_2.10" % "1.2.2"
)

resolvers += DefaultMavenRepository
resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

initialCommands := "import com.diegomagalhaes.sparktestlab._"