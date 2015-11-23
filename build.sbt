/* Project Description */
name := "SparkTestLab"
organization := "com.diegomagalhaes"
version := "1.0"

/* OPTS */
scalaVersion := "2.10.4"
scalacOptions ++= Seq("-deprecation")
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

/* Dependencies */
libraryDependencies ++= Seq(
  "com.diegomagalhaes" %% "scalanginxaccesslogparser" % "1.3-SNAPSHOT",
  "org.apache.spark" % "spark-core_2.10" % "1.5.2"%"provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.5.2"%"provided",
  "com.databricks" % "spark-csv_2.10" % "1.2.0",
  "com.github.tototoshi" % "scala-csv_2.10" % "1.2.2"
)

/* Resolvers */
resolvers += DefaultMavenRepository
resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

/* Assembly Config */
mainClass in assembly := Some("com.diegomagalhaes.spark.RecomendationSparkSQLApp")
assemblyJarName := "RecomendationSparkSQLApp.jar"
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

/* Initializer */
initialCommands := "import com.diegomagalhaes.sparktestlab._"
