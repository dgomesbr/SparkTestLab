package project

import sbt.Keys._
import sbt._
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.{MergeStrategy, PathList}

object BuildSettings {

  // Basic settings for our app
  lazy val basicSettings = Seq[Setting[_]](
    name := "SparkTestLab",
    organization := "com.diegomagalhaes",
    version := "1.0",
    description := "Simple job for the Spark cluster computing platform, ready for Amazon EMR",

    scalaVersion := "2.10.4",
    scalacOptions := Seq("-deprecation", "-encoding", "utf8"),
    javacOptions ++= Seq("-source", "1.7", "-target", "1.7"),

    resolvers ++= Seq(
      DefaultMavenRepository,
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
    ),

    libraryDependencies ++= Seq(
      "com.diegomagalhaes" %% "scalanginxaccesslogparser" % "1.3-SNAPSHOT",
      "org.apache.spark" % "spark-core_2.10" % "1.5.2" % "provided",
      "org.apache.spark" % "spark-sql_2.10" % "1.5.2" % "provided",
      "com.databricks" % "spark-csv_2.10" % "1.2.0",
      "com.github.tototoshi" % "scala-csv_2.10" % "1.2.2",
      "org.specs2" % "specs2_2.10" % "1.12.3" % "test"
    )
  )

  // sbt-assembly settings for building a fat jar

  lazy val sbtAssemblySettings = baseAssemblySettings ++ Seq(

    // Slightly cleaner jar name
    assemblyJarName in assembly := {
      name.value + "-" + version.value + ".jar"
    },

    // Drop these jars
    assemblyExcludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
      val excludes = Set(
        "jsp-api-2.1-6.1.14.jar",
        "jsp-2.1-6.1.14.jar",
        "jasper-compiler-5.5.12.jar",
        "commons-beanutils-core-1.8.0.jar",
        "commons-beanutils-1.7.0.jar",
        "servlet-api-2.5-20081211.jar",
        "servlet-api-2.5.jar"
      )
      cp filter { jar => excludes(jar.data.getName) }
    },

    assemblyMergeStrategy in assembly := {
      case x if x.endsWith(".html") => MergeStrategy.discard
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )

  lazy val buildSettings = basicSettings ++ sbtAssemblySettings
}