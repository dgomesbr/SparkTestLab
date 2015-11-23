package project

import sbt._
import Keys._

object SparkTestLabProjectBuild extends Build {

  import BuildSettings._

  // Configure prompt to show current project
  override lazy val settings = super.settings :+ {
    shellPrompt := { s => Project.extract(s).currentProject.id + " > " }
  }

  // Define our project, with basic project information and library dependencies
  lazy val project = Project("spark-example-project", file(".")).settings(buildSettings: _*)
}