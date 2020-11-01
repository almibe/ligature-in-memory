import Dependencies._

ThisBuild / scalaVersion     := "2.13.3"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "dev.ligature"
ThisBuild / organizationName := "Ligature"

resolvers += Resolver.mavenLocal

lazy val root = (project in file("."))
  .settings(
    name := "ligature-keyvalue",
    libraryDependencies += "dev.ligature" %% "ligature" % "0.1.0-SNAPSHOT",
    libraryDependencies += "org.scodec" %% "scodec-bits" % "1.1.17",
    libraryDependencies += "org.scodec" %% "scodec-core" % "1.11.7",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "dev.ligature" %% "ligature-test-suite" % "0.1.0-SNAPSHOT" % Test
  )
