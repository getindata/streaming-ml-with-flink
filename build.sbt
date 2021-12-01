import sbt.Keys.libraryDependencies

ThisBuild / resolvers ++= Seq(
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    Resolver.mavenLocal
)

name := "flink-mleap"

version := "0.1"

organization := "com.getindata"

ThisBuild / scalaVersion := "2.12.8"

val flinkVersion = "1.14.0"


val commonDependencies = Seq(
  "org.apache.flink" %% "flink-clients" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-table-api-scala-bridge" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-table-planner" % flinkVersion % "provided",

  "ml.combust.mleap" %% "mleap-runtime" % "0.17.0",
  "com.google.cloud" % "google-cloud-storage" % "2.2.2",
  "net.lingala.zip4j" % "zip4j" % "2.9.0"
)

lazy val lib = (project in file("lib")).
  settings(
    libraryDependencies ++= commonDependencies,

    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % Test
)

lazy val example = (project in file("example")).
  settings(
    libraryDependencies ++= commonDependencies
  )
  .dependsOn(lib)


// make run command include the provided dependencies
Compile / run  := Defaults.runTask(Compile / fullClasspath,
                                   Compile / run / mainClass,
                                   Compile / run / runner
                                  ).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)

lazy val mainRunner = project.in(file("mainRunner")).dependsOn(example).settings(
  // we set all provided dependencies to none, so that they are included in the classpath of mainRunner
  libraryDependencies := (libraryDependencies in example).value.map{
    module => module.configurations match {
      case Some("provided") => module.withConfigurations(None)
      case _ => module
    }
  }
)
