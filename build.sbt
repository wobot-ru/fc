resolvers in ThisBuild ++= Seq(Resolver.mavenLocal, "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/")

name := "Flink Focus Crawler"

version := "0.1-SNAPSHOT"

organization := "ru.wobot"

scalaVersion in ThisBuild := "2.11.7"
autoScalaLibrary := true
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }


val flinkVersion = "1.1-SNAPSHOT"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided")

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

mainClass in assembly := Some("ru.wobot.Job")

// make run command include the provided dependencies
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))


// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

