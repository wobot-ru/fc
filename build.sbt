//overrideBuildResolvers := true
//
resolvers in ThisBuild ++= Seq("Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/")
//resolvers in ThisBuild ++= Seq(Resolver.mavenLocal, "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/")
//resolvers in ThisBuild ++= Seq("Local Maven Repository" at "file://"+Path.userHome+"/.m2/repository")
//resolvers in ThisBuild ++= Seq(Resolver.mavenLocal)

name := "fc"

version := "0.1-SNAPSHOT"

organization := "ru.wobot"

scalaVersion in ThisBuild := "2.11.7"

autoScalaLibrary := true

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }


val flinkVersion = "1.1-SNAPSHOT"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-hbase" % flinkVersion,// % "provided",
  "org.apache.flink" %% "flink-connector-kafka-0.9" % flinkVersion,
  "org.apache.flink" %% "flink-table" % flinkVersion,
  "org.apache.hbase" % "hbase-common" % "0.98.11-hadoop2",
  "net.debasishg" %% "redisclient" % "3.0",
  "com.typesafe.play" %% "play-ws" % "2.5.4")

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

mainClass in assembly := Some("ru.wobot.example.CrawlJob")

assemblyMergeStrategy in assembly := {
  case x if x.endsWith(".class") => MergeStrategy.last
  case x if x.endsWith(".properties") => MergeStrategy.last
  case x if x.endsWith(".xml") => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    if (oldStrategy == MergeStrategy.deduplicate)
      MergeStrategy.first
    else
      oldStrategy(x)
}


// make run command include the provided dependencies
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
