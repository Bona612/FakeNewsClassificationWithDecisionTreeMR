
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  // MANCA UNA DEPENDENCY PER GOOGLE, NON RICORDO QUALE (Fatto)
  //"org.apache.hadoop" % "hadoop-gcs" % "3.2.0",
  "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.1.5",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-core
  "org.apache.spark" %% "spark-core" % "3.5.0",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "3.5.0",
  // https://mvnrepository.com/artifact/com.johnsnowlabs.nlp/spark-nlp
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.1.4",
  "com.google.cloud" % "google-cloud-storage" % "2.29.1",
  "org.scala-lang" %% "toolkit" % "0.1.7"
)


lazy val root = (project in file("."))
  .settings(
    name := "FakeNewsClassificationWithDecisionTreeMR",
    assemblyMergeStrategy := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
      case "log4j.properties" => MergeStrategy.discard
      case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
      case "reference.conf" => MergeStrategy.concat
      case _ =>
        MergeStrategy.first
    },
    assemblyJarName := "FakeNewsClassificationWithDecisionTreeMR.jar"
    )
  .enablePlugins(AssemblyPlugin)



Global / onChangedBuildSource := ReloadOnSourceChanges