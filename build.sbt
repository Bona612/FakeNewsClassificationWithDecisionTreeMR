ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  // https://mvnrepository.com/artifact/org.apache.spark/spark-core
  "org.apache.spark" %% "spark-core" % "3.4.0",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  "org.apache.spark" %% "spark-sql" % "3.4.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "3.4.0",
  // https://mvnrepository.com/artifact/com.johnsnowlabs.nlp/spark-nlp
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.1.0",
  // https://mvnrepository.com/artifact/org.tartarus/snowball
  "com.github.rholder" % "snowball-stemmer" % "1.3.0.581.1",
  "com.eed3si9n" % "sbt-assembly" % "2.1.4" % "assembly"
)


lazy val root = (project in file("."))
  .settings(
    name := "FakeNewsClassificationWithDecisionTreeMR",
    assemblyMergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
      case "log4j.properties" => MergeStrategy.discard
      case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    },
    assemblyJarName := "FakeNewsClassificationWithDecisionTreeMR.jar"
  )
  .enablePlugins(AssemblyPlugin)

Global / onChangedBuildSource := ReloadOnSourceChanges

//artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) => "FakeNewsClassificationWithDecisionTreeMR.jar" }