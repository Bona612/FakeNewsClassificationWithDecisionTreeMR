
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"


libraryDependencies ++= Seq(
  // https://mvnrepository.com/artifact/org.apache.spark/spark-core
  "org.apache.spark" %% "spark-core" % "3.4.0",
  // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
  "org.apache.spark" %% "spark-sql" % "3.4.0" % "provided",
  "org.apache.spark" %% "spark-mllib" % "3.4.0",
  // https://mvnrepository.com/artifact/com.johnsnowlabs.nlp/spark-nlp
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.1.0"
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
      case _ =>
        MergeStrategy.first
    },
    assemblyJarName := "FakeNewsClassificationWithDecisionTreeMR.jar"
    )
  .enablePlugins(AssemblyPlugin)

/*

case "com.johnsnowlabs.nlp.DocumentAssembler" =>
  //println("com.johnsnowlabs.nlp.DocumentAssembler")
  MergeStrategy.first
case "com.johnsnowlabs.nlp.annotator.LemmatizerModel" =>
  //println("com.johnsnowlabs.nlp.annotator.LemmatizerModel")
  MergeStrategy.first
case "com.johnsnowlabs.nlp.annotator.Stemmer" =>
  //println("com.johnsnowlabs.nlp.annotator.Stemmer")
  MergeStrategy.first
case "com.johnsnowlabs.nlp.annotator.StopWordsCleaner" =>
  //println("com.johnsnowlabs.nlp.annotator.StopWordsCleaner")
  MergeStrategy.first
case "com.johnsnowlabs.nlp.annotator.Tokenizer" =>
  //println("com.johnsnowlabs.nlp.annotator.Tokenizer")
  MergeStrategy.first
case x if x.startsWith("com/johnsnowlabs/nlp/") =>
  //println("discard: " + x)
  MergeStrategy.discard

proguardOptions := Seq(
  "-keep class com.johnsnowlabs.nlp.DocumentAssembler { *; }",
  "-keep class com.johnsnowlabs.nlp.annotator.LemmatizerModel { *; }",
  "-keep class com.johnsnowlabs.nlp.annotator.Stemmer { *; }",
  "-keep class com.johnsnowlabs.nlp.annotator.StopWordsCleaner { *; }",
  "-keep class com.johnsnowlabs.nlp.annotator.Tokenizer { *; }"
),
javaOptions in (Proguard, proguard) := Seq("-XX:+UseG1GC") // Adjust the value as needed, for example, "-Xmx4G" for 4 GB
 */


/*
mainClass in (Compile, packageBin) := Some("Main")

mappings in (Compile,packageBin) ~= { (ms: Seq[(File, String)]) =>
  ms filter { case (file, path) =>
    List("com/johnsnowlabs/nlp/DocumentAssembler.class",
      "com/johnsnowlabs/nlp/annotator/LemmatizerModel.class",
      "com/johnsnowlabs/nlp/annotator/Stemmer.class",
      "com/johnsnowlabs/nlp/annotator/StopWordsCleaner.class",
      "com/johnsnowlabs/nlp/annotator/Tokenizer.class").contains(path)
  }
}
*/

Global / onChangedBuildSource := ReloadOnSourceChanges

//artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) => "FakeNewsClassificationWithDecisionTreeMR.jar" }