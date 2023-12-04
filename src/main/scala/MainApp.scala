import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.{Stemmer, StopWordsCleaner, Tokenizer}

import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import dataacquisition.DataAcquisition
import decisiontreealg.MapReduceAlgorithm

import java.nio.file.{Files, Path, Paths}
import scala.language.postfixOps
import scala.sys.process._
import decisiontree.DecisionTree
import decisiontree.Node
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import utils.GCSUtils

import scala.util.Try
//import org.apache.spark.sql.functions.{col, lit, rand, row_number}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.math.Fractional.Implicits.infixFractionalOps

object MainApp {

  // MI PARE MANCHI UN CONFIG
  final val spark: SparkSession = SparkSession.builder()
                                  .master("local[*]")
                                  .appName("Fake News Classification")
                                  .getOrCreate()


  def main(args: Array[String]): Unit = {

    val data_2 = Seq(
      Row("  This is \"the\"  . first document.", 1),
      Row(" This document is \"the\" second . document.", 0),
      Row("And this is . \"the\" third one .  ", 1),
      Row(" Is this. \"the\" first document? ", 1)
    )
    // Define the schema for the DataFrame
    val schema_2 = StructType(Seq(
      StructField("text", StringType, false),
      StructField("ground_truth", IntegerType, false)
    ))
    val finalDataset10 = spark.createDataFrame(spark.sparkContext.parallelize(data_2), schema_2)

    val documentAssembler = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    // Tokenize the text into words
    val tokenizer = new Tokenizer()
      .setInputCols("document")
      .setOutputCol("tokens")

    val remover = StopWordsCleaner.pretrained()
      .setInputCols(Array("tokens"))
      .setOutputCol("cleanTokens")
      .setCaseSensitive(false)

    val stopWords = remover.getStopWords
    // Print or display the stop words
    stopWords.foreach(println)

    // Define the Stemmer annotator
    val stemmer = new Stemmer()
      .setInputCols(Array("cleanTokens"))
      .setOutputCol("stemmedTokens")
      .setLanguage("English")

    /*
    val lemmatizer = LemmatizerModel.pretrained("lemma_lines", "en")
      .setInputCols("tokens")
      .setOutputCol("lemmaTokens")
     */

    // Create a pipeline with the tokenizer and stemmer
    val pipeline = new Pipeline().setStages(Array(documentAssembler, tokenizer, remover, stemmer))

    // Fit the pipeline to the data
    val model = pipeline.fit(finalDataset10)

    // Transform the DataFrame
    val resultDF = model.transform(finalDataset10)
    // Selecting a single column and creating a new DataFrame
    val results = resultDF.selectExpr("*", "stemmedTokens.result as final_tokens")
    val results_tosave = results.select("final_tokens", "ground_truth").dropDuplicates()
    results_tosave.show()

    val inputPath = args(0)
    val outputPath = args(1)
    // MOMENTANEAMENTE UTILE SOLO PER VEDERE SE RIFARE CREAZIONE DATASET O NO
    val what = args(2)
    // ALLORA QUESTO SARà UN INTERO QUINDI USARE TRY E FARE IL GET DALLA STRINGA
    val defaultMaxVocabSize = 500
    val maxVocabSizeCV = Try(args(3).toInt).getOrElse(defaultMaxVocabSize)


    /*val data_2 = Seq(
      Row("  This is \"the\"  . first document.", 1),
      Row(" This document is \"the\" second . document.", 0),
      Row("And this is . \"the\" third one .  ", 1),
      Row(" Is this. \"the\" first document? ", 1)
    )
    // Define the schema for the DataFrame
    val schema_2 = StructType(Seq(
      StructField("text", StringType, false),
      StructField("label", IntegerType, false)
    ))
    val finalDataset10 = spark.createDataFrame(spark.sparkContext.parallelize(data_2), schema_2)
    val splittedDF = finalDataset10.withColumn("split", split(col("text"), "\\."))
    val explodedDF = splittedDF.select(explode(col("split")).alias("exploded"))
    val trimmedDF = explodedDF.select(trim(col("exploded")).alias("title"))
    val noWhitespace = trimmedDF.filter("title != ''")

    val resultDF = noWhitespace.withColumn("ground_truth", lit(1))
    println(resultDF.schema)
    println("vediamo le colonne")
    resultDF.columns.foreach(println)

    // Updated filter condition to check for numeric values
    val dfNot01 = resultDF.filter(col("ground_truth").notEqual(0) && col("ground_truth").notEqual(1))
    if (dfNot01.isEmpty) {
      println("BENEEEEE")
    } else {
      println("NO BUONO")
      dfNot01.show()
    }

    println("text function")
    resultDF.show()*/

    /*// Specify your output path and format (e.g., parquet, csv, etc.)
    val outputPath_save = "hdfs:///user/"
    // Write the DataFrame to a single CSV file
    resultDF.write //.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .option("quote", "\"") // Quote character
      .option("escape", "\"") // Quote escape character (end of quote)
      .option("multiLine", "true")
      .option("delimiter", ",")
      .option("charset", "UTF-8")
      .csv(outputPath_save)

    // Read CSV file from HDFS
    val labeledDFsave_read: DataFrame = spark.read
      .option("header", "true")
      .option("quote", "\"") // Quote character
      .option("escape", "\"") // Quote escape character (end of quote)
      .option("multiLine", "true")
      .option("sep", ",")
      .option("charset", "UTF-8")
      .csv(outputPath_save)

    println(labeledDFsave_read.schema)
    labeledDFsave_read.show()

    // Updated filter condition to check for numeric values
    val dfNot012 = labeledDFsave_read.filter(col("ground_truth").notEqual(0) && col("ground_truth").notEqual(1))
    if (dfNot012.isEmpty) {
      println("BENEEEEE")
    } else {
      println("NO BUONO")
      dfNot012.show()
    }*/



    // RICORDARSI DI SETTARE HADOOP CONFIURATION PER LEGGERE E SCRIVERE DIRETTAMENTE DA GCS
    val keyfileName = "spring-cab-402321-b19bfffc91be.json"
    val keyfileGCSPath = keyfileName //s"gs://$inputPath/$keyfileName"
    val keyfileLocalPath = "."
    GCSUtils.getFile(keyfileGCSPath, s"$keyfileLocalPath/$keyfileName")
    //s"gsutil cp gs://your-gcs-bucket/spring-cab-402321-b19bfffc91be.json ./"
    //val copyKeyfileCommand = s"hdfs dfs -copyFromLocal ./spring-cab-402321-b19bfffc91be.json hdfs:///user/"
    //val copyKeyfileCommandExitCode = copyKeyfileCommand !

    // Set your Hadoop Configuration with GCS credentials
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    hadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoopConf.set("google.cloud.auth.service.account.enable", "true")
    hadoopConf.set("google.cloud.auth.service.account.json.keyfile", s"$keyfileLocalPath/$keyfileName")



    val decisionTreePath = "gs://fnc-bucket-final" // "/Users/luca/Desktop/tree.txt"
    //val tree = DecisionTree.fromFile(decisionTreePath)

    //var directoryStream = Files.list(Paths.get(decisionTreePath))
    //var fileList = directoryStream.toArray

    val currentDir = new File(".").getCanonicalPath
    println(s"currentDir $currentDir")

    println(s"Input Path $inputPath")
    println(inputPath)
    println(s"Output Path $outputPath")
    println(outputPath)
    // NON USARE Paths.get() per GCS
    println(Paths.get(inputPath).toString)
    println(Paths.get(inputPath).resolve("data/download").toString)
    println(s"$inputPath/data/download")

    /*
    var tree: DecisionTree = null

    fileList.foreach { path =>
      if (path.asInstanceOf[Path].toString == Paths.get(decisionTreePath).resolve("tree.txt").toString) {

        tree = DecisionTree.fromFile(decisionTreePath+"/tree.txt")
      }
    }

    if(tree != null)
      return
     */
    /*
    var node : DecisionTree = Node("Prova",0.3,null,null,None)
    var tmp = node
    tmp.asInstanceOf[Node].setAttribute()

    println(node.asInstanceOf[Node].getAttribute())
    println(tmp.asInstanceOf[Node].getAttribute())
    */

    //tree.asInstanceOf[Node].writeRulesToFile("/Users/luca/Desktop/treeOutput.txt")

    println("Start")
    val downloadPath: String = "data/download"
    val datasetPath = "data/dataset"
    val csv = "dataset.csv"


    // Creating a list of strings
    // "jruvika/fake-news-detection",
    val kaggleDatasetList: List[String] = List("therohk/million-headlines", "clmentbisaillon/fake-and-real-news-dataset", "mrisdal/fake-news", "hassanamin/textdb3", "saurabhshahane/fake-news-classification", "stevenpeutz/misinformation-fake-news-text-dataset-79k", "jainpooja/fake-news-detection", "niranjanank/fake-news-log", "mohammadaflahkhan/fake-news-dataset-combined-different-sources")
    // Create a Map
    // "fake-news-detection" -> "data.csv",
    val csvPerDataset: Map[String, String] = Map("million-headlines" -> "abcnews-date-text.csv", "fake-and-real-news-dataset" -> "Fake.csv", "fake-news" -> "fake.csv", "textdb3" -> "fake_or_real_news.csv", "fake-news-classification" -> "WELFake_Dataset.csv", "misinformation-fake-news-text-dataset-79k" -> "DataSet_Misinfo_FAKE.csv", "fake-news-detection" -> "Fake.csv", "fake-news-log" -> "train.csv", "fake-news-dataset-combined-different-sources" -> "PreProcessedData.csv")
    // , "fake-news-detection" -> "Headline"
    val columnsMap: Map[String, String] = Map("million-headlines" -> "headline_text", "fake-and-real-news-dataset" -> "title", "fake-news" -> "title", "textdb3" -> "title", "fake-news-classification" -> "title", "misinformation-fake-news-text-dataset-79k" -> "text", "fake-news-detection" -> "title", "fake-news-log" -> "title", "fake-news-dataset-combined-different-sources" -> "title")
    val textColumn: String = "title"

    /*
    // Creating a list of strings
    val kaggleDatasetList: List[String] = List("mrisdal/fake-news")
    // Create a Map
    val csvPerDataset: Map[String, String] = Map("fake-news" -> "fake.csv")
    val columnsMap: Map[String, String] = Map("fake-news" -> "title")
    val textColumn: String = "title"
    */

    // Use the Files.list method to get a Stream of paths in the directory
    //directoryStream = Files.list(Paths.get(datasetPath))

    val isDatasetPresent = false  // CHIARAMENTE DA SISTEMARE CON IL VERO CODICE  !!!
    // isDatasetPresent = isFilePresent(s"$datasetPath/$csv", spark)

/*
    // Convert the Stream to a Scala List and print the file names
    fileList = directoryStream.toArray
    fileList.foreach { path =>
      if (path.asInstanceOf[Path].toString == Paths.get(datasetPath).resolve(csv).toString) {
        isDatasetPresent = true
      }
    }
  */
    // Close the directory stream
    //directoryStream.close()

    var dataset: DataFrame = null
    // If the dataset isn't created, load the dataset and save it
    if (!isDatasetPresent) {
      // INSERIRE IL PROCESSO DI TRY DA ARGS(3), GESTIRE SIA SUCCESS CHE FAILURE (FATTO SOPRA)
      val dataAcquisition: DataAcquisition = new DataAcquisition(kaggleDatasetList, csvPerDataset, columnsMap, textColumn, s"$inputPath/$downloadPath", s"$inputPath/$datasetPath", csv, maxVocabSizeCV, spark)
      dataset = dataAcquisition.loadDataset()
      println("Dataset loaded succesfully!")
    }
    else {

      // QUI INSERIRE IL LOADING DIRETTO DA GCS, FORSE AGIUNGERE QUALCHE CONFIG
      dataset = spark.read
        .option("header", "true")
        .option("quote", "\"") // Quote character
        .option("escape", "\"") // Quote escape character (end of quote)
        .option("multiLine", "true")
        .option("sep", ",")
        .option("charset", "UTF-8")
        .csv(s"$inputPath/$datasetPath/$csv")

      // ATTENZIONE ALLO SCHEMA. LA GROUND_TRUTH SEMBRA STRING
      println(dataset.schema.toString())

      /*
      val loadCommand = s"gsutil cp $inputPath/$datasetPath/$csv ./"
      val exitCodeLoad = loadCommand !

      if (exitCodeLoad == 0) {
        println("Loading from GS riuscito!")
      }
      else {
        println("Problema nel loading da GS...")
      }

      val fromLocal = s"hdfs dfs -copyFromLocal ./$csv hdfs:///user/fnc_user/"
      val exitCodeFromlocal = fromLocal !

      if (exitCodeFromlocal == 0) {
        println("Loading from local riuscito!")
      }
      else {
        println("Problema nel loading from local...")
      }

      dataset = spark.read
        .option("header", "true")
        .option("escape", "\"")
        .option("multiLine", "true")
        .option("sep", ",")
        .option("charset", "UTF-8")
        .csv(s"hdfs:///user/fnc_user/$csv")
       */

      println("NUM PARTITIONS: " + dataset.rdd.partitions.length.toString)
      println("fatto")

      dataset.show()

      // DA QUI IN Avanti vai te andri o tot
    }


    // write store datset in a directory of .csv, one csv file for each partition
    // coalesce instead, allow to select the number of partition that you want

    // Save the DataFrame as a CSV file
    //dataset.write.csv(Paths.get(datasetPath).resolve(csv).toString)

    // Coalesce to a single partition
    // Save as a single CSV file
    //dataset.coalesce(1).write.csv(Paths.get(datasetPath).resolve(csv).toString)
    //dataset.coalesce(1).write.csv(s"./$csv")

    // FORSE DA PROVARE
    //s"gsutil cp ./$csv gs://$outputPath/$csv".!!

    // MOMENTANEAMENTE COMMENTO TUTTO SOTTO PER FARE UNA PROVA DI CREAZIONE DEL DATASET


    println("DATASET COUNT", dataset.count())

    // Conta il numero di righe per ciascuna classe
    val classCounts = dataset.groupBy("label").count()
    println("ClassCounts" , classCounts)

     // Calcola il numero massimo di righe da prendere per ciascuna label nel set di addestramento
    val maxRowsPerLabel = dataset.groupBy("label").agg(count("label").alias("count")).agg(min("count")).collect()(0).getLong(0).toInt

    // Dividi il DataFrame in due parti: una con label 0 e una con label 1
    val dfLabel0 = dataset.filter("label = 0")
    val dfLabel1 = dataset.filter("label = 1")

    println(maxRowsPerLabel)
    println(dfLabel0.count())
    println(dfLabel0.count()*0.8)
    println(dfLabel1.count())
    println(dfLabel1.count()*0.8)
    // Prendi un campione bilanciato per ciascuna label
    val trainLabel0 = dfLabel0.sample(false, (maxRowsPerLabel.toDouble)*0.8 / dfLabel0.count()) //SI PUÒ AGGIUNGERE IL SEED
    val trainLabel1 = dfLabel1.sample(false, (maxRowsPerLabel.toDouble)*0.8 / dfLabel1.count()) //SI PUÒ AGGIUNGERE IL SEED


    // Unisci i due campioni per ottenere il set di addestramento bilanciato
    val trainSet = trainLabel0.union(trainLabel1)

    // Rimuovi le righe utilizzate per l'addestramento dal DataFrame originale
    val testSet = dataset.except(trainSet)

    // Suddividi il rimanente in test e validation
    //val Array(testSet, validationSet) = remainingData.randomSplit(Array(0.8, 0.2))


    println("Train Set: ",trainSet.count())
    println("Train Set Label 0: ", trainSet.filter(col("label") === 0).count())
    println("Train Set Label 1: ", trainSet.filter(col("label") === 1).count() )
    println("Test Set: " , testSet.count())
    println("Test Set Label 0: ", testSet.filter(col("label") === 0).count())
    println("Test Set Label 1: ", testSet.filter(col("label") === 1).count())
  /*
    val dataPreparation: MapReduceAlgorithm = new MapReduceAlgorithm(trainSet)
    val decTree = dataPreparation.initAlgorithm()
    decTree.asInstanceOf[Node].writeRulesToFile("/Users/luca/Desktop/treeOutput.txt")

    val predictedLabels = decTree.predict(testSet,decTree)

    println("predictedLabels")
    println(predictedLabels.mkString("Array(", ", ", ")"))
    val tmp : Array[Any] = testSet.select("label").collect().map(row => row(0))
    val trueLabels : Array[Int] = tmp.collect{
      case i : Int => i
    }

    println("trueLabels")
    println(trueLabels.mkString("Array(", ", ", ")"))


    val (truePositives, falsePositives, falseNegatives) = calculateMetrics(trueLabels, predictedLabels)

    // Print the results
    println(s"True Positives: $truePositives")
    println(s"False Positives: $falsePositives")
    println(s"False Negatives: $falseNegatives")

    // Calculate precision, recall, and F1-score
    val precision = truePositives.toDouble / (truePositives + falsePositives)
    val recall = truePositives.toDouble / (truePositives + falseNegatives)
    val f1Score = 2 * (precision * recall) / (precision + recall)

    // Print precision, recall, and F1-score
    println(s"Precision: $precision")
    println(s"Recall: $recall")
    println(s"F1-Score: $f1Score")

     */

    println("Stopping Spark")
    // Stop the Spark session
    spark.stop()
    println("Stop")
  }

  def calculateMetrics(trueLabels: Array[Int], predictedLabels: Array[Int]): (Int, Int, Int) = {
    if (trueLabels.length != predictedLabels.length) {
      throw new IllegalArgumentException("Input arrays must have the same length.")
    }

    var truePositives = 0
    var falsePositives = 0
    var falseNegatives = 0

    for ((trueLabel, predictedLabel) <- trueLabels zip predictedLabels) {
      if (trueLabel == 1 && predictedLabel == 1) {
        truePositives += 1
      } else if (trueLabel == 0 && predictedLabel == 1) {
        falsePositives += 1
      } else if (trueLabel == 1 && predictedLabel == 0) {
        falseNegatives += 1
      }
    }

    (truePositives, falsePositives, falseNegatives)
  }

  object MetricsCalculator {
    def main(args: Array[String]): Unit = {
      // Example: True labels and predicted labels
      val trueLabels = Array(1, 0, 1, 0, 1, 0, 0, 1, 1, 0)
      val predictedLabels = Array(1, 0, 0, 0, 1, 1, 0, 1, 1, 1)

      // Calculate true positives, false positives, and false negatives
      val (truePositives, falsePositives, falseNegatives) = calculateMetrics(trueLabels, predictedLabels)

      // Print the results
      println(s"True Positives: $truePositives")
      println(s"False Positives: $falsePositives")
      println(s"False Negatives: $falseNegatives")

      // Calculate precision, recall, and F1-score
      val precision = truePositives.toDouble / (truePositives + falsePositives)
      val recall = truePositives.toDouble / (truePositives + falseNegatives)
      val f1Score = 2 * (precision * recall) / (precision + recall)

      // Print precision, recall, and F1-score
      println(s"Precision: $precision")
      println(s"Recall: $recall")
      println(s"F1-Score: $f1Score")
    }

  }





}