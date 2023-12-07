import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.{Stemmer, StopWordsCleaner, Tokenizer}
import com.johnsnowlabs.nlp.Finisher

import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import dataacquisition.{DataAcquisition, TextCleaner, VectorExpander}
import decisiontreealg.MapReduceAlgorithm

import java.nio.file.{Files, Path, Paths}
import scala.language.postfixOps
import scala.sys.process._
import decisiontree.DecisionTree
import decisiontree.Node
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF, VectorSlicer}
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


    val testLocal = false

    var dataset: DataFrame = null

    var inputPath = ""
    var outputPath = ""
    var what = ""
    var defaultMaxVocabSize = 500
    var maxVocabSizeCV = 0

    var num_cols = 0
    var num_rows = 0

    if (testLocal) {

      /* LOCAL TEST */


      val data_2 = Seq(
        Row("NASA's Juno spacecraft provides stunning images of Jupiter's polar regions", 1),
        Row("Breakthrough in the development of a COVID-19 treatment reduces severity of illness", 1),
        Row("Global initiative aims to eliminate mother-to-child transmission of HIV", 1),
        Row("Researchers make progress in the development of a universal cancer vaccine", 1),
        Row("Advancements in fusion energy research bring us closer to sustainable power", 1),
        Row("Renewable energy capacity in India sees significant growth", 1),
        Row("NASA's New Horizons spacecraft provides detailed images of Pluto's surface", 1),
        Row("Breakthrough in CRISPR technology allows for precise gene editing in plants", 1),
        Row("Scientists discover a parallel universe where gravity pushes instead of pulls", 0),
        Row("New law requires citizens to wear socks on their hands in public", 0),
        Row("Incredible breakthrough: Plants develop the ability to communicate with each other", 0),
        Row("Government introduces mandatory daily dance breaks for all citizens", 0),
        Row("Study finds that laughter can be used as a renewable energy source", 0),
        Row("Penguins start their own rock band and top the charts", 0),
        Row("Researchers create a device that turns dreams into reality", 0),
        Row("Aliens demand to be included in Earth's next census", 0),
        Row("New scientific experiment turns tomatoes into strawberries", 0),
        Row("International hot dog shortage declared; world leaders convene emergency summit", 0),
        Row("Astronauts host a space-themed cooking show from the International Space Station", 0),
        Row("Invention allows humans to breathe underwater; scuba diving industry in crisis", 0),
        Row("Government to replace all road signs with emojis for better communication", 0),
      )
      // Define the schema for the DataFrame
      val schema_2 = StructType(Seq(
        StructField("text", StringType, false),
        StructField("ground_truth", IntegerType, false)
      ))

      val finalDataset10 = spark.createDataFrame(spark.sparkContext.parallelize(data_2), schema_2)

      val textCleaner = new TextCleaner()
        .setInputCol("text")
        .setOutputCol("cleaned_title")


      val documentAssembler = new DocumentAssembler()
        .setInputCol(textCleaner.getOutputCol)
        .setOutputCol("document")

      // Tokenize the text into words
      val tokenizer = new Tokenizer()
        .setInputCols(Array(documentAssembler.getOutputCol))
        .setOutputCol("tokens")

      val remover = StopWordsCleaner.pretrained()
        .setInputCols(tokenizer.getOutputCol)
        .setOutputCol("cleanTokens")
        .setCaseSensitive(false)

      val stopWords = remover.getStopWords
      // Print or display the stop words
      stopWords.foreach(println)

      // Define the Stemmer annotator
      val stemmer = new Stemmer()
        .setInputCols(Array(tokenizer.getOutputCol))
        .setOutputCol("stemmedTokens")
        .setLanguage("English")

      /*
      val lemmatizer = LemmatizerModel.pretrained("lemma_lines", "en")
        .setInputCols("tokens")
        .setOutputCol("lemmaTokens")
       */

      val finisher = new Finisher()
        .setInputCols(stemmer.getOutputCol)
        .setOutputCols(Array("tokens"))
        .setOutputAsArray(true)
        .setCleanAnnotations(false)

      println(tokenizer.getOutputCol)

      val maxVocabSize = 500
      // Step 2: CountVectorizer to build a vocabulary
      val cv = new CountVectorizer()
        .setInputCol(finisher.getOutputCols(0))
        .setOutputCol("rawFeatures")
        .setVocabSize(maxVocabSize)

      // Step 3: IDF to transform the counts to TF-IDF
      val idf = new IDF()
        .setInputCol(cv.getOutputCol)
        .setOutputCol("features")

      val slicer = new VectorSlicer()
        .setInputCol("features")
        .setOutputCol("slicedFeatures")
        .setIndices(Array(0, 2))

      val finisher2 = new Finisher()
        .setInputCols(idf.getOutputCol)
        .setOutputCols(Array("final"))
        .setOutputAsArray(true)
        .setCleanAnnotations(false)

      // Create a pipeline with the tokenizer and stemmer
      val pipeline = new Pipeline().setStages(Array(textCleaner, documentAssembler, tokenizer, stemmer, finisher, cv, idf))

      // Fit the pipeline to the data
      val fittedPipelineModel = pipeline.fit(finalDataset10)

      // Transform the DataFrame
      val resultDF2 = fittedPipelineModel.transform(finalDataset10)

      // Retrieve the CountVectorizerModel from the fitted pipeline
      val countVectorizerModel = fittedPipelineModel.stages(5).asInstanceOf[CountVectorizerModel]
      // Get the vocabulary from the CountVectorizerModel
      val vocabulary = countVectorizerModel.vocabulary

      val vectorExpander = new VectorExpander()
        .setInputCol(idf.getOutputCol)
        .setOutputCol("")
        .setVocabulary(vocabulary) // Placeholder for vocabulary
        .setVocabSize(countVectorizerModel.getVocabSize)

      val pipeline2 = new Pipeline().setStages(Array(vectorExpander))

      // Fit the pipeline to the data
      val fittedPipelineModel2 = pipeline2.fit(resultDF2)

      // Transform the DataFrame
      dataset = fittedPipelineModel2.transform(resultDF2)

      dataset.show()
      println(dataset.schema)

    } else {

      /* EXECUTE ON CLUSTER */

      inputPath = args(0)
      outputPath = args(1)
      what = args(2)
      defaultMaxVocabSize = 500
      maxVocabSizeCV = Try(args(3).toInt).getOrElse(defaultMaxVocabSize)

      num_cols = Try(args(3).toInt).getOrElse(0)
      num_rows = Try(args(4).toInt).getOrElse(0)

      // RICORDARSI DI SETTARE HADOOP CONFIURATION PER LEGGERE E SCRIVERE DIRETTAMENTE DA GCS
      val keyfileName = "prefab-bruin-402414-a2db7e809915.json"
      val keyfileGCSPath = keyfileName //s"gs://$inputPath/$keyfileName"
      val keyfileLocalPath = "."
      GCSUtils.getFile(keyfileGCSPath, s"$keyfileLocalPath/$keyfileName")

      /*s"gsutil cp gs://your-gcs-bucket/spring-cab-402321-b19bfffc91be.json ./"
      val copyKeyfileCommand = s"hdfs dfs -copyFromLocal ./spring-cab-402321-b19bfffc91be.json hdfs:///user/"
      val copyKeyfileCommandExitCode = copyKeyfileCommand ! */

      // Set your Hadoop Configuration with GCS credentials
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      hadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      hadoopConf.set("google.cloud.auth.service.account.enable", "true")
      hadoopConf.set("google.cloud.auth.service.account.json.keyfile", s"$keyfileLocalPath/$keyfileName")

      val executorMemory = spark.sparkContext.getConf.get("spark.executor.memory")
      println(s"Executor Memory: $executorMemory")
      val executorCores = spark.sparkContext.getConf.get("spark.executor.cores")
      println(s"Executor Cores: $executorCores")
      val executorInstances = spark.sparkContext.getConf.get("spark.executor.instances")
      println(s"Executor Instances: $executorInstances")
      val sparkSerializer = spark.sparkContext.getConf.get("spark.serializer")
      println(s"Spark Serializer: $sparkSerializer")
      val sparkExtraJavaOptions = spark.sparkContext.getConf.get("spark.executor.extraJavaOptions")
      println(s"Spark ExtraJavaOptions: $sparkExtraJavaOptions")


      val decisionTreePath = "gs://fnc_bucket_final" // "/Users/luca/Desktop/tree.txt"
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


      val isDatasetPresent: Boolean = true

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

        //dataset = resultDF
        // ATTENZIONE ALLO SCHEMA. LA GROUND_TRUTH SEMBRA STRING
        println(dataset.schema.toString())

        println("NUM PARTITIONS: " + dataset.rdd.partitions.length.toString)
        println("fatto")

        dataset.show()


      }
    }



    println("DATASET COUNT", dataset.count())



    var lastColumns: Array[String] = null
    var finalTrainSet: DataFrame = null
    var testSet: DataFrame = null

    if (testLocal) {
      // Select the last 5 columns

      if (num_cols > 0) {
        lastColumns = dataset.columns.take(num_cols) ++ dataset.columns.takeRight(1)

        finalTrainSet = dataset.select(lastColumns.map(col): _*)
      }
      else
        finalTrainSet = dataset
    }
    else {

      /*// Conta il numero di righe per ciascuna classe
      val classCounts = dataset.groupBy("ground_truth").count()
      println("ClassCounts" , classCounts)*/

      // Dividi il DataFrame in due parti: una con label 0 e una con label 1
      val dfLabel0 = dataset.filter(col("ground_truth") === 0)
      val dfLabel1 = dataset.filter(col("ground_truth") === 1)

      val label0_count = dfLabel0.count().toInt
      val label1_count = dfLabel1.count().toInt

      val minCount = label0_count.min(label1_count)

      // Prendi un campione bilanciato per ciascuna label
      /*val trainLabel0 = dfLabel0.sample(withReplacement = false, minCount * 0.8 / label0_count) //SI PUÒ AGGIUNGERE IL SEED
      val trainLabel1 = dfLabel1.sample(withReplacement = false, minCount * 0.8 / label1_count) //SI PUÒ AGGIUNGERE IL SEED*/

      val trainLabel0 = dfLabel0.limit(num_rows) //SI PUÒ AGGIUNGERE IL SEED
      val trainLabel1 = dfLabel1.limit(num_rows)

      // Unisci i due campioni per ottenere il set di addestramento bilanciato
      val trainSet = trainLabel0.unionAll(trainLabel1)
      lastColumns = trainSet.columns.take(num_cols) ++ trainSet.columns.takeRight(1)

      finalTrainSet = trainSet.select(lastColumns.map(col): _*)

      // Rimuovi le righe utilizzate per l'addestramento dal DataFrame originale
      testSet = dataset.exceptAll(trainSet)

      // Suddividi il rimanente in test e validation
      //val Array(testSet, validationSet) = remainingData.randomSplit(Array(0.8, 0.2))

    }

    val dataPreparation: MapReduceAlgorithm = new MapReduceAlgorithm()
    val decTree = dataPreparation.startAlgorithm(finalTrainSet)
    decTree.asInstanceOf[Node].writeRulesToFile("./treeOutput.txt")

    if (testSet != null) {
      val predictedLabels = decTree.predict(testSet, decTree)

      println("predictedLabels")
      println(predictedLabels.mkString("Array(", ", ", ")"))
      val tmp: Array[Any] = testSet.select("ground_truth").collect().map(row => row(0))
      val trueLabels: Array[Int] = tmp.collect {
        case i: Int => i
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
    }


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