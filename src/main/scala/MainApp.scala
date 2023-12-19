import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.{Stemmer, StopWordsCleaner, Tokenizer}
import com.johnsnowlabs.nlp.Finisher

import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import dataacquisition.{DataAcquisition, TextCleaner, VectorExpander}

import java.nio.file.{Files, Path, Paths}
import scala.language.postfixOps
import scala.sys.process._
import decisiontree.DecisionTreeClassifier
import decisiontree.Node
import org.apache.spark.{SparkFiles, ml}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, IDF, VectorAssembler, VectorSlicer}
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import utils.GCSUtils

import scala.util.Try
//import org.apache.spark.sql.functions.{col, lit, rand, row_number}
import decisiontree.{Leaf, NewDecisionTree, Node}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.math.Fractional.Implicits.infixFractionalOps

object MainApp {


  // MI PARE MANCHI UN CONFIG
  var spark: SparkSession = _

  def main(args: Array[String]): Unit = {

    // set true if you want to test in local
    val testLocal = true

    /*
    * vars set in local mode
    * in cluster mode they will be set at beginning of else
    */
    var dataset: DataFrame = null

    var inputPath = ""
    var outputPath = ""
    var what = ""
    var defaultMaxVocabSize = 500
    var maxVocabSizeCV = 0

    var num_cols = 0  // 0 is default, > 0 change number of cols to take
    var num_rows = 0

    if (testLocal) {

      spark  =SparkSession.builder()
        .master("local[6]")
        .appName("Fake News Classification")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.adaptive.enabled","true")
        .getOrCreate()

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

      /*val remover = StopWordsCleaner.pretrained()
        .setInputCols(tokenizer.getOutputCol)
        .setOutputCol("cleanTokens")
        .setCaseSensitive(false)

      val stopWords = remover.getStopWords
      // Print or display the stop words
      stopWords.foreach(println)*/

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


      /*
      // DA PROVARE
      val decisionTreeMaxDepth = 10
      val customDecisionTree = new DecisionTreeClassifier()
        .setMaxDepth(decisionTreeMaxDepth)

      // Assuming you have a DataFrame called "trainingData" with columns "feature1", "feature2", and "label"
      val modelDT = customDecisionTree.fit(dataset.repartition(4).cache())

      modelDT.getDecisionTree.asInstanceOf[Node].writeRulesToFile("C:\\Users\\bocca\\Desktop\\perAndri\\vedem.txt")


      // Make predictions on a test dataset
      val predictions = modelDT.transform(dataset)
      println("predictions")
      predictions.show()

      val predictionsWithDoubleLabels = predictions.withColumn("ground_truth", col("ground_truth").cast("Double")).withColumn("Prediction", col("Prediction").cast("Double"))


      // Assuming your label column is named "label" and the prediction column is named "prediction"
      val evaluatorAccuracy = new BinaryClassificationEvaluator()
        .setLabelCol("ground_truth")
        .setRawPredictionCol("Prediction")
        .setMetricName("areaUnderROC")
      println(evaluatorAccuracy.evaluate(predictionsWithDoubleLabels).toString)

      val evaluatorPrecision = new BinaryClassificationEvaluator()
        .setLabelCol("ground_truth")
        .setRawPredictionCol("Prediction")
        .setMetricName("areaUnderPR")
      println(evaluatorPrecision.evaluate(predictionsWithDoubleLabels).toString)

      val evaluatorPrecision = new BinaryClassificationEvaluator()
        .setLabelCol("ground_truth")
        .setRawPredictionCol("Prediction")
        .setMetricName("precision")

      val evaluatorRecall = new BinaryClassificationEvaluator()
        .setLabelCol("ground_truth")
        .setRawPredictionCol("Prediction")
        .setMetricName("recall")


      val pipeline3 = new Pipeline().setStages(Array(vectorExpander, customDecisionTree))

      // Fit the pipeline to the data
      val fittedPipelineModel3 = pipeline3.fit(resultDF2)

      // Transform the DataFrame
      val resultDF3 = fittedPipelineModel3.transform(resultDF2)
      resultDF3.show()
      */

    } else {

      /* EXECUTE ON CLUSTER */

      spark = SparkSession.builder()
        .appName("Fake News Classification")
        .getOrCreate()

      inputPath = args(0)
      outputPath = args(1)
      what = args(2)
      defaultMaxVocabSize = 500
      maxVocabSizeCV = Try(args(3).toInt).getOrElse(defaultMaxVocabSize)



      // RICORDARSI DI SETTARE HADOOP CONFIURATION PER LEGGERE E SCRIVERE DIRETTAMENTE DA GCS
      val keyfileName = "dogwood-mission-408515-3c37fd13e43f.json"

      val hadoopConf = spark.sparkContext.hadoopConfiguration


      val distributedFilePath = "/dogwood-mission-408515-3c37fd13e43f.json"

      // Set your Hadoop Configuration with GCS credentials
      hadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      hadoopConf.set("google.cloud.auth.service.account.enable", "true")
      hadoopConf.set("google.cloud.auth.service.account.json.keyfile", distributedFilePath)
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



      println("Start")
      val downloadPath: String = "data/download"
      val datasetPath = "data/dataset"
      val csv = args(4)

      // Creating a list of strings
      // "jruvika/fake-news-detection",
      val kaggleDatasetList: List[String] = List("therohk/million-headlines", "clmentbisaillon/fake-and-real-news-dataset", "mrisdal/fake-news", "hassanamin/textdb3", "saurabhshahane/fake-news-classification", "stevenpeutz/misinformation-fake-news-text-dataset-79k", "jainpooja/fake-news-detection", "niranjanank/fake-news-log", "mohammadaflahkhan/fake-news-dataset-combined-different-sources")
      // Create a Map
      // "fake-news-detection" -> "data.csv",
      val csvPerDataset: Map[String, String] = Map("million-headlines" -> "abcnews-date-text.csv", "fake-and-real-news-dataset" -> "Fake.csv", "fake-news" -> "fake.csv", "textdb3" -> "fake_or_real_news.csv", "fake-news-classification" -> "WELFake_Dataset.csv", "misinformation-fake-news-text-dataset-79k" -> "DataSet_Misinfo_FAKE.csv", "fake-news-detection" -> "Fake.csv", "fake-news-log" -> "train.csv", "fake-news-dataset-combined-different-sources" -> "PreProcessedData.csv")
      // , "fake-news-detection" -> "Headline"
      val columnsMap: Map[String, String] = Map("million-headlines" -> "headline_text", "fake-and-real-news-dataset" -> "title", "fake-news" -> "title", "textdb3" -> "title", "fake-news-classification" -> "title", "misinformation-fake-news-text-dataset-79k" -> "text", "fake-news-detection" -> "title", "fake-news-log" -> "title", "fake-news-dataset-combined-different-sources" -> "title")
      val textColumn: String = "title"

      val isDatasetPresent: Boolean = true

      // If the dataset isn't created, load the dataset and save it
      if (!isDatasetPresent) {
        // INSERIRE IL PROCESSO DI TRY DA ARGS(3), GESTIRE SIA SUCCESS CHE FAILURE (FATTO SOPRA)
        val dataAcquisition: DataAcquisition = new DataAcquisition(kaggleDatasetList, csvPerDataset, columnsMap, textColumn, s"$inputPath/$downloadPath", s"$inputPath/$datasetPath", csv, maxVocabSizeCV, spark)
        dataset = dataAcquisition.loadDataset()
        println("Dataset loaded successfully!")
      }
      else {

        dataset = spark.read
          .option("header", "true")
          .option("quote", "\"") // Quote character
          .option("escape", "\"") // Quote escape character (end of quote)
          .option("multiLine", "true")
          .option("sep", ",")
          .option("charset", "UTF-8")
          .csv(s"$inputPath/$datasetPath/$csv")


        var schema: Seq[StructField] = Seq()
        dataset.columns.foreach {
          col =>
            if (col.equals("ground_truth") || col.equals("Index"))
              schema = schema :+ StructField(col, IntegerType, nullable = true)
            else
              schema = schema :+ StructField(col, DoubleType, nullable = true)
        }

        dataset = spark.read
          .option("header", "true")
          .option("quote", "\"") // Quote character
          .option("escape", "\"") // Quote escape character (end of quote)
          .option("multiLine", "true")
          .option("sep", ",")
          .option("charset", "UTF-8")
          .schema(StructType(schema))
          .csv(s"$inputPath/$datasetPath/$csv")

        println("NUM PARTITIONS: " + dataset.rdd.partitions.length.toString)
        println("fatto")

        dataset.show()

      }
    }


    println("DATASET COUNT", dataset.count())


    var lastColumns: Array[String] = null
    var trainSet: DataFrame = null
    var testSet: DataFrame = null


    if (testLocal) {

      dataset = spark.read
        .option("header", "true")
        .option("quote", "\"") // Quote character
        .option("escape", "\"") // Quote escape character (end of quote)
        .option("multiLine", "true")
        .option("sep", ",")
        .option("charset", "UTF-8")
        .csv("/Users/andreamancini/IdeaProjects/FakeNewsClassificationWithDecisionTreeMR/dataset100k.csv")


      var schema: Seq[StructField] = Seq()
      dataset.columns.foreach {
        col =>
          if (col.equals("ground_truth") || col.equals("Index"))
            schema = schema :+ StructField(col, IntegerType, nullable = true)
          else
            schema = schema :+ StructField(col, DoubleType, nullable = true)
      }

      dataset = spark.read
        .option("header", "true")
        .option("quote", "\"") // Quote character
        .option("escape", "\"") // Quote escape character (end of quote)
        .option("multiLine", "true")
        .option("sep", ",")
        .option("charset", "UTF-8")
        .schema(StructType(schema))
        .csv("/Users/andreamancini/IdeaProjects/FakeNewsClassificationWithDecisionTreeMR/dataset100k.csv")


      /* TRAINSET IN TEST MODE - è uguale al dataset (nessuno split) */


      if( num_rows > 0) {

        dataset = dataset.limit(num_rows)

      }

      if (num_cols > 0) {
        lastColumns = dataset.columns.take(num_cols) ++ dataset.columns.takeRight(1)

        dataset = dataset.select(lastColumns.map(col): _*)
      }

      /*
      val dfWithConsecutiveIndex = trainSet.withColumn("Index", row_number().over(Window.orderBy(monotonically_increasing_id())))
      val orderedColumns : Array[String] = "Index" +: trainSet.columns.map(col => col)
      trainSet = dfWithConsecutiveIndex.select(orderedColumns.map(col): _*)*/


    }


    /* TRAINSET IN CLUSTER MODE */

    // Dividi il DataFrame in due parti: una con label 0 e una con label 1
    val dfLabel0 = dataset.filter(col("ground_truth") === 0)
    val dfLabel1 = dataset.filter(col("ground_truth") === 1)

    val label0_count = dfLabel0.count().toInt
    val label1_count = dfLabel1.count().toInt

    val minCount = label0_count.min(label1_count)
    // Prendi un campione bilanciato per ciascuna label
    val trainLabel0 = dfLabel0.sample(withReplacement = false, minCount * 0.8 / label0_count)
    val trainLabel1 = dfLabel1.sample(withReplacement = false, minCount * 0.8 / label1_count)

    // Unisci i due campioni per ottenere il set di addestramento bilanciato
    trainSet = trainLabel0.unionAll(trainLabel1)

    // Rimuovi le righe utilizzate per l'addestramento dal DataFrame originale
    testSet = dataset.exceptAll(trainSet)


    //val dataPreparation: MapReduceAlgorithm = new MapReduceAlgorithm()

    // DA PROVARE
    val decisionTreeMaxDepth = 100
    val customDecisionTree = new DecisionTreeClassifier()
      .setMaxDepth(decisionTreeMaxDepth)

    // Assuming you have a DataFrame called "trainingData" with columns "feature1", "feature2", and "label"
    val decTree = customDecisionTree.fit(trainSet.cache())

    decTree.getDecisionTree.printAllNodes(decTree.getDecisionTree.getTreeNode)
    decTree.getDecisionTree.saveToFile("./tree.ser")

    if (testSet != null) {
      val predictedLabels = decTree.transform(testSet)

      val predictionsWithDoubleLabels = predictedLabels.withColumn("ground_truth", col("ground_truth").cast("Double")).withColumn("Prediction", col("Prediction").cast("Double"))
      val evaluatorAccuracy = new BinaryClassificationEvaluator()
        .setLabelCol("ground_truth")
        .setRawPredictionCol("Prediction")
        .setMetricName("areaUnderROC")
      println(evaluatorAccuracy.evaluate(predictionsWithDoubleLabels).toString)

      val evaluatorPrecision = new BinaryClassificationEvaluator()
        .setLabelCol("ground_truth")
        .setRawPredictionCol("Prediction")
        .setMetricName("areaUnderPR")
      println(evaluatorPrecision.evaluate(predictionsWithDoubleLabels).toString)
    }

    if(testLocal) {
      var userInput = ""

      // Keep waiting for input until the user enters "exit"
      while (userInput.toLowerCase != "exit") {
        userInput = scala.io.StdIn.readLine()
      }
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


}