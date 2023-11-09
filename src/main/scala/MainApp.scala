import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}
import dataacquisition.DataAcquisition
import decisiontreealg.MapReduceAlgorithm

import java.nio.file.{Files, Path, Paths}
import scala.language.postfixOps
import scala.sys.process._
import decisiontree.DecisionTree
import decisiontree.Node

object MainApp {

  final val spark: SparkSession = SparkSession.builder()
                                  .master("local[*]")
                                  .appName("Fake News Classification")
                                  .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.0")
                                  .getOrCreate()



  def main(args: Array[String]): Unit = {

    val decisionTreePath = "/Users/luca/Desktop/tree.txt"

    //val tree = DecisionTree.fromFile(decisionTreePath)

    //var directoryStream = Files.list(Paths.get(decisionTreePath))
    //var fileList = directoryStream.toArray

    val currentDir = new File(".").getCanonicalPath
    println(s"currentDir $currentDir")

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
    val downloadPath: String = "./data/download"
    val datasetPath = "./data/dataset"
    val csv = "dataset.csv"

    /*
    // Creating a list of strings
    val kaggleDatasetList: List[String] = List("hassanamin/textdb3")
    // Create a Map
    val csvPerDataset: Map[String, String] = Map("textdb3" -> "fake_or_real_news.csv")
    val columnsMap: Map[String, String] = Map("textdb3" -> "title")

    // Creating a list of strings
    val kaggleDatasetList: List[String] = List("therohk/million-headlines", "clmentbisaillon/fake-and-real-news-dataset", "mrisdal/fake-news", "hassanamin/textdb3", "jruvika/fake-news-detection", "saurabhshahane/fake-news-classification", "stevenpeutz/misinformation-fake-news-text-dataset-79k", "jainpooja/fake-news-detection", "niranjanank/fake-news-log", "mohammadaflahkhan/fake-news-dataset-combined-different-sources")
    // Create a Map
    val csvPerDataset: Map[String, String] = Map("million-headlines" -> "abcnews-date-text.csv", "fake-and-real-news-dataset" -> "Fake.csv", "fake-news" -> "fake.csv", "textdb3" -> "fake_or_real_news.csv", "fake-news-detection" -> "data.csv", "fake-news-classification" -> "WELFake_Dataset.csv", "misinformation-fake-news-text-dataset-79k" -> "DataSet_Misinfo_FAKE.csv", "fake-news-detection" -> "Fake.csv", "fake-news-log" -> "train.csv", "fake-news-dataset-combined-different-sources" -> "PreProcessedData.csv")
    val columnsMap: Map[String, String] = Map("million-headlines" -> "headline_text", "fake-and-real-news-dataset" -> "title", "fake-news" -> "title", "textdb3" -> "title", "fake-news-detection" -> "Headline", "fake-news-classification" -> "title", "misinformation-fake-news-text-dataset-79k" -> "text", "fake-news-detection" -> "title", "fake-news-log" -> "title", "fake-news-dataset-combined-different-sources" -> "title")
    val textColumn: String = "title"

    */
    // Creating a list of strings
    val kaggleDatasetList: List[String] = List("mrisdal/fake-news")
    // Create a Map
    val csvPerDataset: Map[String, String] = Map("fake-news" -> "fake.csv")
    val columnsMap: Map[String, String] = Map("fake-news" -> "title")
    val textColumn: String = "title"


    // Use the Files.list method to get a Stream of paths in the directory
    //directoryStream = Files.list(Paths.get(datasetPath))

    var isDatasetPresent: Boolean = false
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
      val dataAcquisition: DataAcquisition = new DataAcquisition(kaggleDatasetList, csvPerDataset, columnsMap, textColumn, downloadPath, datasetPath, csv, spark)
      dataset = dataAcquisition.loadDataset()
      println("Dataset loaded succesfully!")
    }
    print(dataset)

    val dataPreparation: MapReduceAlgorithm = new MapReduceAlgorithm(dataset)
    val decTree = dataPreparation.initAlgorithm()
    decTree.asInstanceOf[Node].writeRulesToFile("/Users/luca/Desktop/treeOutput.txt")

    val predictedLabels = decTree.predict(dataset,decTree)

    println("predictedLabels")
    println(predictedLabels.mkString("Array(", ", ", ")"))
    val tmp : Array[Any] = dataset.select("label").collect().map(row => row(0))
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

  /*
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
  }*/





}