import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}
import dataacquisition.DataAcquisition
import decisiontreealg.DataPreparation

import java.nio.file.{Files, Path, Paths}
import scala.language.postfixOps
import scala.sys.process._

object MainApp {

  final val spark: SparkSession = SparkSession.builder()
                                  .master("local[*]")
                                  .appName("Fake News Classification")
                                  .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.0")
                                  .getOrCreate()



  def main(args: Array[String]): Unit = {
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
    */
    /*
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
    val directoryStream = Files.list(Paths.get(datasetPath))

    var isDatasetPresent: Boolean = false

    // Convert the Stream to a Scala List and print the file names
    val fileList = directoryStream.toArray
    fileList.foreach { path =>
      if (path.asInstanceOf[Path].toString == Paths.get(datasetPath).resolve(csv).toString) {
        isDatasetPresent = true
      }
    }

    // Close the directory stream
    directoryStream.close()

    var dataset: DataFrame = null
    // If the dataset isn't created, load the dataset and save it
    if (!isDatasetPresent) {
      val dataAcquisition: DataAcquisition = new DataAcquisition(kaggleDatasetList, csvPerDataset, columnsMap, textColumn, downloadPath, datasetPath, csv, spark)
      dataset = dataAcquisition.loadDataset()
      println("Dataset loaded succesfully!")
    }

    val dataPreparation: DataPreparation = new DataPreparation(dataset)
    dataPreparation.createAttribTable()
    /*
    println("Read dataset!")
    // Read the dataset
    val dataset = spark.read.format("csv").option("header", "true").load(Paths.get(datasetPath).resolve(csv).toString)

    println("Show dataset")
    dataset.show()
    */

    println("Stopping Spark")
    // Stop the Spark session
    spark.stop()
    println("Stop")
  }
}