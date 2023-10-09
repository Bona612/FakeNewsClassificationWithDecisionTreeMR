import java.io.File
import org.apache.hadoop.conf.Configuration


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime
import dataacquisition.DataAcquisition
import java.nio.file.{Files, Path, Paths}
import scala.language.postfixOps
import scala.sys.process._

object MainApp {

  final val spark: SparkSession = SparkSession.builder()
                                  .master("local[*]")
                                  .appName("Fake News Classification")
                                  .getOrCreate()



  def main(args: Array[String]): Unit = {
    println("Start")
    val downloadPath: String = "./data/download"
    val datasetPath = "./data/dataset"
    val csv = "dataset.csv"

    // Creating a list of strings
    val kaggleDatasetList: List[String] = List("therohk/million-headlines", "clmentbisaillon/fake-and-real-news-dataset", "mrisdal/fake-news")
    // Create a Map
    val csvPerDataset: Map[String, String] = Map("million-headlines" -> "abcnews-date-text.csv", "fake-and-real-news-dataset" -> "Fake.csv", "fake-news" -> "fake.csv")
    val columnsMap: Map[String, String] = Map("million-headlines" -> "headline_text", "fake-and-real-news-dataset" -> "title", "fake-news" -> "title")
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

    // If the dataset isn't created, load the dataset and save it
    if (!isDatasetPresent) {
      val dataAcquisition: DataAcquisition = new DataAcquisition(kaggleDatasetList, csvPerDataset, columnsMap, textColumn, downloadPath, datasetPath, csv, spark)
      dataAcquisition.loadDataset()
      println("Dataset loaded succesfully!")
    }

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