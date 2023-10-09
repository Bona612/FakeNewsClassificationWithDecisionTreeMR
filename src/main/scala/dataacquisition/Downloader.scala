package dataacquisition

import org.apache.spark.sql.SparkSession

import scala.language.postfixOps
import scala.sys.process._
import dataacquisition.Unzipper

import java.nio.file.{Files, Path, Paths}

class Downloader(val kaggleDataset: String, val csvPerDataset: Map[String, String], val downloadPath: String, val spark: SparkSession) {

  // Set the path to your Kaggle API token file
  final val kaggleApiTokenPath = "./token/kaggle.json"

  def downloadDataset(): String = {
    var kaggleDatasetName = ""

    // Find the index of the character in the string
    val i = kaggleDataset.indexOf('/')

    // If the character is found, get the substring from that index to the end
    if (i != -1) {
      kaggleDatasetName = kaggleDataset.substring(i + 1) // kaggleDataset.length
      println(s"Original: $kaggleDataset, Character Found: '/', Substring: $kaggleDatasetName")
    }
    else {
      println(s"Original: $kaggleDataset, Character '/' not found.")
    }


    // Use the Kaggle API to download the dataset
    val command = s"kaggle datasets download -d $kaggleDataset -p $downloadPath --force"
    val exitCode = command !

    // Check the exit code to see if the command was successful
    if (exitCode == 0) {
      println("Dataset downloaded successfully!")

      println(s"$downloadPath/$kaggleDatasetName/")
      val unzipper = new Unzipper(s"$downloadPath/$kaggleDatasetName.zip", s"$downloadPath/$kaggleDatasetName/", spark)
      unzipper.unzip()
      println("Dataset unzipped successfully!")

      // Use the Files.list method to get a Stream of paths in the directory
      val directoryStream = Files.list(Paths.get(s"$downloadPath/$kaggleDatasetName"))

      var csv = ""

      // Convert the Stream to a Scala List and print the file names
      val fileList = directoryStream.toArray
      fileList.foreach { path =>
        if (path.asInstanceOf[Path].getFileName.toString == csvPerDataset(kaggleDatasetName)) {
          csv = path.asInstanceOf[Path].getFileName.toString
        }
      }

      // Close the directory stream
      directoryStream.close()

      println("Downloader finished!")

      // s"$downloadPath/$kaggleDatasetName.csv"
      csv
    }
    else {
      println(s"Error downloading dataset. Exit code: $exitCode")
      s""
    }

  }

}
