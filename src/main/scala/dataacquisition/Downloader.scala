package dataacquisition

import org.apache.spark.sql.SparkSession

import scala.language.postfixOps
import scala.sys.process._
import dataacquisition.Unzipper

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.security.CodeSource

class Downloader(val kaggleDataset: String, val csvPerDataset: Map[String, String], val downloadPath: String, val spark: SparkSession) {

  // Set the path to your Kaggle API token file
  final val kaggleApiTokenPath = "./token/kaggle.json"

  def getCurrentDirectory() : String = {
    val codeSource: CodeSource = getClass.getProtectionDomain.getCodeSource
    val jarFileLocation = if (codeSource != null) codeSource.getLocation.toURI.getPath else ""
    val absolutePath = new java.io.File(jarFileLocation).getParentFile.getAbsolutePath
    absolutePath
  }

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

    val currentDir = getCurrentDirectory()
    // val currentDir = new File(".").getCanonicalPath

    val test = s"hadoop dfs -test -d hdfs:///user/fnc_user/download/$kaggleDatasetName/" // + csvPerDataset(kaggleDatasetName)
    val dirIsPresent = test !

    println("download già fatto: " + dirIsPresent.toString)
    if (dirIsPresent != 0) {

      // Use the Kaggle API to download the dataset
      val command = s"kaggle datasets download -d $kaggleDataset -p $currentDir --force"
      val exitCode = command !

      // Check the exit code to see if the command was successful
      if (exitCode == 0) {
        println("Dataset downloaded successfully!")


        println(s"$currentDir/$kaggleDatasetName/")
        val unzipper = new Unzipper(s"$currentDir/$kaggleDatasetName.zip", s"$currentDir/$kaggleDatasetName/", spark)
        unzipper.unzip()


        /*val downloadDirCommand = s"hdfs dfs -mkdir hdfs:///user/fnc_user/download/$kaggleDatasetName"
        val downloadDirCommandExitCode = downloadDirCommand !

        println("hadoop dir dataset creation exit code: " + downloadDirCommandExitCode)*/

        //Copy files from local file system to HDFS
        val command = s"hdfs dfs -copyFromLocal $currentDir/$kaggleDatasetName hdfs:///user/fnc_user/download"
        val exitCode2 = command !


        println("hdfs exit code: " + exitCode2)

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


    ""
  }

}
