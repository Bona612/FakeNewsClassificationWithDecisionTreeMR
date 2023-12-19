package utils

import java.io.InputStream
import java.io.OutputStream
import java.nio.file.{Files, Path, Paths}
import java.net.URI
import com.google.cloud.storage.{Blob, BlobId, BlobInfo, Storage, StorageOptions}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark
import org.apache.spark.sql.SparkSession

import java.io.ByteArrayInputStream
import java.nio.channels.Channels
import java.io.FileInputStream
import java.io.File
import java.io.FileOutputStream
import scala.language.postfixOps
import scala.sys.process._

/**
 * Utility  functions for accessing the google cloud bucket
 */
object GCSUtils {

  val storage = StorageOptions.getDefaultInstance().getService()

  def getFile(keyfileGCSPath: String, keyfileLocalPath: String, spark:SparkSession): Unit = {
    // da sistemare
    val projectId = "spring-cab-402321"
    val bucketNameGCS = "fnc_bucket_final"

    println("STORAGE: " + storage.toString)

    //val blob = storage.get(bucketNameGCS, keyfileGCSPath)
    // Download the blob to a local file
    //blob.downloadTo(Paths.get(keyfileLocalPath))
    /*
    val keyfileContent = new String(blob.getContent())
    // Write the key file content to the local file system
    Files.write(Paths.get(keyfileLocalPath), keyfileContent.getBytes)
    */


    try {
      // Create a Storage object
      val storage: Storage = StorageOptions.getDefaultInstance.getService

      // Get the Blob (file) from GCS
      val blob: Blob = storage.get(bucketNameGCS, keyfileGCSPath)

      // Download the content of the Blob
      val fileContent: Array[Byte] = blob.getContent()

      // Save the content to HDFS
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val os = fs.create(new org.apache.hadoop.fs.Path("hdfs:///user/prefab-bruin-402414-a2db7e809915.json"))
      os.write(fileContent)
      os.close()

    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  def isFilePresent(fileGCSPath: String, spark: SparkSession): Boolean = {
    val bucketNameGCS = "fnc_bucket_final"
    val GCSPath = s"gs://$bucketNameGCS/$fileGCSPath"

    try {
      // Create a Hadoop Configuration
      val hadoopConf = spark.sparkContext.hadoopConfiguration

      // Get the FileSystem for GCS
      val fs = FileSystem.get(new java.net.URI(GCSPath), hadoopConf)

      // Check if the file exists
      fs.exists(new org.apache.hadoop.fs.Path(GCSPath))
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        false
    }
  }


  def saveFile(outputPathGCS: String, stringFilePath: String): BlobInfo = {
    // ANCHE QUI NON RICORDO BENE COSA AVESSI FATTO, DARE UN OCCHIO SU CHAT-GPT

    // da sistemare
    val projectId = "prefab-bruin-402414"
    val bucketName = "fnc_bucket_prova2"

    println(storage)
    //val storage: Storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService

    val blobId = BlobId.of(bucketName, outputPathGCS)
    val blobInfo = BlobInfo.newBuilder(blobId).build()


    println(stringFilePath)
    val test = s"hdfs dfs -test -d $stringFilePath" // + csvPerDataset(kaggleDatasetName)
    val dirIsPresent = test !

    println("file finale present: " + dirIsPresent.toString)
    if (dirIsPresent == 0) {
      println("BENEEEEEEEEEEE :)")
    }
    else {
      println("no bene :(")
    }
    val path: Path = Paths.get(stringFilePath)

    // Upload the file to GCS
    val blob: Blob = storage.create(blobInfo, java.nio.file.Files.readAllBytes(path))

    blob
  }



}
