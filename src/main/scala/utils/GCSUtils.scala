package utils

import java.io.InputStream
import java.io.OutputStream
import java.nio.file.{Files, Path, Paths}
import java.net.URI
import com.google.cloud.storage.{Blob, BlobId, BlobInfo, Storage, StorageOptions}
import org.apache.hadoop.fs.FileSystem
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

  def getFile(keyfileGCSPath: String, keyfileLocalPath: String): Unit = {
    // da sistemare
    val projectId = "spring-cab-402321"
    val bucketNameGCS = "fnc_bucket_final"

    println("STORAGE: " + storage.toString)

    val blob = storage.get(bucketNameGCS, keyfileGCSPath)
    // Download the blob to a local file
    blob.downloadTo(Paths.get(keyfileLocalPath))
    /*
    val keyfileContent = new String(blob.getContent())
    // Write the key file content to the local file system
    Files.write(Paths.get(keyfileLocalPath), keyfileContent.getBytes)
    */
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

  /**
   * Returns an input stream of the provided path
   *
   * @param stringPath file path
   * @return file input stream
   */
  def getInputStream(stringPath: String): InputStream = {
    val isGS = stringPath.size > 5 && stringPath.indexOf("gs://") == 0
    if(isGS)
      new ByteArrayInputStream(storage.readAllBytes(getBlobIdFromPath(stringPath)))
    else
      new FileInputStream(new File(stringPath))
  }

  /**
   * Returns an output stream of the provided path
   *
   * @param stringPath file path
   * @return file output stream
   */
  def getOutputStream(stringPath: String): OutputStream = {
    val isGS = stringPath.size > 5 && stringPath.indexOf("gs://") == 0
    if(isGS) {
      val blobId = getBlobIdFromPath(stringPath)
      val blobInfo = BlobInfo.newBuilder(blobId).build()
      val blob = storage.create(blobInfo)
      Channels.newOutputStream(blob.writer())
    }
    else {
      val file = new File(stringPath)
      file.createNewFile()
      new FileOutputStream(file, false)
    }
  }

  /**
   * Get a blob from a given path
   *
   * @param stringPath file path
   * @return blobId
   */
  private def getBlobIdFromPath(stringPath: String) : BlobId = {
    val bucket = stringPath.split("/")(2)
    val file = stringPath.split(s"gs://${bucket}/")(1)
    BlobId.of(bucket, file)
  }


}
