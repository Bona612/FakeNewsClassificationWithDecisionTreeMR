package dataacquisition

import org.apache.spark.sql.SparkSession

import java.io.{BufferedOutputStream, FileInputStream, FileOutputStream}
import java.nio.file.{Files, Paths}
import java.util.zip.ZipInputStream

class Unzipper(val zipPath: String, val extractDir: String, val spark: SparkSession)  {

  def unzip(): Unit = {

    // Create the directory
    try {
      Files.createDirectories(Paths.get(extractDir))
      println(s"Directory '$extractDir' created successfully.")
    } catch {
      case e: Exception =>
        println(s"Error creating directory: ${e.getMessage}")
    }

    // Create a buffer for reading the ZIP file
    val buffer = new Array[Byte](1024)

    // Create a ZipInputStream to read the ZIP file
    val zipInputStream = new ZipInputStream(new FileInputStream(zipPath))

    // Iterate over entries in the ZIP file
    var zipEntry = zipInputStream.getNextEntry
    while (zipEntry != null) {
      // Specify the path for the new file
      val newFilePath = extractDir + java.io.File.separator + zipEntry.getName

      // If the entry is a directory, create the directory
      if (zipEntry.isDirectory) {
        new java.io.File(newFilePath).mkdirs()
      } else {
        // If the entry is a file, extract it
        val outputStream = new BufferedOutputStream(new FileOutputStream(newFilePath))
        var len = zipInputStream.read(buffer)
        while (len > 0) {
          outputStream.write(buffer, 0, len)
          len = zipInputStream.read(buffer)
        }
        outputStream.close()
      }

      // Move to the next entry
      zipEntry = zipInputStream.getNextEntry
    }

    // Close the ZipInputStream
    zipInputStream.closeEntry()
    zipInputStream.close()

    println(s"File '$zipPath' successfully extracted to '$extractDir'")

  }

}
