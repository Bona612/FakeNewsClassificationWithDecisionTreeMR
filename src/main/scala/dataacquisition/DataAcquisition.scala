package dataacquisition

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, expr}

import java.nio.file.Paths
import scala.collection.mutable.ListBuffer


class DataAcquisition(datasetList: List[String], csvPerDataset: Map[String, String], columnsMap: Map[String, String], textColumn: String, downloadPath: String, datasetPath: String, csv: String, spark: SparkSession) {

  def loadDataset(): Unit = {

    var datasetPathList: ListBuffer[String] = ListBuffer()

    datasetList.foreach { dataset: String =>
      // Creating an instance of MyClass inside AnotherObject
      val downloader = new Downloader(dataset, csvPerDataset, downloadPath, spark)

      // Calling a method on the created instance
      val datasetPath: String = downloader.downloadDataset()
      datasetPathList += datasetPath
    }

    // Combine the two lists using zip
    //val keyValuePairs: ListBuffer[(String, String)] = datasetPathList.zip(columnsMap)

    // Convert the list of pairs to a map
    //val datasetMap: Map[String, String] = keyValuePairs.toMap

    println("Final Dataset creation started!")

    var finalDataset: DataFrame = null

    for((dataset, column) <- columnsMap) {
      println(csvPerDataset(dataset))
      println(csvPerDataset.get(dataset))
      println(Paths.get(downloadPath).resolve(dataset).resolve(csvPerDataset(dataset)).toString)
      // Your Scala code to read the downloaded dataset
      var df: DataFrame = spark.read.option("header", "true").csv(Paths.get(downloadPath).resolve(dataset).resolve(csvPerDataset(dataset)).toString)
      println(df.count())
      // Get column names
      val columnNames: Array[String] = df.columns

      // Print column names
      columnNames.foreach { colName =>
        if (colName == "language") {
          // Select rows where the value
          df = df.filter(col(colName) === "english")
        }
      }


      // Select only specific columns
      val selectedColumns: Array[String] = Array(column)
      df = df.select(selectedColumns.map(col): _*)

      // Remove rows with missing values
      df = df.na.drop(selectedColumns)
      println(df.count())
      // Drop duplicates based on all columns
      df = df.dropDuplicates(selectedColumns)
      println(df.count())
      // Rename columns
      val renamedColumns: Map[String, String] = Map(column -> textColumn)

      // Check if a column is present
      val isColumnPresent: Boolean = df.columns.contains(textColumn)

      for ((oldName, newName) <- renamedColumns) {
        if (!isColumnPresent) {
          df = df.withColumnRenamed(oldName, newName)
        }
      }

      if (dataset == "million-headlines") {
        // Add a new column
        df = df.withColumn("label", expr("0"))
      }
      else {
        // Add a new column
        df = df.withColumn("label", expr("1"))
      }


      val optionDataset: Option[DataFrame] = Option(finalDataset)
      if (optionDataset.isEmpty) {
        // Deep copy of the DataFrame
        finalDataset = df.select(df.columns.map(col): _*)
      }
      else {
        println(df.count())
        // Concatenate DataFrames vertically
        finalDataset = finalDataset.union(df)
      }
    }

    println("Final Dataset creation finished!")

    finalDataset.show()
    println(finalDataset.count())

    // Count the number of rows with the specific value in the specified column
    val rowCount: Long = finalDataset.filter(col("label") === "1").count()
    println(rowCount)

    // Specify the path where you want to write the CSV file
    val outputPath = Paths.get(datasetPath).resolve(csv).toString //
    /*
    // Write the DataFrame to CSV
    finalDataset.write
      .option("header", "true") // Write header
      .csv(outputPath)

    finalDataset.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save(outputPath)
*/
    /*
    datasetPathList.foreach { datasetPath: String =>
      // Your Scala code to read the downloaded dataset
      var df = spark.read.option("header", "true").csv(datasetPath)

      // Select only specific columns
      val selectedColumns: Array[String] = Array("")
      df = df.select(selectedColumns.map(col): _*)

      // Remove rows with missing values
      df = df.na.drop()

      // Drop duplicates based on all columns
      df = df.dropDuplicates("")

      // Rename columns
      val renamedColumns: Map[String, String] = Map("" -> "")

      for ((oldName, newName) <- renamedColumns) {
        df = df.withColumnRenamed(oldName, newName)
      }

      // Add a new column "City" with constant value "New York"
      df = df.withColumn("label", expr("0")) // 1

      // Concatenate DataFrames vertically
      df = df.union(df)

      // Show the first few rows of the DataFrame
      df.show()
    }*/

  }

}