package dataacquisition

import com.johnsnowlabs.nlp.DocumentAssembler
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}

import java.nio.file.{Files, Paths}
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StopWordsRemover
import com.johnsnowlabs.nlp.annotator.{LemmatizerModel, Stemmer, StopWordsCleaner, Tokenizer}
//import com.johnsnowlabs.nlp.annotators.EnglishStemmer
import org.apache.spark.sql.catalyst.ScalaReflection.universe.typeOf
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.{CountVectorizer, IDF}

import java.io.File
import java.security.CodeSource
import scala.sys.process._


class DataAcquisition(datasetList: List[String], csvPerDataset: Map[String, String], columnsMap: Map[String, String], textColumn: String, downloadPath: String, datasetPath: String, csv: String, spark: SparkSession) {

  def getCurrentDirectory(): String = {
    val codeSource: CodeSource = getClass.getProtectionDomain.getCodeSource
    val jarFileLocation = if (codeSource != null) codeSource.getLocation.toURI.getPath else ""
    val absolutePath = new java.io.File(jarFileLocation).getParentFile.getAbsolutePath
    absolutePath
  }

  def loadDataset(): DataFrame = {

    var datasetPathList: ListBuffer[String] = ListBuffer()

    /*var cd = "hadoop dfs -ls hdfs://" + getCurrentDirectory()
    var cdExitCode = cd !

    println("cuurent dir exit code: " + cdExitCode)*/
    var cd = "hadoop dfs -ls hdfs:///user/bocca"
    var cdExitCode = cd !

    println("tmp- cuurent dir exit code: " + cdExitCode)
    /*cd = "hadoop dfs -ls hdfs:///tmp"
    cdExitCode = cd !

    println("bocca- cuurent dir exit code: " + cdExitCode)
    cd = "hadoop dfs -ls hdfs:///user/root"
    cdExitCode = cd !

    println("root- cuurent dir exit code: " + cdExitCode)
    cd = "hadoop dfs -ls hdfs:///user/dataproc"
    cdExitCode = cd !

    println("dataproc- cuurent dir exit code: " + cdExitCode)*/


    val downloadDirCommand = "hadoop fs -mkdir hdfs:///user/bocca/download"
    val downloadDirCommandExitCode = downloadDirCommand !

    println("hadoop dir dataset creation exit code: " + downloadDirCommandExitCode)

    datasetList.foreach { dataset: String =>
      // Creating an instance of MyClass inside AnotherObject
      val downloader = new Downloader(dataset, csvPerDataset, ".", spark)

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

    // VERSIONE DISTRIBUITA
    val datasetFrames: Map[String, DataFrame] = columnsMap.map {
      case (datasetName, columnName) =>
        val currentDir = getCurrentDirectory()
        println(currentDir)
        var datasetDF: DataFrame = spark.read.option("header", "true").option("escape","\"").option("multiLine","true").option("sep", ",").option("charset", "UTF-8").csv(s"hdfs:///user/bocca/download/$datasetName/" + csvPerDataset(datasetName))
        println("così va bene!!!")

        val columnNames: Array[String] = datasetDF.columns

        // Print column names
        columnNames.foreach { colName =>
          if (colName == "label" || colName == "Label" || colName == "Ground Label") {
            // Select rows where the value
            datasetDF = datasetDF.filter(col(colName) === "fake" || col(colName) === "FAKE" || col(colName) === 1)
          }

          if (colName == "language") {
            // Select rows where the value
            datasetDF = datasetDF.filter(col(colName) === "english")
          }
        }
        var textDF: DataFrame = null
        columnNames.foreach { colName =>
          // controllare sul nome del dataset
          if (colName == "text" || colName == "text") { // perchè due volte, uno è un typo?
            // Split the column and explode to create new rows
            val explodedDF = datasetDF.withColumn("split", split(col("text"), "\\."))
              .select(col("split"))

            var resultDF = explodedDF.select(explode(col("split")).alias("text"))
            resultDF = resultDF.withColumnRenamed("text", "title")
            // Add a new column
            textDF = resultDF.withColumn("label", expr("1"))
          }
        }

        // Select only specific columns
        val selectedColumns: Array[String] = Array(columnName)
        datasetDF = datasetDF.select(selectedColumns.map(col): _*)

        // CONTROLLARE CHE NON FACCIA CAZZATE!!!
        // Remove rows with missing values
        datasetDF = datasetDF.na.drop(selectedColumns)
        println(datasetDF.count())
        // Drop duplicates based on all columns
        datasetDF = datasetDF.dropDuplicates(selectedColumns)
        println(datasetDF.count())
        // Rename columns
        val renamedColumns: Map[String, String] = Map(columnName -> textColumn)

        // Check if a column is present
        val isColumnPresent: Boolean = datasetDF.columns.contains(textColumn)

        for ((oldName, newName) <- renamedColumns) {
          if (!isColumnPresent) {
            datasetDF = datasetDF.withColumnRenamed(oldName, newName)
          }
        }

        if (datasetName == "million-headlines") {
          // Add a new column
          datasetDF = datasetDF.withColumn("label", expr("0"))
        }
        else {
          // Add a new column
          datasetDF = datasetDF.withColumn("label", expr("1"))
        }


        // Union the DataFrames
        val unionedDF: DataFrame = textDF.union(datasetDF)

        //val selectedColumnDF = datasetDF.select(col(columnName))

        (datasetName, unionedDF)
    }
    val unionedDataset: DataFrame = datasetFrames.values.reduce(_ union _)
    unionedDataset.show()


    // SOPRA STO FACENDO LA VERSIONE DISTRIBUITA
    /*
    for((dataset, column) <- columnsMap) {
      println(csvPerDataset(dataset))
      println(csvPerDataset.get(dataset))
      println(Paths.get(downloadPath).resolve(dataset).resolve(csvPerDataset(dataset)).toString)


      // Your Scala code to read the downloaded dataset
      var df: DataFrame = spark.read.option("header", "true").option("escape","\"").option("multiLine","true").option("sep", ",").option("charset", "UTF-8").csv(Paths.get(downloadPath).resolve(dataset).resolve(csvPerDataset(dataset)).toString)
      println(df.count())
      // Get column names
      val columnNames: Array[String] = df.columns

      // Print column names
      columnNames.foreach { colName =>
        if (colName == "label" || colName == "Label" || colName == "Ground Label") {
          // Select rows where the value
          df = df.filter(col(colName) === "fake" || col(colName) === "FAKE" || col(colName) === 1)
        }

        if (colName == "language") {
          // Select rows where the value
          df = df.filter(col(colName) === "english")
        }
      }
      columnNames.foreach { colName =>
        if (colName == "text" || colName == "text") {
          // Split the column and explode to create new rows
          val explodedDF = df.withColumn("split", split(col("text"), "\\."))
            .select(col("split"))

          var resultDF = explodedDF.select(explode(col("split")).alias("text"))
          resultDF = resultDF.withColumnRenamed("text", "title")
          // Add a new column
          resultDF = resultDF.withColumn("label", expr("1"))

          println("COLUMNS")
          // Get column names
          val columnNames: Array[String] = resultDF.columns
          // Print column names
          columnNames.foreach { colName =>
            println(colName)
          }
          resultDF.show()

          val optionDataset: Option[DataFrame] = Option(finalDataset)
          if (optionDataset.isEmpty) {
            // Deep copy of the DataFrame
            finalDataset = resultDF.select(resultDF.columns.map(col): _*)
          }
          else {
            println(resultDF.count())
            // Concatenate DataFrames vertically
            finalDataset = finalDataset.union(resultDF)
          }
        }
      }


      // Select only specific columns
      val selectedColumns: Array[String] = Array(column)
      df = df.select(selectedColumns.map(col): _*)

      // CONTROLLARE CHE NON FACCIA CAZZATE!!!
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
    */

    // Read the JSONL file into a DataFrame
    /*var df_jsonl: DataFrame = spark.read.json("./data/data.jsonl")
    // Remove rows with missing values
    df_jsonl = df_jsonl.na.drop()
    println(df_jsonl.count())
    // Drop duplicates based on all columns
    df_jsonl = df_jsonl.dropDuplicates()
    println(df_jsonl.count())
    df_jsonl = df_jsonl.withColumn("label", expr("1"))
    // Concatenate DataFrames vertically
    finalDataset = finalDataset.union(df_jsonl)*/

    // Sample DataFrame
    val data_2 = Seq(
      Row("This is the first document.", 1),
      Row("This document is the second document.", 0),
      Row("And this is the third one.", 1),
      Row("Is this the first document?", 1)
    )
    // Define the schema for the DataFrame
    val schema_2 = StructType(Seq(
      StructField("title", StringType, true),
      StructField("label", IntegerType, true)
    ))
    finalDataset = spark.createDataFrame(spark.sparkContext.parallelize(data_2), schema_2)
    finalDataset = unionedDataset

    println("Final Dataset creation finished!")

    finalDataset.show()
    println(finalDataset.count())
    // Drop duplicates based on all columns
    finalDataset = finalDataset.dropDuplicates()
    println(finalDataset.count())

    // Count the number of rows with the specific value in the specified column
    val rowCount: Long = finalDataset.filter(col("label") === "1").count()
    println(rowCount)

    // Apply trim() function to remove leading and trailing whitespaces
    val dfTrimmed = finalDataset.withColumn("title", trim(col("title")))
    // Apply lower() function to convert text to lowercase
    val dfLower = dfTrimmed.withColumn("title", lower(col("title")))

    // Define a regular expression pattern to match Twitter user mentions
    val mentionPattern = "@[a-zA-Z0-9_]+"

    // Apply regexp_replace to remove Twitter user mentions
    val dfNoMentions = dfLower.withColumn("title", regexp_replace(col("title"), mentionPattern, ""))

    // Define a regular expression pattern to match URLs
    val urlPattern = """\b(?:https?|ftp|com):\/\/\S+"""

    // Apply regexp_replace to remove URLs
    val dfNoURLs = dfNoMentions.withColumn("title", regexp_replace(col("title"), urlPattern, ""))

    // Apply regexp_replace to remove newline characters
    val dfNoNewlines = dfNoURLs.withColumn("title", regexp_replace(col("title"), "\n", ""))

    // Apply regexp_replace to remove newline characters
    val dfNoTab = dfNoNewlines.withColumn("title", regexp_replace(col("title"), "\t", ""))

    // Apply regexp_replace to remove numbers
    val dfNoNumbers = dfNoTab.withColumn("title", regexp_replace(col("title"), "\\d+", ""))

    // Apply regexp_replace to remove consecutive whitespace characters
    val dfNoConsecutiveSpaces = dfNoNumbers.withColumn("title", regexp_replace(col("title"), "\\s+", " "))

    // Apply regexp_replace to remove punctuation
    val dfNoPunctuation = dfNoConsecutiveSpaces.withColumn("title", regexp_replace(col("title"), "[^a-zA-Z0-9\\s]", ""))

    // VEDERE SE TENERE QUI, O FARE TUTTO NEL PREPROCESSING
    val dfNotEmpty = dfNoPunctuation.filter(row => row.mkString("").nonEmpty)
    val dfOnlyWhitespace = dfNotEmpty.filter(row => !(row.mkString("").forall(_.isWhitespace)))

    //val dfWithTokens = dfOnlyWhitespace.withColumn("tokens", split(col("title"), "\\s+"))


    val documentAssembler = new DocumentAssembler()
      .setInputCol("title")
      .setOutputCol("document")

    // Tokenize the text into words
    val tokenizer = new Tokenizer()
      .setInputCols("document")
      .setOutputCol("tokens")
    //val tokenizedDF = tokenizer.transform(dfOnlyWhitespace)

    /*
    // Define StopWordsRemover
    val remover = new StopWordsRemover()
      .setInputCol("tokens")
      .setOutputCol("tokensNoStop")
    // Transform the DataFrame to remove stopwords
    //val dfNoStopwords = remover.transform(tokenizedDF)
     */
    // Define the stop words cleaner using predefined English stop words
    // or you can use pretrained models for StopWordsCleaner
    /*
    val stopWordsCleaner = StopWordsCleaner.pretrained()
      .setInputCols("tokens")
      .setOutputCol("cleanTokens")
      .setCaseSensitive(false)*/

    // Define the Stemmer annotator
    val stemmer = new Stemmer()
      .setInputCols("tokens")
      .setOutputCol("stemmedTokens")
      .setLanguage("English")

    /*
    val lemmatizer = LemmatizerModel.pretrained("lemma_lines", "en")
      .setInputCols("tokens")
      .setOutputCol("lemmaTokens")
     */

    // Create a pipeline with the tokenizer and stemmer
    // , stopWordsCleaner
    val pipeline = new Pipeline().setStages(Array(documentAssembler, tokenizer, stemmer))

    // Fit the pipeline to the data
    val model = pipeline.fit(dfOnlyWhitespace)

    // Transform the DataFrame
    val resultDF = model.transform(dfOnlyWhitespace)



    resultDF.show()
    resultDF.selectExpr("stemmedTokens.result").show(truncate = false)


    // Selecting a single column and creating a new DataFrame
    val results = resultDF.selectExpr("*", "stemmedTokens.result as final_tokens")
    results.show()
    //resultDF = resultDF.withColumn("result", resultDF.selectExpr("stemmedTokens.result"))
    // Getting the column names
    /*var columnNames: Array[String] = w2v_df.columns
    // Printing the column names
    columnNames.foreach(println)
    results.show()
    // Getting the column names
    columnNames = results.columns
    // Printing the column names
    columnNames.foreach(println)*/
    val tfidf_df = results.select("final_tokens", "label")

    //dfOnlyWhitespace.write.format("csv").save("C:\\Users\\bocca\\Desktop\\laurea_magistrale_informatica\\ScalableCloud\\progetto\\FakeNewsClassificationWithDecisionTreeMR\\data\\dataset\\dataset.csv")

    // Step 2: CountVectorizer to build a vocabulary
    val cvModel = new CountVectorizer()
      .setInputCol("final_tokens")
      .setOutputCol("rawFeatures")
      .fit(tfidf_df)

    val featurizedData = cvModel.transform(tfidf_df)

    // Step 3: IDF to transform the counts to TF-IDF
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)

    rescaledData.show()

    /*// Get the list of words in the vocabulary
    val vocabulary = cvModel.vocabulary
    println("Vocabulary Words:")
    vocabulary.foreach(println)*/

    // Get the vocabulary size
    val vocabSize = cvModel.vocabulary.length
    println(s"Vocabulary Size: $vocabSize")

    //rescaledData = spark.createDataFrame(spark.sparkContext.parallelize(rescaledData.tail(rescaledData.count().toInt-2)), rescaledData.schema)

    // Get the value of a cell at row 1, column "name"
    /*val rowValue = typeOf[rescaledData.collect()(3)] // .getAs[String]("features")
    println(rowValue)*/

    /*
    val rowValue2 = rescaledData.collect()(0).getAs[String]("features")
    for (element <- rowValue2) {
      // Some logic here
      println(element)
      println(element.getClass)
    }
    */

    rescaledData.select("features", "label").printSchema()


    // Get the list of words in the vocabulary
    val vocabulary = cvModel.vocabulary

    var schema_seq: Seq[StructField] = Seq()

    vocabulary.foreach(word => {
      schema_seq = schema_seq :+ StructField(word, DoubleType, true)
    })
    schema_seq = schema_seq :+ StructField("label", IntegerType, true)

    // Define the schema for the DataFrame
    val schema = StructType(schema_seq)

    // Create a sequence of Row objects representing the data
    var data: Seq[Row] = Seq()

    var row_seq: Seq[Any] = Seq()
    var newRow: Row = null

    var c = 0
    rescaledData.select("features", "label").show()
    val sel: DataFrame = rescaledData.select("features", "label")
    sel.rdd.foreach {  // .collect()
      case Row(features: Vector, label: Integer) =>
        println(s"vector: $features")
        // Get the length of the vector
        val vectorLength = features.size
        println(s"length: $vectorLength")
        // Iterate over all elements of the vector
        for (index <- 0 until vectorLength) { // VEDERE SE FARLO DISTRIBUITO
          val value = features(index)
          //println(s"index: $index,   value: $value")
          row_seq = row_seq :+ value
        }
        row_seq = row_seq :+ label
        // Create a new Row with an additional element
        println(s"row_seq: $row_seq")
        newRow = Row.fromSeq(row_seq)
        data = data :+ newRow
        println(s"data: $data")

        println(s"count: $c")
        c = c + 1

      case Row(num: Int, text: Seq[_], features: Vector) =>
      println(s"final_tokens: [${text.mkString(", ")}] => \nVector: $features\n")
    case Row(num: Int, text: Vector, features: Vector) =>
      println(s"final_tokens: [${text}] => \nVector: $features\n")
    case Row(num: Int, text: Seq[_], features: Seq[_]) =>
      println(s"final_tokens: [${text.mkString(", ")}] => \nVector: $features\n")
    }

    println(data)
    // Create the DataFrame
    val final_dataset = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    final_dataset.show()

    /*
    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("final_tokens")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val w2c_model = word2Vec.fit(w2v_df)

    var result_w2c = w2c_model.transform(w2v_df)

    // Create the DataFrame
    result_w2c = spark.createDataFrame(spark.sparkContext.parallelize(result_w2c.tail(result_w2c.count().toInt-2)), result_w2c.schema)

    result_w2c.select("result").show()
    */

    /*
    // Original Seq
    var originalSeq: Seq[(Any, Any, Any)] = Seq()

    result_w2c.collect().foreach { case Row(text: Seq[_], num: Int, features: Vector) =>
      println(s"final_tokens: [${text.mkString(", ")}] => \nVector: $features\n")
      val daje: Seq[Any] = features.toArray.toSeq
      println(daje)
      originalSeq :+ daje

      case Row(text: Seq[_], features: Vector) =>
      println(s"final_tokens: [${text.mkString(", ")}] => \nVector: $features\n")

        val daje: Seq[Any] = features.toArray.toSeq
        println(daje)
        originalSeq :+ daje
    }

    println(originalSeq)
    // Define the schema for the DataFrame
    val schema = StructType(Seq(
      StructField("first", DoubleType, true),
      StructField("second", DoubleType, true),
      StructField("third", DoubleType, true)
    ))

    // Convert the Seq to Rows
    val rows = originalSeq.map { case (first, second, third) => Row(first, second, third) }

    // Create a DataFrame from the Rows and Schema
    var df_perandri = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    df_perandri = df_perandri.withColumn("label", expr("1"))

    df_perandri.show()*/


    // Specify the path where you want to write the CSV file
    val outputPath = s"$datasetPath/$csv" //
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

    final_dataset

  }


}