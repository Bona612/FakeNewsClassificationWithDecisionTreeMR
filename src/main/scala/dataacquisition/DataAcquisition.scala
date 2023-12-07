package dataacquisition

import com.johnsnowlabs.nlp.DocumentAssembler
import org.apache.spark.sql.{Column, DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}

import java.nio.file.{Files, Paths}
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StopWordsRemover
import com.johnsnowlabs.nlp.annotator.{LemmatizerModel, Stemmer, StopWordsCleaner, Tokenizer}
import org.apache.spark.ml.linalg.{SQLDataTypes, Vectors}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import utils.GCSUtils
import utils.GCSUtils.isFilePresent
//import com.johnsnowlabs.nlp.annotators.EnglishStemmer
import org.apache.spark.sql.catalyst.ScalaReflection.universe.typeOf
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.{CountVectorizer, IDF}

import java.io.File
import java.security.CodeSource
import scala.sys.process._


class DataAcquisition(datasetList: List[String], csvPerDataset: Map[String, String], columnsMap: Map[String, String], textColumn: String, downloadPath: String, datasetPath: String, csv: String, maxVocabSizeCV: Integer, spark: SparkSession) {

  def getCurrentDirectory(): String = {
    val codeSource: CodeSource = getClass.getProtectionDomain.getCodeSource
    val jarFileLocation = if (codeSource != null) codeSource.getLocation.toURI.getPath else ""
    val absolutePath = new java.io.File(jarFileLocation).getParentFile.getAbsolutePath
    absolutePath
  }

  def loadDataset(): DataFrame = {

/*
    val data2 = Seq((1, Vectors.sparse(3, Seq((1, 0.5), (2, 0.6))), 0),
      (2, Vectors.sparse(3, Seq((1, 0.5), (2, 0.6))), 0),
      (3, Vectors.sparse(3, Seq((1, 0.5), (2, 0.6))), 1))

    // Define the schema
    val schema2 = StructType(Seq(StructField("id", IntegerType, true), StructField("features", SQLDataTypes.VectorType, true), StructField("label", IntegerType, true)))

    // Create an RDD of Rows
    val rdd2 = spark.sparkContext.parallelize(data2)
    val rows = rdd2.map { case (id, features, label) => Row(id, features, label) }

    val v = Array("first", "second", "third")

    // Create a DataFrame
    val df2 = spark.createDataFrame(rows, schema2)

    // Define a UDF to convert Vector to Array
    val vectorToArray2 = udf((v: Vector) => v.toArray)

    // Apply the UDF to the "features" column
    val dfWithArray2 = df2.withColumn("features_array", vectorToArray2(col("features")))

    val newColumns2 = v.zipWithIndex.map {
      case (alias, idx) => col("features_array").getItem(idx).alias(alias)
    }
    println("new columns created")
    // Add the new columns to the DataFrame
    val newDF22 = dfWithArray2.select(newColumns2 :+ col("label"): _*)
    newDF22.show()
*/
/*
    // Specify your HDFS command
    val hdfsCommand1 = Seq("hdfs", "dfs", "-ls", "hdfs:///user/fnc_user/dataset_finaleee")

    // Run the command and capture the output line by line
    val process1 = Process(hdfsCommand1)
    val output1 = process1.lineStream

    // Use foldLeft to process lines and accumulate the last element
    val lastElement1: String = output1
      .filter(_.endsWith(".csv"))
      .foldLeft("N/A") { (_, line) =>
        // Split the line based on "/"
        val pathElements1 = line.split("/")

        // Return the last element of the array or "N/A" if empty
        pathElements1.lastOption.getOrElse("N/A")
      }

    // Print or use the final last element
    println(s"Final last element: $lastElement1")

    GCSUtils.saveFile("/data/dataset/fake_news_log_train.csv", "hdfs:///user/fnc_user/download/fake-news-log/train.csv")
*/

    var datasetPathList: ListBuffer[String] = ListBuffer()

    /*var cd = "hdfs dfs -ls hdfs://" + getCurrentDirectory()
    var cdExitCode = cd !

    println("cuurent dir exit code: " + cdExitCode)*/
    var cd = "hdfs dfs -ls hdfs:///user/"
    var cdExitCode = cd !

    println("tmp- current dir exit code: " + cdExitCode)
    /*cd = "hdfs dfs -ls hdfs:///tmp"
    cdExitCode = cd !

    println("bocca- cuurent dir exit code: " + cdExitCode)
    cd = "hdfs dfs -ls hdfs:///user/root"
    cdExitCode = cd !

    println("root- cuurent dir exit code: " + cdExitCode)
    cd = "hdfs dfs -ls hdfs:///user/dataproc"
    cdExitCode = cd !

    println("dataproc- cuurent dir exit code: " + cdExitCode)*/

    val userDirCommand = "hdfs dfs -mkdir hdfs:///user/fnc_user"
    val userDirCommandExitCode = userDirCommand !

    val downloadDirCommand = "hdfs dfs -mkdir hdfs:///user/fnc_user/download"
    val downloadDirCommandExitCode = downloadDirCommand !

    println("hdfs dir dataset creation exit code: " + downloadDirCommandExitCode)

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

    def processDataset(spark: SparkSession, datasetName: String, columnName: String): DataFrame = {

      // RICONTROLLARE TUTTI I PASSAGGI, E INSERIRE QUALCHE PRINT

      // Read CSV file from HDFS
      val datasetDF: DataFrame = spark.read
        .option("header", "true")
        .option("escape", "\"")
        .option("multiLine", "true")
        .option("sep", ",")
        .option("charset", "UTF-8")
        .csv(s"hdfs:///user/fnc_user/download/$datasetName/" + csvPerDataset(datasetName))

      println("NUM PARTITIONS: " + datasetDF.rdd.partitions.length.toString)

      // Filter rows based on certain conditions
      val filteredDF: DataFrame = filterRows(datasetDF)

      // Process the "text" column
      val textDF: DataFrame = processTextColumn(filteredDF)
      if (textDF != null) {
        val dfNot01 = textDF.filter(col("ground_truth").notEqual(0) && col("ground_truth").notEqual(1))
        if (dfNot01.isEmpty) {
          println("BENEEEE")
        }
        else {
          println("NO BUONO")
          dfNot01.show()
        }
      }

      // Select only specific columns
      val selectedColumns: Array[String] = Array(columnName)
      val selectedDF: DataFrame = filteredDF.select(selectedColumns.map(col): _*)

      // Remove rows with missing values
      val cleanedDF: DataFrame = selectedDF.na.drop(selectedColumns)

      // Drop duplicates based on selected columns
      val deduplicatedDF: DataFrame = cleanedDF.dropDuplicates(selectedColumns)

      // Rename columns
      val renamedDF: DataFrame = renameColumns(deduplicatedDF, columnName, textColumn)

      // Add label column based on datasetName
      val labeledDF: DataFrame = addLabelColumn(renamedDF, datasetName)

      // Union textDF and labeledDF
      if (textDF != null) {

        // QUI MODIFICARE WRITE AND READ, CON QUOTE ESCAPE

        val uDF = textDF.unionByName(labeledDF)
        // Coalesce to a single partition before saving
        val uDFsave: DataFrame = uDF.coalesce(1)

        // Specify your output path and format (e.g., parquet, csv, etc.)
        val outputPath_save = s"hdfs:///user/fnc_user/save/$datasetName"
        // Write the DataFrame to a single CSV file
        uDFsave.write //.format("com.databricks.spark.csv")
          .mode("overwrite")
          .option("header", "true")
          .option("quote", "\"") // Quote character
          .option("escape", "\"") // Quote escape character (end of quote)
          .option("multiLine", "true")
          .option("delimiter", ",")
          .option("charset", "UTF-8")
          .csv(outputPath_save)

        // Read CSV file from HDFS
        val uDFsave_read: DataFrame = spark.read
          .option("header", "true")
          .option("quote", "\"") // Quote character
          .option("escape", "\"") // Quote escape character (end of quote)
          .option("multiLine", "true")
          .option("sep", ",")
          .option("charset", "UTF-8")
          .csv(outputPath_save)

        val dfNot10 = uDFsave_read.filter(col("ground_truth").notEqual(0) && col("ground_truth").notEqual(1))
        if (dfNot10.isEmpty) {
          println("BENEEEE")
        }
        else {
          println("NO BUONO")
          dfNot10.show()
        }

        println(datasetName)
        println("COUNT FINALE: " + uDFsave_read.count())
        uDFsave_read
      } else {

        // QUI MODIFICARE WRITE AND READ, CON QUOTE ESCAPE

        val labeledDFsave: DataFrame = labeledDF.coalesce(1)

        // Specify your output path and format (e.g., parquet, csv, etc.)
        val outputPath_save = s"hdfs:///user/fnc_user/save/$datasetName"
        // Write the DataFrame to a single CSV file
        labeledDFsave.write //.format("com.databricks.spark.csv")
          .mode("overwrite")
          .option("header", "true")
          .option("quote", "\"") // Quote character
          .option("escape", "\"") // Quote escape character (end of quote)
          .option("multiLine", "true")
          .option("delimiter", ",")
          .option("charset", "UTF-8")
          .csv(outputPath_save)

        // Read CSV file from HDFS
        val labeledDFsave_read: DataFrame = spark.read
          .option("header", "true")
          .option("quote", "\"") // Quote character
          .option("escape", "\"") // Quote escape character (end of quote)
          .option("multiLine", "true")
          .option("sep", ",")
          .option("charset", "UTF-8")
          .csv(outputPath_save)

        val dfNot102 = labeledDFsave_read.filter(col("ground_truth").notEqual(0) && col("ground_truth").notEqual(1))
        if (dfNot102.isEmpty) {
          println("BENEEEE")
        }
        else {
          println("NO BUONO")
          dfNot102.show()
        }

        println(datasetName)
        println("COUNT FINALE: " + labeledDFsave_read.count())
        labeledDFsave_read
      }
    }

    def filterRows(datasetDF: DataFrame): DataFrame = {
      // Filter rows based on certain conditions
      val columnNames: Array[String] = datasetDF.columns
      columnNames.foldLeft(datasetDF)((df, colName) => {
        colName match {
          case "label" | "Label" | "Ground Label" =>
            val filteredDF = df.filter(col(colName) === "fake" || col(colName) === "FAKE" || col(colName) === 1)
            filteredDF.withColumnRenamed(colName, "ground_truth")
          case "language" =>
            df.filter(col(colName) === "english")
          case _ => df
        }
      })
    }

    // CAMBIARE DEVE ESSERE FATTO UN PASSO ALLA VOLTA
    def processTextColumn(datasetDF: DataFrame): DataFrame = {

      if (datasetDF.columns.contains("text")) {
        val splittedDF = datasetDF.withColumn("split", split(col("text"), "\\."))
        val explodedDF = splittedDF.select(explode(col("split")).alias("exploded"))
        val trimmedDF = explodedDF.select(trim(col("exploded")).alias("title"))
        val noWhitespace = trimmedDF.filter("title != ''")

        val resultDF = noWhitespace.withColumn("ground_truth", lit(1))
        println(resultDF.schema)
        println("vediamo le colonne")
        resultDF.columns.foreach(println)

        // Updated filter condition to check for numeric values
        val dfNot01 = resultDF.filter(col("ground_truth").notEqual(0) && col("ground_truth").notEqual(1))
        if (dfNot01.isEmpty) {
          println("BENEEEEE")
        } else {
          println("NO BUONO")
          dfNot01.show()
        }

        println("text function")
        resultDF.show()

        resultDF
      } else {
        // If "text" column is not present, return the original DataFrame
        null
      }
    }

    // MOLTO PIù SEMPLICE DI COSì, SEMPLIFICARE
    def renameColumns(datasetDF: DataFrame, oldName: String, newName: String): DataFrame = {
      if (oldName != null && newName != null && !oldName.equals(newName) && datasetDF.columns.contains(oldName)) {
        datasetDF.withColumnRenamed(oldName, newName)
      }
      else {
        datasetDF
      }
    }

    // MI PARE SIANO FINITE LE MODIFICHE QUI
    def addLabelColumn(datasetDF: DataFrame, datasetName: String): DataFrame = {
      // Add label column based on datasetName
      if (datasetName == "million-headlines") {
        datasetDF.withColumn("ground_truth", lit("0"))
      } else {
        datasetDF.withColumn("ground_truth", lit("1"))
      }
    }

    // MI PARE SIANO FINITE LE MODIFICHE QUI
    val unionedDataset2: DataFrame = columnsMap.map {
      case (datasetName, columnName) => processDataset(spark, datasetName, columnName)
    }.reduce(_ unionByName _)


    val drop2 = unionedDataset2.dropDuplicates()
    drop2.show()

    // Coalesce to a single partition before saving
    val finalDataset2: DataFrame = drop2.coalesce(1)

    // QUI MODIFICARE WRITE AND READ, CON QUOTE ESCAPE

    // Specify your output path and format (e.g., parquet, csv, etc.)
    val outputPath2 = "hdfs:///user/fnc_user/final"
    // Write the DataFrame to a single CSV file
    finalDataset2.write//.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .option("quote", "\"") // Quote character
      .option("escape", "\"") // Quote escape character (end of quote)
      .option("multiLine", "true")
      .option("delimiter", ",")
      .option("charset", "UTF-8")
      .csv(outputPath2)

    // Read CSV file from HDFS
    val readDF: DataFrame = spark.read
      .option("header", "true")
      .option("quote", "\"") // Quote character
      .option("escape", "\"") // Quote escape character (end of quote)
      .option("multiLine", "true")
      .option("sep", ",")
      .option("charset", "UTF-8")
      .csv(outputPath2)


    val dfNot0123 = readDF.filter(col("ground_truth").notEqual(0) && col("ground_truth").notEqual(1))
    println(dfNot0123.schema)
    if (dfNot0123.isEmpty) {
      println("BENEEEE")
    }
    else {
      println("NO BUONO")
      dfNot0123.show()
    }

    val count = readDF.repartition(4)
    println("NUM PARTITIONS: " + count.rdd.partitions.length.toString)
    println(count.count())
    println("fatto")


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


    finalDataset = unionedDataset2

    println("Final Dataset creation finished!")
    // Define a regular expression pattern to match Twitter user mentions
    val mentionPattern = "@[a-zA-Z0-9_]+"
    // Define a regular expression pattern to match URLs
    val urlPattern = """\b(?:https?|ftp|com):\/\/\S+"""
/*
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



    // Apply regexp_replace to remove Twitter user mentions
    val dfNoMentions = dfLower.withColumn("title", regexp_replace(col("title"), mentionPattern, ""))



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
*/
    //val dfWithTokens = dfOnlyWhitespace.withColumn("tokens", split(col("title"), "\\s+"))

    // IN MODO DISTRIBUITO
    // Define a list of transformations as functions
    val transformations: List[Column => Column] = List(
      trim, // Trim whitespaces
      lower, // Convert to lowercase
      c => regexp_replace(c, mentionPattern, ""), // Remove Twitter user mentions
      c => regexp_replace(c, urlPattern, ""), // Remove URLs
      c => regexp_replace(c, "\n", ""), // Remove newline characters
      c => regexp_replace(c, "\t", ""), // Remove tab characters
      c => regexp_replace(c, "\\d+", ""), // Remove numbers
      c => regexp_replace(c, "\\s+", " "), // Remove consecutive whitespace characters
      c => regexp_replace(c, "[^a-zA-Z0-9\\s]", "") // Remove punctuation
    )

    // Apply the transformations in a distributed manner
    val dfProcessed: DataFrame = transformations.foldLeft(count)((df, transformation) => df.withColumn("title", transformation(col("title"))))

    // DA CAMBIARE QUESTO, è LEGGERMENTE DIVERSO DA COSì
    // Filter out empty strings and rows with only whitespace characters
    val dfWoutWhitespace: DataFrame = dfProcessed.filter(trim(col("title")) =!= "")

    val dfWoutDup: DataFrame = dfWoutWhitespace.dropDuplicates() // Drop duplicates



    println(dfWoutDup.schema)
    if (dfWoutDup.isEmpty) {
      println("IL PROBLEMA è QUI, è VUOTOOOOO !!!")
    }
    else {
      println("IL DATAFRAME è OKAY")
    }


    // Coalesce to a single partition before saving
    val cleaned = dfWoutDup.coalesce(1).limit(1000000)
    // Specify your output path and format (e.g., parquet, csv, etc.)
    val outputPath3 = "hdfs:///user/fnc_user/final_clean"
    // Write the DataFrame to a single CSV file
    cleaned.write //.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true")
      .option("quote", "\"") // Quote character
      .option("escape", "\"") // Quote escape character (end of quote)
      .option("multiLine", "true")
      .option("delimiter", ",")
      .option("charset", "UTF-8")
      .csv(outputPath3)

    // Read CSV file from HDFS
    val readDF2: DataFrame = spark.read
      .option("header", "true")
      .option("quote", "\"") // Quote character
      .option("escape", "\"") // Quote escape character (end of quote)
      .option("multiLine", "true")
      .option("sep", ",")
      .option("charset", "UTF-8")
      .csv(outputPath3)

    println("nuova rilettura")
    val dfNot1023 = readDF2.filter(col("ground_truth").notEqual(0) && col("ground_truth").notEqual(1))
    println(dfNot1023.schema)
    if (dfNot1023.isEmpty) {
      println("BENEEEE")
    }
    else {
      println("NO BUONO")
      dfNot1023.show()
    }
    /*val part4 = readDF2.repartition(4)
    println("NUM PARTITIONS: " + part4.rdd.partitions.length.toString)
    println(part4.count())
    println("fatto")*/


    val documentAssembler = new DocumentAssembler()
      .setInputCol("title")
      .setOutputCol("document")

    // Tokenize the text into words
    val tokenizer = new Tokenizer()
      .setInputCols("document")
      .setOutputCol("tokens")

    val remover = StopWordsCleaner.pretrained()
      .setInputCols(Array("tokens"))
      .setOutputCol("cleanTokens")
      .setCaseSensitive(false)

    val stopWords = remover.getStopWords
    // Print or display the stop words
    stopWords.foreach(println)

    // Define the Stemmer annotator
    val stemmer = new Stemmer()
      .setInputCols(Array("cleanTokens"))
      .setOutputCol("stemmedTokens")
      .setLanguage("English")

    /*
    val lemmatizer = LemmatizerModel.pretrained("lemma_lines", "en")
      .setInputCols("tokens")
      .setOutputCol("lemmaTokens")
     */

    // Create a pipeline with the tokenizer and stemmer
    val pipeline = new Pipeline().setStages(Array(documentAssembler, tokenizer, remover, stemmer))

    // Fit the pipeline to the data
    val model = pipeline.fit(readDF2)

    // Transform the DataFrame
    val resultDF = model.transform(readDF2)
    // Selecting a single column and creating a new DataFrame
    val results = resultDF.selectExpr("*", "stemmedTokens.result as final_tokens")
    val results_tosave = results.select("final_tokens", "ground_truth").dropDuplicates()
    results_tosave.show()
/*
    val outputPath4 = "hdfs:///user/fnc_user/final_pipeline"
    val pipel = results_tosave.coalesce(1)
    // Write the DataFrame to a single CSV file
    pipel.write //.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true") // Include header in the CSV file
      .csv(outputPath4)

    // Read CSV file from HDFS
    val pipel_read: DataFrame = spark.read
      .option("header", "true")
      .option("escape", "\"")
      .option("multiLine", "true")
      .option("sep", ",")
      .option("charset", "UTF-8")
      .csv(outputPath4)

    println("nuova rilettura")

    println("NUM PARTITIONS: " + pipel_read.rdd.partitions.length.toString)
    println(pipel_read.count())
    println("fatto")

    pipel_read.show()
 */
    /*val resultDF_rep = readDF2.repartition(2)
    println("NUM PARTITIONS: " + resultDF_rep.rdd.partitions.length.toString)
    resultDF_rep.selectExpr("stemmedTokens.result").show(truncate = false)*/


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


    //dfOnlyWhitespace.write.format("csv").save("C:\\Users\\fnc_user\\Desktop\\laurea_magistrale_informatica\\ScalableCloud\\progetto\\FakeNewsClassificationWithDecisionTreeMR\\data\\dataset\\dataset.csv")

    //val tfidf_df_rep = tfidf_df.repartition(12)
    println("NUM PARTITIONS: " + results_tosave.rdd.partitions.length.toString)
    println("cv iniziato !!!")
    println("FIT iniziato !!!")

    val maxVocabSize = maxVocabSizeCV
    // Step 2: CountVectorizer to build a vocabulary
    val cvModel = new CountVectorizer()
      .setInputCol("final_tokens")
      .setOutputCol("rawFeatures")
      .setVocabSize(maxVocabSize)
      .fit(results_tosave)

    println("FIT finito e TRANSFORM iniziato !!!")

    val featurizedData = cvModel.transform(results_tosave)

    println("TRANSFORM finito !!!")
    println("idf iniziato !!!")
    // Step 3: IDF to transform the counts to TF-IDF
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    println("FIT finito e TRANSFORM iniziato !!!")
    val rescaledData = idfModel.transform(featurizedData)
    // Rename the 'name' column to 'full_name'

    // FORSE QUANDO ARRIVA QUA è GIà CAMBIATA LA LABEL
    //val rescaledData = rescaledData_1.withColumnRenamed("label", "ground_truth")
    println("TRANSFORM finito !!!")
    rescaledData.show()
    println(rescaledData.schema)

    /*// Get the list of words in the vocabulary
    val vocabulary = cvModel.vocabulary
    println("Vocabulary Words:")
    vocabulary.foreach(println)*/

    // Get the vocabulary size
    val vocabSize = cvModel.vocabulary.length
    println(s"Vocabulary Size: $vocabSize")



    val sc = rescaledData.select("features", "ground_truth").schema
    println(sc)
    println(sc.toString())
    println(sc.length)
/*
    val outputPath5 = "hdfs:///user/fnc_user/final_cvidf"
    val final_cvidf = rescaledData.coalesce(1)
    // Write the DataFrame to a single CSV file
    final_cvidf.write //.format("com.databricks.spark.csv")
      .mode("overwrite")
      .option("header", "true") // Include header in the CSV file
      .csv(outputPath5)

    // Read CSV file from HDFS
    val final_cvidf_read: DataFrame = spark.read
      .option("header", "true")
      .option("escape", "\"")
      .option("multiLine", "true")
      .option("sep", ",")
      .option("charset", "UTF-8")
      .csv(outputPath5)

    val finalll4 = final_cvidf_read.repartition(4)
    println("NUM PARTITIONS: " + finalll4.rdd.partitions.length.toString)
    println(finalll4.count())
    println("fatto")
*/
    // Get the list of words in the vocabulary
    val vocabulary = cvModel.vocabulary
/*
    val headers = vocabulary :+ "label"

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


    // Define the schema for the new DataFrame
    val schemaFinal = StructType(vocabulary.init.map(fieldName => StructField(fieldName, DoubleType, nullable = false)) :+ StructField("label", IntegerType, nullable = false))
*/

    val schemaFinall = StructType(vocabulary.init.map(fieldName => StructField(fieldName, DoubleType, nullable = false)) :+ StructField("ground_truth", IntegerType, nullable = false))


    try {
      println("1 --- Iinizio della trasformazione finale del dataset !!!")

      // Create a schema for the new dataframe
      val schemaFinal = StructType(vocabulary.init.map(fieldName => StructField(fieldName, DoubleType, nullable = true)) :+ StructField("ground_truth", IntegerType, nullable = true))
      println("SCHEMA")
      //println(schemaFinal.fields.mkString("Array(", ", ", ")"))
      println(schemaFinal.length)


      // Get the schema of the DataFrame
      val schema_print = rescaledData.schema

      // Get the data type of the specified column
      val dataType = schema_print("features").dataType
      println("TYPE of features: " + dataType)
      val z = 688
      println(rescaledData.select("features").head(z).last.toString())
      println("VOCAB TYPE: " + vocabulary.getClass)
      /*
      // Create new columns for each element in the dense vector
      val numElements = rescaledData.select("features").head().getAs[Vector](0).size
      println("SIZE: " + numElements.toString)

      val newColumns = (0 until numElements).map(i => col("features").getItem(i).alias(vocabulary(i)))
      */
      //val vecToArray = udf((xs: Vector) => xs.toArray)
      //val dfArr = rescaledData.withColumn("featuresArr", vecToArray(col("features")))

      // Define a UDF to convert Vector to Array
      val vectorToArray = udf((v: Vector) => v.toArray)
      println("adding array column")
      // Apply the UDF to the "features" column
      val rescaledDataWArray = rescaledData.withColumn("features_array", vectorToArray(col("features")))
      println("Starting sql definition")
      val newColumns = vocabulary.zipWithIndex.map {
        case (alias, idx) => col("features_array").getItem(idx).alias(alias)
      }
      println("new columns created")
      // Add the new columns to the DataFrame
      val newDF = rescaledDataWArray.select(newColumns :+ col("ground_truth"): _*)

      println("FINITOOOOOOOOOOOOOOOO")

      // NON RICORDO SE TUTTI QUESTI PASSAGGI SIANO NECESSARI, FORSE AVEVO TROVATO UN MODO PIù VELOCE

      val outputPath5 = "hdfs:///user/fnc_user/dataset_finaleee"
      val outputGCSPath = "gs://fnc-bucket-final/data/dataset/dataset.csv"
      val finaleee = newDF.coalesce(1)
      println(finaleee.schema)
      if (finaleee.isEmpty) {
        println("NO BUONO")
      }
      else {
        println("BENEEEE")
        finaleee.show()
      }

      finaleee.columns.foreach(column => {
        if (column.equals("")) {
          println("Abbiamo una colonna ancora vuota")
        }
      })

      // Write the DataFrame to a single CSV file
      finaleee.write //.format("com.databricks.spark.csv")
        .mode("overwrite")
        .option("header", "true")
        .option("quote", "\"") // Quote character
        .option("escape", "\"") // Quote escape character (end of quote)
        .option("multiLine", "true")
        .option("delimiter", ",")
        .option("charset", "UTF-8")
        .csv(outputPath5)

      // Write the DataFrame to a single CSV file
      finaleee.write //.format("com.databricks.spark.csv")
        .mode("overwrite")
        .option("header", "true")
        .option("quote", "\"") // Quote character
        .option("escape", "\"") // Quote escape character (end of quote)
        .option("multiLine", "true")
        .option("delimiter", ",")
        .option("charset", "UTF-8")
        .csv(outputGCSPath)

      val isDatasetPresent = isFilePresent("data/dataset/dataset.csv", spark)
      println("the dataset is present? " + isDatasetPresent.toString)


/*
      println("between")

      // Specify your HDFS command
      val hdfsCommand = Seq("hdfs", "dfs", "-ls", outputPath5)

      // Run the command and capture the output line by line
      val process = Process(hdfsCommand)
      val output = process.lineStream

      // Use foldLeft to process lines and accumulate the last element
      val csvName: String = output
        .filter(_.endsWith(".csv"))
        .foldLeft("N/A") { (_, line) =>
          // Split the line based on "/"
          val pathElements = line.split("/")

          // Return the last element of the array or "N/A" if empty
          pathElements.lastOption.getOrElse("N/A")
        }

      // Print or use the final last element
      println(s"CSV name: $csvName")


      // Read CSV file from HDFS
      val finaleee_read: DataFrame = spark.read
        .option("header", "true")
        .option("quote", "\"") // Quote character
        .option("escape", "\"") // Quote escape character (end of quote)
        .option("multiLine", "true")
        .option("sep", ",")
        .option("charset", "UTF-8")
        .csv(outputPath5)

      val dfNot10234 = finaleee_read.filter(col("ground_truth").notEqual(0) && col("ground_truth").notEqual(1))
      if (dfNot10234.isEmpty) {
        println("BENEEEE")
      }
      else {
        println("NO BUONO")
        dfNot10234.show()
      }

      println("NUM PARTITIONS: " + finaleee_read.rdd.partitions.length.toString)
      println("fatto")

      finaleee_read.show()

      val hdfs_o = outputPath5 + "/" + csvName
      println("hdfs finale: " + hdfs_o)
      GCSUtils.saveFile("/data/dataset/dataset.csv", hdfs_o) // gs://fnc-bucket-final
*/
      //val cgs = s"gsutil cp $hdfs_o gs://fnc-bucket-final/data/dataset/$csvName".!!
      //val exit_cgs = cgs !

      //println("gsutil exit code: " + exit_cgs)
    } catch {
      case e: Exception =>
        // Log the exception details
        println("CATCH")
        println(s"Error occurred: ${e.getMessage}")
        e.printStackTrace()
        // You can also log to a file or another logging system
        // For example, spark.log.error(s"Error occurred: ${e.getMessage}")
        // Rethrow the exception to ensure the job fails with the error
        //throw e
        println("CATCH")
    }


    // Specify the path where you want to write the CSV file
    val outputPath = s"$datasetPath/$csv" //

    null
  }

}