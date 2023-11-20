package dataacquisition

import com.johnsnowlabs.nlp.DocumentAssembler
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
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

    // VERSIONE DISTRIBUITA
    val datasetFrames: Map[String, DataFrame] = columnsMap.map {
      case (datasetName, columnName) =>
        val currentDir = getCurrentDirectory()
        println(currentDir)
        var datasetDF: DataFrame = spark.read.option("header", "true").option("escape","\"").option("multiLine","true").option("sep", ",").option("charset", "UTF-8").csv(s"hdfs:///user/fnc_user/download/$datasetName/" + csvPerDataset(datasetName))
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
        var unionedDF: DataFrame = null
        if(textDF != null)
          unionedDF = textDF.union(datasetDF)
        else
          unionedDF = datasetDF

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
      Row("Is this the first document? ", 1),
      Row("COVID-19 vaccines prove highly effective in preventing severe illness", 1),
      Row("Global economy shows signs of recovery after the pandemic", 1),
      Row("Scientists make progress in the development of a malaria vaccine", 1),
      Row("UN Climate Change Conference results in new commitments to reduce emissions", 1),
      Row("NASA's Ingenuity helicopter successfully completes multiple flights on Mars", 1),
      Row("Groundbreaking study identifies new treatment for Alzheimer's disease", 1),
      Row("International effort leads to the successful restoration of a coral reef", 1),
      Row("Renewable energy sources surpass coal in the United States for the first time", 1),
      Row("Researchers discover a new species of deep-sea marine life", 1),
      Row("Global initiative aims to plant one trillion trees to combat climate change", 1),
      Row("WHO announces the eradication of wild poliovirus in Africa", 1),
      Row("First-ever image of a black hole captured by the Event Horizon Telescope", 1),
      Row("SpaceX's Crew Dragon successfully docks with the International Space Station", 1),
      Row("Breakthrough in cancer research leads to promising new treatment options", 1),
      Row("World Health Organization approves a new malaria vaccine for use in children", 1),
      Row("NASA's Perseverance Rover collects first samples of Martian rock", 1),
      Row("Global efforts lead to a significant decline in new HIV infections", 1),
      Row("Advancements in gene therapy show promise in treating genetic disorders", 1),
      Row("International collaboration results in the successful sequencing of the human genome", 1),
      Row("UNESCO designates new World Heritage Sites to protect cultural and natural treasures", 1),
      Row("Breakthrough in fusion energy research brings us closer to clean and abundant energy", 1),
      Row("Scientists develop a promising new treatment for drug-resistant tuberculosis", 1),
      Row("World leaders commit to ambitious goals to protect biodiversity", 1),
      Row("Successful trial of a universal flu vaccine raises hopes for better influenza prevention", 1),
      Row("Renewable energy capacity surpasses coal and gas for the first time globally", 1),
      Row("International Space Station celebrates 20 years of continuous human presence", 1),
      Row("Global initiative aims to eliminate single-use plastics by 2030", 1),
      Row("Researchers make significant progress in the quest for a viable HIV vaccine", 1),
      Row("Advancements in CRISPR technology open new possibilities for gene editing", 1),
      Row("NASA's Juno spacecraft provides unprecedented insights into Jupiter's atmosphere", 1),
      Row("Breakthrough in Alzheimer's research identifies a blood test for early detection", 1),
      Row("International collaboration leads to the discovery of a potential treatment for Ebola", 1),
      Row("Renewable energy outpaces fossil fuels in the European electricity mix", 1),
      Row("Scientists achieve a major milestone in the development of a COVID-19 antiviral pill", 1),
      Row("Global initiative aims to protect 30% of the world's oceans by 2030", 1),
      Row("NASA's Artemis program aims to return humans to the Moon by 2024", 1),
      Row("Breakthrough in quantum computing brings us closer to practical applications", 1),
      Row("International Space Station completes 100,000 orbits around Earth", 1),
      Row("Advancements in solar technology lead to more efficient and affordable panels", 1),
      Row("Global efforts to combat deforestation focus on reforestation initiatives", 1),
      Row("Researchers achieve a major milestone in the development of a COVID-19 vaccine for children", 1),
      Row("UNESCO adds the Great Barrier Reef to the list of World Heritage Sites in danger", 1),
      Row("Scientists successfully use CRISPR gene editing to treat a genetic disorder in vivo", 1),
      Row("Global initiative aims to provide COVID-19 vaccines to low-income countries", 1),
      Row("NASA's James Webb Space Telescope set to launch, promising new discoveries", 1),
      Row("Breakthrough in stem cell research offers potential for regenerative medicine", 1),
      Row("International collaboration results in the successful eradication of rinderpest", 1),
      Row("Renewable energy capacity in the United States surpasses coal for the first time", 1),
      Row("Scientists discover a new exoplanet with potential for signs of life", 1),
      Row("Global effort leads to the successful conservation of endangered species", 1),
      Row("NASA's InSight lander provides valuable insights into Mars' seismic activity", 1),
      Row("Breakthrough in AI research enables more accurate weather predictions", 1),
      Row("International collaboration results in the successful development of a malaria vaccine", 1),
      Row("Advancements in 3D printing technology revolutionize healthcare applications", 1),
      Row("Global initiative aims to eliminate river blindness in affected regions", 1),
      Row("Researchers make progress in the development of a universal cancer vaccine", 1),
      Row("UNESCO recognizes traditional Japanese washoku cuisine as intangible cultural heritage", 1),
      Row("Renewable energy surpasses fossil fuels as the largest source of new power capacity", 1),
      Row("Scientists discover a new species of dinosaur in Argentina", 1),
      Row("Breakthrough in the development of a malaria vaccine for pregnant women", 1),
      Row("NASA's Juno spacecraft uncovers new mysteries about Jupiter's magnetic field", 1),
      Row("Global efforts lead to the successful conservation of giant pandas", 1),
      Row("Advancements in fusion energy research bring us closer to sustainable power", 1),
      Row("International collaboration results in the successful sequencing of the wheat genome", 1),
      Row("Renewable energy capacity in China surpasses the rest of the world combined", 1),
      Row("Researchers achieve a major breakthrough in the development of a universal flu vaccine", 1),
      Row("Breakthrough in artificial photosynthesis technology for sustainable fuel production", 1),
      Row("NASA's Parker Solar Probe provides unprecedented close-up views of the Sun", 1),
      Row("Global initiative aims to eliminate blinding trachoma in affected regions", 1),
      Row("Scientists make progress in the development of a Zika virus vaccine", 1),
      Row("International collaboration results in the successful restoration of a mangrove forest", 1),
      Row("Advancements in quantum computing open new possibilities for scientific research", 1),
      Row("Renewable energy capacity in India sees significant growth", 1),
      Row("NASA's New Horizons spacecraft provides detailed images of Pluto's surface", 1),
      Row("Breakthrough in CRISPR technology allows for precise gene editing in plants", 1),
      Row("Global efforts lead to the successful conservation of the Amur leopard", 1),
      Row("Researchers achieve a major milestone in the development of a malaria vaccine for infants", 1),
      Row("UNESCO designates ancient Babylon as a World Heritage Site", 1),
      Row("Renewable energy surpasses coal as the largest source of power in the European Union", 1),
      Row("International collaboration results in the successful elimination of Guinea worm disease", 1),
      Row("Breakthrough in AI research leads to improved diagnosis of rare genetic disorders", 1),
      Row("NASA's TESS mission discovers a new exoplanet with potential for habitability", 1),
      Row("Advancements in gene therapy show promise in treating Duchenne muscular dystrophy", 1),
      Row("Global initiative aims to protect the world's remaining wilderness areas", 1),
      Row("Researchers make progress in the development of a malaria vaccine for adults", 1),
      Row("Renewable energy capacity in Australia reaches a new record", 1),
      Row("NASA's OSIRIS-REx spacecraft successfully collects samples from asteroid Bennu", 1),
      Row("Breakthrough in the development of a universal flu vaccine shows promising results", 1),
      Row("International collaboration results in the successful conservation of the vaquita", 1),
      Row("Advancements in CRISPR technology allow for precise gene editing in animals", 1),
      Row("UNESCO designates the Okavango Delta as a World Heritage Site", 1),
      Row("Renewable energy capacity in Germany surpasses coal for the first time", 1),
      Row("NASA's Juno spacecraft provides stunning images of Jupiter's polar regions", 1),
      Row("Breakthrough in the development of a COVID-19 treatment reduces severity of illness", 1),
      Row("Global initiative aims to eliminate mother-to-child transmission of HIV", 1),
      Row("Researchers make progress in the development of a universal cancer vaccine", 1),
      Row("Advancements in fusion energy research bring us closer to sustainable power", 1),
      Row("Renewable energy capacity in India sees significant growth", 1),
      Row("NASA's New Horizons spacecraft provides detailed images of Pluto's surface", 1),
      Row("Breakthrough in CRISPR technology allows for precise gene editing in plants", 1),
      Row("Scientists discover a parallel universe where gravity pushes instead of pulls", 0),
      Row("New law requires citizens to wear socks on their hands in public", 0),
      Row("Incredible breakthrough: Plants develop the ability to communicate with each other", 0),
      Row("Government introduces mandatory daily dance breaks for all citizens", 0),
      Row("Study finds that laughter can be used as a renewable energy source", 0),
      Row("Penguins start their own rock band and top the charts", 0),
      Row("Researchers create a device that turns dreams into reality", 0),
      Row("Aliens demand to be included in Earth's next census", 0),
      Row("New scientific experiment turns tomatoes into strawberries", 0),
      Row("International hot dog shortage declared; world leaders convene emergency summit", 0),
      Row("Astronauts host a space-themed cooking show from the International Space Station", 0),
      Row("Invention allows humans to breathe underwater; scuba diving industry in crisis", 0),
      Row("Government to replace all road signs with emojis for better communication", 0),
      Row("Scientists develop a potion that grants invisibility for 24 hours", 0),
      Row("World record set for the largest synchronized dance by robots", 0),
      Row("New law requires pets to obtain a driver's license for traveling in cars", 0),
      Row("Researchers create a language translation device for babies", 0),
      Row("Robots form a union and go on strike for better working conditions", 0),
      Row("International competition held to find the world's best cloud shapers", 0),
      Row("Government to launch a mission to bring back dinosaurs from extinction", 0),
      Row("Study suggests that talking to plants improves their growth", 0),
      Row("Chocolate declared the official currency in a small island nation", 0),
      Row("Scientists develop a pill that allows humans to hibernate for months", 0),
      Row("Penguins organize a global conference on ice preservation", 0),
      Row("New law mandates wearing pajamas to work for increased productivity", 0),
      Row("Space agency plans to build a space hotel on the moon by 2030", 0),
      Row("Astronauts discover a giant alien snowman on a distant planet", 0),
      Row("Government to implement a mandatory daily hour of bubble-wrap popping", 0),
      Row("Researchers create a device that translates baby cries into words", 0),
      Row("International competition for the best underwater sculpture garden", 0),
      Row("Government to build a giant trampoline to ease commuter traffic", 0),
      Row("Scientists create a pill that makes people immune to bad hair days", 0),
      Row("Robots form a jazz band and release a chart-topping album", 0),
      Row("New law requires citizens to celebrate their half-birthdays with a parade", 0),
      Row("Researchers develop a machine that turns thoughts into text", 0),
      Row("Government introduces a tax break for citizens who own talking pets", 0),
      Row("Astronauts organize a marathon on the surface of Mars", 0),
      Row("International competition for the best treehouse design", 0),
      Row("Scientists create a device that allows people to taste colors", 0),
      Row("Penguins start a social media platform exclusively for birds", 0),
      Row("New law requires everyone to wear hats made of recycled materials", 0),
      Row("Government to launch a mission to explore the mysteries of the Bermuda Triangle", 0),
      Row("Researchers develop a device that allows humans to understand animal languages", 0),
      Row("Scientists discover a species of fish that can play musical instruments", 0),
      Row("Astronauts organize a space-themed fashion show on the International Space Station", 0),
      Row("International competition for the best sandcastle construction", 0),
      Row("Government to implement a nationwide pillow fight day", 0),
      Row("New law requires citizens to carry a rubber chicken for good luck", 0),
      Row("Researchers develop a device that allows plants to send text messages", 0),
      Row("Penguins create a documentary on the art of belly sliding", 0),
      Row("Scientists create a pill that makes people immune to boredom", 0),
      Row("Astronauts organize a zero-gravity dance competition", 0),
      Row("Government plans to build a roller coaster around the Eiffel Tower", 0),
      Row("International competition for the best cloud-gazing experience", 0),
      Row("New law requires citizens to celebrate their pets' birthdays with a national holiday", 0),
      Row("Researchers develop a device that allows humans to fly like birds", 0),
      Row("Scientists discover a planet where chocolate grows on trees", 0),
      Row("Astronauts organize a space-themed cooking competition on Mars", 0),
      Row("Government to launch a mission to search for the mythical city of Atlantis", 0),
      Row("International competition for the best invention using recycled materials", 0),
      Row("New law requires citizens to take a daily dose of laughter for improved well-being", 0),
      Row("Researchers develop a device that allows humans to communicate with dolphins", 0),
      Row("Penguins start a podcast on the challenges of living in Antarctica", 0),
      Row("Scientists create a pill that allows people to understand the language of insects", 0),
      Row("Astronauts organize a space-themed art exhibition on the moon", 0),
      Row("Government plans to build an underwater city for marine life", 0),
      Row("International competition for the best underwater dance performance", 0),
      Row("New law requires citizens to take a daily break for cloud-watching", 0),
      Row("Researchers develop a device that allows humans to experience dreams of their choice", 0),
      Row("Scientists create a pill that makes people immune to Monday blues", 0),
      Row("Astronauts organize a space-themed music festival in orbit", 0),
      Row("Government to launch a mission to search for extraterrestrial intelligence", 0),
      Row("International competition for the best tree-climbing technique", 0),
      Row("New law requires citizens to celebrate the first day of spring with a nationwide picnic", 0),
      Row("Researchers develop a device that allows humans to understand the language of birds", 0),
      Row("Penguins organize a synchronized swimming competition in the Antarctic Ocean", 0),
      Row("Scientists create a pill that allows people to understand the language of whales", 0),
      Row("Astronauts organize a space-themed dance party on the International Space Station", 0),
      Row("Government plans to build a giant maze in the middle of a desert", 0),
      Row("International competition for the best sand sculpture of a mythical creature", 0),
      Row("New law requires citizens to participate in a weekly dance-off for community bonding", 0),
      Row("Researchers develop a device that allows humans to communicate with trees", 0),
      Row("Scientists create a pill that makes people immune to bad weather", 0),
      Row("Astronauts organize a space-themed film festival on Mars", 0),
      Row("Government to launch a mission to explore the mysteries of the deep sea", 0),
      Row("International competition for the best underwater treasure hunt", 0),
      Row("New law requires citizens to celebrate the first snowfall with a snowball fight", 0),
      Row("Researchers develop a device that allows humans to understand the language of elephants", 0),
      Row("Penguins start a campaign to raise awareness about climate change in Antarctica", 0),
      Row("Scientists create a pill that allows people to understand the language of bees", 0),
      Row("Astronauts organize a space-themed fashion week on the International Space Station", 0),
      Row("Government plans to build a giant umbrella to shade a city from the sun", 0),
      Row("International competition for the best sandcastle on a desert island", 0),
      Row("New law requires citizens to celebrate the first day of summer with a water balloon fight", 0),
      Row("Researchers develop a device that allows humans to understand the language of dolphins", 0),
      Row("Scientists create a pill that makes people immune to traffic jams", 0),
      Row("Astronauts organize a space-themed talent show on the moon", 0),
      Row("Government to launch a mission to create a sustainable floating city", 0),
      Row("International competition for the best invention using recycled ocean plastic", 0),
      Row("New law requires citizens to celebrate the first day of autumn with a pumpkin carving contest", 0)
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

    //dfOnlyWhitespace.write.format("csv").save("C:\\Users\\fnc_user\\Desktop\\laurea_magistrale_informatica\\ScalableCloud\\progetto\\FakeNewsClassificationWithDecisionTreeMR\\data\\dataset\\dataset.csv")

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
/*
    // Extract values from the "features" column and create a new DataFrame
    // .select("features").as[Vector]
    // .select("features", "label").as[(Vector, Int)]
    val extractedDF = rescaledData.flatMap { case Row(features: Vector, label: Integer) =>
      // Convert the vector to a Seq of Double values
      val featureValues = features.toArray.toSeq
      // Append the label to the sequence
        // Return a Row with feature values and label
      Row.fromSeq(featureValues)
    }
      //.toDF(schemaFinal: _*) // Use the header names directly
*/

    // Define a function to create a new dataframe for each row
    def createNewDataFrame(features: Vector, label: Integer): DataFrame = {
      val newRow = Row(features.toArray.toSeq, label)

      // Create a schema for the new dataframe
      val schemaFinal = StructType(vocabulary.init.map(fieldName => StructField(fieldName, DoubleType, nullable = false)) :+ StructField("label", IntegerType, nullable = false))

      // Create the new dataframe
      spark.createDataFrame(spark.sparkContext.parallelize(Seq(newRow)), schemaFinal)
    }

    // Apply the function to each row and union the results
    val final_dataset = rescaledData.rdd.map{case Row(features: Vector, label: Integer) => createNewDataFrame(features, label)}.reduce(_ union _)

/*
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
/*
      case Row(num: Int, text: Seq[_], features: Vector) =>
      println(s"final_tokens: [${text.mkString(", ")}] => \nVector: $features\n")
    case Row(num: Int, text: Vector, features: Vector) =>
      println(s"final_tokens: [${text}] => \nVector: $features\n")
    case Row(num: Int, text: Seq[_], features: Seq[_]) =>
      println(s"final_tokens: [${text.mkString(", ")}] => \nVector: $features\n")
 */
    }

    println(data)
    // Create the DataFrame
    val final_dataset = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

 */
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