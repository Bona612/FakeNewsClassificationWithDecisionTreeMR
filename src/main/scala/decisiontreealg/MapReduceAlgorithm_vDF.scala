package decisiontreealg

import org.apache.spark.sql.functions._
import decisiontree.{DecisionTree, Leaf, Node}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Encoders.row
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.RowEncoder
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, collect_list, explode, expr, lit, sort_array, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType}

import scala.math.log10


class MapReduceAlgorithm_vDF() {

  def startAlgorithm_vDF(dataset: DataFrame, maxDepth: Int): DecisionTree = {

    val cols = dataset.columns.dropRight(1) //discard class column

    val idx_label = dataset.columns.length - 1

    mainAlgorithm_vDF(dataset, cols, idx_label, None, 0)
  }

  private def mainAlgorithm_vDF(dataset: DataFrame, cols: Array[String], idx_label: Int, par: Option[Node], depthTree: Int): DecisionTree = {
    println("start calc entropy...")
    val (entropy, (maxClass, _), allTable) = calcEntropyTable_vDF_v2(dataset)

    if (entropy <= 0.3f || depthTree >= 100) { //stop check
      return Leaf(label = maxClass, parent = par)
    }

    println("start data preparation...")

    val countSplitDF = dataPreparation_vDF(dataset, cols, idx_label)

    val startTimeMillis = System.currentTimeMillis()

    if (countSplitDF == null) { //stop check
      return Leaf(label = maxClass, parent = par)
    }

    println("find best split...")
    val bestSplit = findBestSplit_vDF(countSplitDF, entropy, allTable.toInt)

    val (bestAttr, bestValue) = (bestSplit._1, bestSplit._2.toDouble)
    println(s"num partition dataset: ${dataset.rdd.getNumPartitions}")


    val greaterEqualDF = dataset.filter(col(bestAttr) >= bestValue)
    val lowerDF = dataset.filter(col(bestAttr) < bestValue)


    val currentNode = Node(bestAttr, bestValue, null, null, par)

    val endTimeMillis = System.currentTimeMillis()
    // Calculate the elapsed time
    val elapsedTimeMillis = endTimeMillis - startTimeMillis
    // Print the result
    println(s"Elapsed Time: $elapsedTimeMillis milliseconds")
    println("iterate right and left...")


    val right = mainAlgorithm_vDF(greaterEqualDF, cols, idx_label, Option(currentNode), depthTree + 1)
    val left = mainAlgorithm_vDF(lowerDF, cols, idx_label, Option(currentNode), depthTree + 1)

    //dataset.unpersist()

    currentNode.insertLeftChild(left)
    currentNode.insertRightChild(right)

    currentNode
  }


  private def calcEntropyTable_vDF(dataset: DataFrame): (Double, (String, Double), Double) = {
    val log2: Double => Double = (x: Double) =>
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)

    val calcEntropy: Double => Double = (p: Double) => -p * log2(p)

    val countLabel0 = dataset.withColumn("value", col("ground_truth").cast(IntegerType))
      .filter("value = 0")
      .count()
      .toDouble

    val countLabel1 = dataset.withColumn("value", col("ground_truth").cast(IntegerType))
      .filter("value = 1")
      .count()
      .toDouble

    val allValue = countLabel0 + countLabel1

    var maxKey: (String, Double) = null
    if (countLabel0 > countLabel1)
      maxKey = ("0", countLabel0)
    else
      maxKey = ("1", countLabel1)

    var entropy = 0.0

    if (allValue > 0.0)
      entropy = (calcEntropy(countLabel0 / allValue) + calcEntropy(countLabel1 / allValue)).abs

    println("entropy: " + entropy)
    println("0: " + countLabel0, "1: " + countLabel1)

    (entropy, maxKey, allValue)
  }

  private def calcEntropyTable_vDF_v2(dataset: DataFrame): (Double, (String, Double), Double) = {
    val log2: Double => Double = (x: Double) =>
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)

    val calcEntropy: Double => Double = (p: Double) => -p * log2(p)

    dataset.show()

    val countDF = dataset
      .groupBy("ground_truth")
      .agg(count("*").cast(DoubleType).alias("count"))
    countDF.show()

    val countLabel0Option = Option(countDF.filter(col("ground_truth") === 0).select("count").take(1).headOption)
    val countLabel0 = countLabel0Option.flatten.map(_.getAs[Double]("count")).getOrElse(0.0)
    val countLabel1Option = Option(countDF.filter(col("ground_truth") === 1).select("count").take(1).headOption)
    val countLabel1 = countLabel1Option.flatten.map(_.getAs[Double]("count")).getOrElse(0.0)

    //val countLabel0 = countDF.filter(col("ground_truth") === 0).select("count").first().getAs[Double]("count")
    //val countLabel1 = countDF.filter(col("ground_truth") === 1).select("count").first().getAs[Double]("count")

    val allValue = countLabel0 + countLabel1

    val maxKey = if (countLabel0 > countLabel1) ("0", countLabel0) else ("1", countLabel1)

    val entropy = if (allValue > 0.0) {
      (calcEntropy(countLabel0 / allValue) + calcEntropy(countLabel1 / allValue)).abs
    } else {
      0.0
    }

    println(s"Entropy: $entropy")
    println(s"Label 0 count: $countLabel0, Label 1 count: $countLabel1")

    (entropy, maxKey, allValue)
  }

  def dataPreparation_vDF(dataset: DataFrame, cols: Array[String], idx_label: Int): DataFrame = {
    //if (dataset.drop("ground_truth").distinct().head(2).length > 1) {
    if (dataset.select("ground_truth").distinct().count() > 1) {
      println(s"is cached: ${dataset.storageLevel.useMemory}")

      val attr_columns = dataset.columns.filter(!_.equals("ground_truth"))


      val attrTableArray: Array[DataFrame] = attr_columns.map(colName => {
        val distinctDF = dataset.select(colName).distinct().groupBy().agg(collect_list(colName).alias("distinct_values"))
        //distinctDF.show()
        //val arrayColumnDF = distinctDF.groupBy().agg(collect_list(colName).alias("distinct_values"))
        //arrayColumnDF.show()
        val sortedDF = distinctDF.withColumn("sorted_values", sort_array(col("distinct_values")))
        //sortedDF.show()

        /*val attrTableDF = dataset
          .select(colName, "ground_truth")
          .withColumnRenamed(colName, "attr_value")
          .withColumn("attr", lit(colName))
          .crossJoin(sortedDF)*/

        val broadcastSortedDF = broadcast(sortedDF)
        val attrTableDF = dataset
          .select(colName, "ground_truth")
          .withColumnRenamed(colName, "attr_value")
          .withColumn("attr", lit(colName))
          .join(broadcastSortedDF, lit(true))
        attrTableDF.show()
        /*val attrTableDF = dataset
          .join(sortedDF, "distinct_values", "left_outer")
          .select(colName, "ground_truth", "sorted_values").withColumnRenamed(colName, "attr_value")
*/
        attrTableDF
      })


      /*val attrTableArray: Array[DataFrame] = attr_columns.map(colName => {
        val distinctDF = dataset.select(colName).distinct()
        // Aggregate distinct values into an array
        val arrayColumnDF = distinctDF.groupBy().agg(collect_list(colName).alias("distinct_values"))
        // Use Spark SQL's sort_array function
        val sortedDF = arrayColumnDF.withColumn("sorted_values", sort_array(col("distinct_values")))
        //sortedDF.show()
        // Aggregate distinct values into an array
        val arrayValue: Seq[Double] = sortedDF.collect()(0).getAs[Seq[Double]]("sorted_values")

        // Broadcast the array value
        val valueLabelDF = dataset.select(colName, "ground_truth").withColumnRenamed(colName, "attr_value")
        //valueLabelDF.show()
        valueLabelDF.withColumn("attr", lit(colName)).withColumn("sorted_values", lit(arrayValue))
      })*/

      val attrTable: DataFrame = attrTableArray.reduce(_ union _) //unionByName
      attrTable.show()


      // Use Spark SQL's transform function to calculate the average of adjacent values
      /*val splitPointsDF: DataFrame = attrTable.withColumn("split_points",
        expr("transform(slice(sorted_values, 1, size(sorted_values) - 1), (x, i) -> (x + sorted_values[i + 1]) / 2)"))
*/
      val splitPointsDF = attrTable
        .withColumn("split_points", expr("transform(array_remove(sorted_values, sorted_values[size(sorted_values) - 1]), (x, i) -> (x + sorted_values[i + 1]) / 2)"))

      splitPointsDF.show()

      // Explode the values column
      val explodedDF = splitPointsDF.withColumn("split_point", explode(col("split_points")))
      explodedDF.show()

      val countTableSplitDF = explodedDF.withColumn("comparison", when(col("attr_value") >= col("split_point"), ">=").otherwise("<"))
      countTableSplitDF.show()

      // Group by specified columns and count the rows
      val countSplittDF = countTableSplitDF
        .groupBy("attr", "ground_truth", "split_point", "comparison")
        .agg(count("*").alias("count"))
      countSplittDF.show()

      countSplittDF
    }
    else {
      dataset.limit(0)
    }
  }

  private def findBestSplit_vDF(countValueDF: DataFrame, entropyAll: Double, allTable: Int): (String, Double) = {

    val log2: Double => Double = (x: Double) =>
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)

    val calcEntropy: Double => Double = (p: Double) => -p * log2(p) //roundDouble(-p * log2(p))

    val allTableSplit = countValueDF
      .groupBy("attr", "split_point", "comparison")
      .agg(sum("count").as("sum_count"))
    //allTableSplit.show()

    //join between countTable for splitvalues and allTable for splitvalues
    val infoDF = countValueDF
      .join(allTableSplit, Seq("attr", "split_point", "comparison"))
    //infoDF.show()


    //gainratio table
    /*val gainRatioDF = infoDF.select("gorund_truth", "count", "sum_count")
      .withColumn("entropy", expr("-(count / sum_count * log(count / sum_count)))"))
      .select("sum_count", "entropy")*/
    val gainRatioDF = infoDF
      .select("attr", "split_point", "comparison", "count", "sum_count")
      .withColumn("entropy", -col("count") / col("sum_count") * log(col("count") / col("sum_count")) / log(lit(2)))
    //gainRatioDF.show()


    val finalDF = gainRatioDF
      //.select("attr_value", "split_point", "comparison", "sum_count", "entropy")
      .groupBy("attr", "split_point", "comparison")
      .agg(sum("entropy").as("entropy_split"), first("sum_count").as("sum_count"))
      //.select("attr_value", "split_point", "comparison", "sum_count", "entropy_split")
      .withColumn("info", col("sum_count") / lit(allTable) * col("entropy_split"))
      .withColumn("split_info", -col("sum_count") / lit(allTable) * log(col("sum_count") / lit(allTable)) / log(lit(2))) // math.log(2)
      //.select("attr", "split_point", "info", "split_info")
      .groupBy("attr", "split_point")
      .agg(sum("info").as("info_sum"), sum("split_info").as("split_info_sum"))
      .withColumn("final", (lit(entropyAll) - col("info_sum")) / col("split_info_sum"))
    //finalDF.show()

    println("gainRatioTable")

    //val argmax = finalDF.select("attr", "split_point", "final").agg(max("final"))
    val argmax = finalDF.select("attr", "split_point", "final").orderBy(col("final").desc)
    //argmax.show()

    (argmax.collect()(0).getAs[String]("attr"), argmax.collect()(0).getAs[Double]("split_point"))
  }


  private def findBestSplit_vDF_v2(countValueDF: DataFrame, entropyAll: Double, allTable: Int): (String, Double) = {
    val log2: Double => Double = (x: Double) =>
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)

    val calcEntropy: Double => Double = (p: Double) => -p * log2(p)

    val allTableSplit = countValueDF
      .groupBy("attr", "split_point", "comparison")
      .agg(sum("count").as("sum_count"))

    val infoDF = countValueDF
      .join(allTableSplit, Seq("attr", "split_point", "comparison"))

    val gainRatioDF = infoDF
      .select("attr", "split_point", "comparison", "count", "sum_count")
      .withColumn("entropy", -col("count") / col("sum_count") * log(col("count") / col("sum_count")) / log(lit(2)))

    val finalDF = gainRatioDF
      .groupBy("attr", "split_point", "comparison")
      .agg(sum("entropy").as("entropy_split"), first("sum_count").as("sum_count"))
      .withColumn("info", col("sum_count") / lit(allTable) * col("entropy_split"))
      .withColumn("split_info", -col("sum_count") / lit(allTable) * log(col("sum_count") / lit(allTable)) / log(lit(2)))
      .groupBy("attr", "split_point")
      .agg(sum("info").as("info_sum"), sum("split_info").as("split_info_sum"))
      .withColumn("final", (lit(entropyAll) - col("info_sum")) / col("split_info_sum"))

    val bestSplit = finalDF.orderBy(col("final").desc).select("attr", "split_point").first()

    (bestSplit.getAs[String]("attr"), bestSplit.getAs[Double]("split_point"))
  }


}