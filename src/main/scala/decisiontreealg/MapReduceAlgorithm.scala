package decisiontreealg

import decisiontree.{DecisionTree, Leaf, Node}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{assert_true, col, collect_list, collect_set, countDistinct, expr, lit, udf}
import org.apache.spark.sql.{DataFrame, Dataset, Row, functions}
import spire.compat.ordering

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math.log10


class MapReduceAlgorithm() {

  def startAlgorithm(dataset: DataFrame, maxDepth: Int): DecisionTree = {

    val cols = dataset.columns.drop(1).dropRight(1) //discard class column

    val idx_label = dataset.columns.length-1

    val attrTable = createAttrTable(dataset, idx_label, cols).persist().repartition(6)

    println(s"num partition attrTable: ${attrTable.partitions.length}")
    mainAlgorithm(dataset, attrTable, attrTable, None, 0)
  }

  private def createAttrTable(dataset: DataFrame, idx_label:Int, cols: Array[String]): RDD[((String, Int), (Int, Double))] = {


    dataset.rdd.flatMap {
        row =>
          cols.zipWithIndex.map{
            case (col, idx) =>
              ((col, row(0).asInstanceOf[Int]), (row(idx_label).asInstanceOf[Int], row(idx+1).asInstanceOf[Double]))
          }
    }


  }

  private def filterAttrTable(attrTable: RDD[((String, Int), (Int, Double))], removeRows: Seq[Int]) : RDD[((String, Int), (Int, Double))] = {

    attrTable.filter{case ((_,row_idx), (_,_)) => removeRows.contains(row_idx)}
  }

  private def getRowIndex(dataset: DataFrame): Seq[Int] = {

    dataset.select("Index").collect().map(row => row(0).asInstanceOf[Int])
  }
  private def mainAlgorithm(dataset: DataFrame, mainAttrTable:RDD[((String, Int), (Int, Double))], attrTable: RDD[((String, Int), (Int, Double))], par: Option[Node], depthTree: Int): DecisionTree = {


    println("start calc entropy...")
    //entropy to understand if we arrived in the situation of only or most of instance of a class. (0 is purity)
    val (entropy, (maxClass, _), allTable) = calcEntropyTable(dataset)

    if (entropy <= 0.3f || depthTree >= 100) { //stop check
      return Leaf(label = maxClass, parent = par)
    }

    println("start data preparation...")

    println(s"num partition attrTable: ${attrTable.partitions.length}")
    val countTableSplit = dataPreparation(dataset, attrTable)

    if (countTableSplit == null) { //stop check
      return Leaf(label = maxClass, parent = par)
    }

    println("find best split...")
    val bestSplit = findBestSplit(countTableSplit, entropy, allTable.toInt)

    val (bestAttr, bestValue) = (bestSplit._1, bestSplit._2.toDouble)


    println(s"num partition dataset: ${dataset.rdd.getNumPartitions}")
    val greaterDataset = dataset.filter {

      row =>
        val index = row.schema.fieldIndex(bestAttr)

        row(index).asInstanceOf[Double] >= bestValue
    }.cache()

    val lowerDataset = dataset.filter {

      row =>
        val index = row.schema.fieldIndex(bestAttr)

        row(index).asInstanceOf[Double] < bestValue
    }.cache()



    val rightAttrTable = filterAttrTable(mainAttrTable, getRowIndex(greaterDataset)).persist()
    val leftAttrTable = filterAttrTable(mainAttrTable, getRowIndex(lowerDataset)).persist()

    attrTable.unpersist()
    val currentNode = Node(bestAttr, bestValue, null, null, par)

    println("iterate right and left...")

    val right = mainAlgorithm(greaterDataset, mainAttrTable, rightAttrTable, Option(currentNode), depthTree+1)
    val left = mainAlgorithm(lowerDataset, mainAttrTable, leftAttrTable, Option(currentNode), depthTree+1)

    currentNode.insertLeftChild(left)
    currentNode.insertRightChild(right)

    currentNode
  }

  private def calcEntropyTable(dataset: DataFrame): (Double, (String, Double), Double) = {

    val log2: Double => Double = (x: Double) =>
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)


    val roundDouble : Double => Double = (value: Double) =>
      BigDecimal(value).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble

    val calcEntropy: Double => Double = (p: Double) => -p * log2(p)

    /*
    val countLabel0 = dataset.filter(row => row(idx_label).toString.toInt == 0).count().toDouble
    val countLabel1 = dataset.filter(row => row(idx_label).toString.toInt == 1).count().toDouble


    val countLabel0 = dataset.filter(col("ground_truth") === "0").count().toDouble
    val countLabel1 = dataset.filter(col("ground_truth") === "1").count().toDouble
    */

    var counts = dataset.groupBy("ground_truth").count().collect()

    val countLabel0 = counts.find(row => row.getAs[Int]("ground_truth") == 0).map(_.getAs[Long]("count")).getOrElse(0L).toDouble
    val countLabel1 = counts.find(row => row.getAs[Int]("ground_truth") == 1).map(_.getAs[Long]("count")).getOrElse(0L).toDouble

    val allValue = countLabel0 + countLabel1

    var maxKey: (String, Double) = null
    if (countLabel0 > countLabel1)
      maxKey = ("0", countLabel0)
    else
      maxKey = ("1", countLabel1)

    var entropy = 0.0

    if (allValue > 0.0)
      entropy = roundDouble((calcEntropy(countLabel0 / allValue) + calcEntropy(countLabel1 / allValue)).abs)

    println("entropy: " + entropy)
    println("0: " + countLabel0, "1: " + countLabel1)

    counts = null

    (entropy, maxKey, allValue)
  }

  def dataPreparation(dataset: DataFrame, attrTable: RDD[((String, Int), (Int, Double))]) :RDD[((String, String),( Int, Int))] = {

    /*
    * Map of splitpoints (mean of adiacent values) for attributes, if attr have only one value we discard it
    */

    // Calculate distinct values for each column
    /*val distinctValues = cols.map(c => collect_set(col(c)).alias(s"distinct_$c"))
    val distinctTable = dataset.agg(distinctValues.head, distinctValues.tail: _*)*/

    if (dataset.drop(Seq("ground_truth","Index"): _*).distinct().head(2).length > 1) {

    /*
      val attrTableArray: Array[DataFrame] = cols.map(colName => {
        val distinctDF = dataset.select(colName).distinct()
        // Aggregate distinct values into an array
        val arrayColumnDF = distinctDF.groupBy().agg(collect_list(colName).alias("distinct_values"))

        // Aggregate sorted distinct values into an array
        val arrayValue: Seq[Double] = arrayColumnDF.collect()(0).getAs[Seq[Double]]("distinct_values")
        // Broadcast the array value
        val valueLabelDF = dataset.select(colName, "ground_truth").withColumnRenamed(colName, "attr_value")

        valueLabelDF.withColumn("attr", lit(colName)).withColumn("values", lit(arrayValue))
      })

      val myFunctionUDF = udf((seq: Seq[Double]) => seq.sorted.sliding(2).toList.map(pair => (pair.head + pair(1)) / 2.0))

      val countTableSplit = attrTableArray.reduce(_ unionByName _)
        .filter(functions.size(col("values")) > 1)
        .rdd.flatMap {
          row =>

            row(3).asInstanceOf[Seq[Double]].sorted.sliding(2).toList.map(pair => (pair.head + pair(1)) / 2.0).map{

              value1 => {

                if (row(0).asInstanceOf[Double] < value1)
                  ((row(2).asInstanceOf[String], row(1).asInstanceOf[Int], value1.toString + "<"), 1)
                else
                  ((row(2).asInstanceOf[String], row(1).asInstanceOf[Int], value1.toString + ">="), 1)

              }
            }

        }

      */

      val intermed_attrTable = attrTable.map{ case (key, value) => (key._1, value)}.cache()

      println(s"num partition intermed_attrTable: ${intermed_attrTable.partitions.length}")

      val splitPointsTable = intermed_attrTable.combineByKey(
        createCombiner = (value: (Int, Double)) => Seq(value._2),
        mergeValue = (accumulator: Seq[Double], value: (Int, Double)) => accumulator :+ value._2,
        mergeCombiners = (accumulator1: Seq[Double],accumulator2: Seq[Double]) => accumulator1 ++ accumulator2
      )
      .mapValues(_.distinct.sorted)
      .filter(_._2.length > 1)
      .mapValues(_.sliding(2).toList.map(pair => (pair.head + pair(1)) / 2.0))

      // count table respect to (attr splitvalue, label)
      val countTableSplit = intermed_attrTable.rightOuterJoin(splitPointsTable)
        .flatMap {

          case (attr, (label_and_val: Option[(Int, Double)], val_list)) =>

            val_list.map( //divide in < and >= instances taking in account class label for each splitpoint of this attr

              value1 => {

                if (label_and_val.get._2 < value1)
                  ((attr, label_and_val.get._1, value1.toString + "<"), 1)
                else
                  ((attr, label_and_val.get._1, value1.toString + ">="), 1)

              }
            )

        }
        .reduceByKey(_ + _).map {

           case ((attr, label, value), count) =>
             ((attr, value), (label, count))
        }

      countTableSplit.cache()
    }
    else
      null

  }


  private def findBestSplit(countTableValue: RDD[((String, String), (Int, Int))], entropyAll : Double, allTable: Int): (String, String) = {

    val log2: Double => Double = (x: Double) =>
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)


    val roundDouble : Double => Double = (value: Double) =>
      BigDecimal(value).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble

    val calcEntropy: Double => Double = (p: Double) => -p * log2(p) //roundDouble(-p * log2(p))

    val allTableSplit = countTableValue.mapValues{case (_,count) => count}
      .reduceByKey(_+_)

    //join between countTable for splitvalues and allTable for splitvalues
    val infoTable = countTableValue
      .join(allTableSplit)

    //gainratio table
    val gainRatioTable = infoTable.mapValues {
        case ((_, count), all) => (all, calcEntropy(count.toDouble / all.toDouble))
      }
      .reduceByKey { case ((all,entropyAttr1),(_,entropyAttr2)) => (all, entropyAttr1+entropyAttr2)}
      .map{
        case((attr,value),(allSplit,entropySplit)) =>

          // compute info and splitinfo for each (attr, splitvalue)
          val p = allSplit.toDouble / allTable.toDouble
          val info = p * entropySplit
          val splitinfo = calcEntropy(p)

          if (value.contains(">="))
            ((attr, value.split(">=").apply(0)), (info,splitinfo))
          else
            ((attr, value.split("<").apply(0)), (info,splitinfo))
      }
      .reduceByKey{ case((info1, splitinfo1), (info2, splitinfo2)) => (info1+info2, splitinfo1+splitinfo2)}
      .mapValues { case (info,split) =>

        (entropyAll-info)/split //gain / split
      }

    println("gainRatioTable")

    val argmax = gainRatioTable.reduce((x1, x2) => if (x1._2 > x2._2) x1 else x2)._1

    println(argmax)

    argmax

  }

}
