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

    val attrTable = createAttrTable(dataset, idx_label, cols).persist()

    println(s"num partition attrTable: ${attrTable.partitions.length}")
    mainAlgorithm(dataset, attrTable, None, idx_label, 0)
  }

  private def createAttrTable(dataset: DataFrame, idx_label:Int, cols: Array[String]): RDD[((String, Int, Int, Double), Int)] = {


    dataset.rdd.flatMap {
        row =>
          cols.zipWithIndex.map{
            case (col, idx) =>
              ((col,  row(0).asInstanceOf[Int], row(idx_label).asInstanceOf[Int], row(idx+1).asInstanceOf[Double]), 1)
          }
      }

  }

  private def filterAttrTable(attrTable: RDD[((String, Int, Int, Double), Int)], removeRows: Seq[Int], bestAttr: String) : RDD[((String, Int, Int, Double), Int)] = {

    attrTable.filter{case ((attr,row_idx, _ ,_), _) => removeRows.contains(row_idx) && attr != bestAttr}
  }

  private def getRowIndex(dataset: DataFrame): Seq[Int] = {

    dataset.select("Index").collect().map(row => row(0).asInstanceOf[Int])
  }

  private def removeUniqueCols(dataset: DataFrame):DataFrame = {

    // Identify columns with unique values
    val uniqueColumns = dataset.columns.drop(1).dropRight(1).filter { colName =>
      dataset.select(col(colName)).distinct().count() == 1
    }

    // Select columns with non-unique values
    dataset.select(dataset.columns.filter(!uniqueColumns.contains(_)).map(col): _*)
  }
  private def mainAlgorithm(dataset: DataFrame, attrTable: RDD[((String, Int, Int, Double), Int)], par: Option[Node], idx_label: Int,depthTree: Int): DecisionTree = {


    println("start calc entropy...")
    //entropy to understand if we arrived in the situation of only or most of instance of a class. (0 is purity)
    val (entropy, (maxClass, _), allTable) = calcEntropyTable(dataset)

    if (entropy <= 0.3f || depthTree >= 100) { //stop check
      return Leaf(label = maxClass, parent = par)
    }

    println("start data preparation...")

    println(s"num partition attrTable: ${attrTable.partitions.length}")
    val (rowsAttrTable, distinctTable, countTableSplit) = dataPreparation(dataset, attrTable)

    if (countTableSplit == null) { //stop check
      return Leaf(label = maxClass, parent = par)
    }

    println("find best split...")
    val bestAttr = findBestSplit(countTableSplit, entropy, allTable.toInt)

    val distinct_vals: Seq[Double] = distinctTable.lookup(bestAttr).head

    println("foreach...")
    //val distinct_vals = dataset.select(bestAttr).distinct().collect().map(row => row(0).asInstanceOf[Double])

    var child_list: Array[DecisionTree] = Array()
    distinct_vals.foreach {

      value =>

        println(s"best attr: $bestAttr , value: $value, num dist: ${distinct_vals.length}")
        val currentNode = Node(bestAttr, value, null, null, par)
        child_list :+ currentNode
        val new_dataset = dataset.filter(col(bestAttr) === value).drop(bestAttr).persist()

        val row_idx = rowsAttrTable.lookup((bestAttr, value)).head
        val new_attrTable = filterAttrTable(attrTable, row_idx, bestAttr).persist()

        val new_node = mainAlgorithm(new_dataset, new_attrTable, Option(currentNode), idx_label - 1, depthTree + 1)
        currentNode.insertLeftChild(new_node)
    }


    println(s"num partition dataset: ${dataset.rdd.getNumPartitions}")


    null
  }

  private def calcEntropyTable(dataset: DataFrame): (Double, (String, Double), Double) = {

    val log2: Double => Double = (x: Double) =>
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)


    val roundDouble : Double => Double = (value: Double) =>
      BigDecimal(value).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble

    val calcEntropy: Double => Double = (p: Double) => -p * log2(p)

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

  def dataPreparation(dataset: DataFrame, attrTable: RDD[((String, Int, Int, Double), Int)]): (RDD[((String, Double), Seq[Int])], RDD[(String, Seq[Double])], RDD[((String, Double), (Int, Int))]) = {

    /*
    * Map of splitpoints (mean of adiacent values) for attributes, if attr have only one value we discard it
    */


    if (dataset.drop(Seq("ground_truth", "Index"): _*).distinct().head(2).length > 1) {

      val rowsAttrTable = attrTable
        .map { case ((attr, row_idx, _, value), _) => ((attr, value), row_idx) }
        .combineByKey(
          createCombiner = (value: Int) => Seq(value),
          mergeValue = (accumulator: Seq[Int], value: Int) => accumulator :+ value,
          mergeCombiners = (accumulator1: Seq[Int], accumulator2: Seq[Int]) => accumulator1 ++ accumulator2
        )
        .persist()

      val distinctTable = attrTable
        .map { case ((attr, _, _, value), _) => (attr, value) }
        .combineByKey(
          createCombiner = (value: Double) => Seq(value),
          mergeValue = (accumulator: Seq[Double], value: Double) => accumulator :+ value,
          mergeCombiners = (accumulator1: Seq[Double], accumulator2: Seq[Double]) => accumulator1 ++ accumulator2
        )
        .mapValues(_.distinct)
        .filter(_._2.length > 1)
        .persist()

      distinctTable.foreach(println)

      val keys = distinctTable.keys.collect()

      val countTable = attrTable
        .filter(item => keys.contains(item._1._1))
        .map { case ((attr, _, label, value), count) => ((attr, label, value), count) }
        .reduceByKey(_ + _)
        .map {

          case ((attr, label, value), count) =>
            ((attr, value), (label, count))
        }
        .persist()

      (rowsAttrTable, distinctTable, countTable)
    }
    else
      null

  }


  private def findBestSplit(countTableValue: RDD[((String, Double), (Int, Int))], entropyAll : Double, allTable: Int): String = {

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
        case((attr,_),(allSplit,entropySplit)) =>

          // compute info and splitinfo for each (attr, splitvalue)
          val p = allSplit.toDouble / allTable.toDouble
          val info = p * entropySplit
          val splitinfo = calcEntropy(p)


          (attr, (info,splitinfo))

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
