package decisiontreealg

import decisiontree.{DecisionTree, Leaf, Node}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import scala.math.log10


class MapReduceAlgorithm2() {

  def startAlgorithm(dataset : DataFrame): DecisionTree = {

    val cols: Array[String] = dataset.columns.dropRight(1) //discard class column

    val idx_label = dataset.columns.length-1

    mainAlgorithm(dataset, cols, None, idx_label, 0)
  }

  private def mainAlgorithm(dataset: DataFrame, cols: Array[String], par: Option[Node], idx_label: Int, depthTree: Int): DecisionTree = {

    println("start calc entropy...")
    //entropy to understand if we arrived in the situation of only or most of instance of a class. (0 is purity)
    val (entropy, (maxClass, _), allTable) = calcEntropyTable(dataset, idx_label)

    if (entropy <= 0.3f || depthTree >= 100) { //stop check
      return Leaf(label = maxClass, parent = par)
    }

    println("start data preparation...")
    val countTableSplit = dataPreparation(dataset, cols, idx_label)

    if (countTableSplit == null) { //stop check
      return Leaf(label = maxClass, parent = par)
    }

    println("find best split...")
    val bestAttr = findBestSplit(countTableSplit, entropy, allTable)

    println("foreach...")
    val distinct_vals = dataset.select(bestAttr).distinct().collect()

    var child_list: Array[DecisionTree] = Array()
    distinct_vals.foreach{

      row =>


        val currentNode = Node(bestAttr, row.getAs[Double](0), null, null, par)
        child_list :+ currentNode
        val new_dataset = dataset.filter(col(bestAttr) === row.getAs[Double](0)).drop(bestAttr)
        println(new_dataset.columns.dropRight(1).foreach(print))
        val new_node = mainAlgorithm(new_dataset, new_dataset.columns.dropRight(1), Option(currentNode), idx_label-1, depthTree+1)
        currentNode.insertLeftChild(new_node)
    }


    null
  }

  private def calcEntropyTable(dataset: DataFrame, idx_label: Int): (Double, (String, Double), Double) = {

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

  def dataPreparation(dataset: DataFrame, cols: Array[String], idx_label: Int) :RDD[((String, Double),( Int, Int))] = {

    /*
    * Map of splitpoints (mean of adiacent values) for attributes, if attr have only one value we discard it
    */

    // Calculate distinct values for each column
    /*val distinctValues = cols.map(c => collect_set(col(c)).alias(s"distinct_$c"))
    val distinctTable = dataset.agg(distinctValues.head, distinctValues.tail: _*)*/

    if (dataset.drop("ground_truth").distinct().head(2).length > 1) {


      val countTableSplit = dataset.rdd
        .flatMap {
          row =>
            cols.zipWithIndex.map{
              case (col, idx) =>
                ((col, row(idx_label).asInstanceOf[Int], row(idx).asInstanceOf[Double]), 1)
            }
        }
        .reduceByKey(_+_)
        .map {

           case ((attr, label, value), count) =>
             ((attr, value), (label, count))
        }


      countTableSplit
    }
    else
      null

  }


  private def findBestSplit(countTableValue: RDD[((String, Double), (Int, Int))], entropyAll : Double, allTable: Double): String = {

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
