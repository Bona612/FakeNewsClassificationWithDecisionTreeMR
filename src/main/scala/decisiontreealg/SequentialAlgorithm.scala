package decisiontreealg

import decisiontree.{Leaf, Node, TreeNode}
import org.apache.spark.sql.{DataFrame, Row}

import scala.math.log10

class SequentialAlgorithm() {

  var maxDepth = 0

  var idx_label = 0

  def startAlgorithm(dataset: DataFrame, max: Int, countLabel0: Double, countLabel1: Double): TreeNode = {

    idx_label = dataset.columns.length-1

    //val sparkContext = dataset.rdd.sparkContext
    //val cols: Broadcast[Array[(String, Int)]] = sparkContext.broadcast(dataset.columns.drop(1).dropRight(1).zipWithIndex)

    val cols = dataset.columns.drop(1).dropRight(1).zipWithIndex
    maxDepth = max
    val dsArray = dataset.collect()

    mainAlgorithm(dsArray, cols, None, idx_label, 0, countLabel0, countLabel1)
  }

  private def mainAlgorithm(dataset: Array[Row],
                            cols: Array[(String,Int)],
                            par: Option[TreeNode],
                            idx_label: Int,
                            depthTree: Int,
                            countLabel0: Double,
                            countLabel1: Double): TreeNode = {


    //println(s"dataset repartition: ${dataset.rdd.partitions.length}")
    println("start calc entropy...")
    //entropy to understand if we arrived in the situation of only or most of instance of a class. (0 is purity)
    val (entropy, (maxClass, _), allTable) = calcEntropyTable(countLabel0, countLabel1)

    if (entropy <= 0.3f || depthTree >= maxDepth) { //stop check
      return Leaf(par, maxClass.toInt)
    }

    println("start data preparation...")


    val (countLabel, countTable) = dataPreparation(dataset, cols)//attrTable)

    if (countTable == null) { //stop check
      return Leaf(par, maxClass.toInt)
    }

    println("find best split...")

    val bestAttr = findBestSplit(countTable, entropy, allTable.toInt)

    bestAttr match {

      case Some(result) =>


        val bestAttr = result._1
        val bestValue = result._2

        println(bestAttr,bestValue)

        val greaterDataset = dataset.filter {

          row =>
            val index = row.schema.fieldIndex(bestAttr)

            row(index).asInstanceOf[Double] >= bestValue
        }

        val lowerDataset = dataset.filter {

          row =>
            val index = row.schema.fieldIndex(bestAttr)

            row(index).asInstanceOf[Double] < bestValue
        }

        val currentNode = Node(bestValue, bestAttr, null, null, par)

        println("iterate right and left...")

        var countSeq = countLabel.get(bestAttr,0,bestValue,true)

        val new_greaterCountLabel0 = if(countSeq.nonEmpty) countSeq.head else 0

        countSeq = countLabel.get(bestAttr,1,bestValue,true)
        val new_greaterCountLabel1 = if (countSeq.nonEmpty) countSeq.head else 0

        countSeq = countLabel.get(bestAttr,0,bestValue,false)
        val new_lowerCountLabel0 = if (countSeq.nonEmpty) countSeq.head else 0

        countSeq = countLabel.get(bestAttr,1,bestValue,false)
        val new_lowerCountLabel1 = if (countSeq.nonEmpty) countSeq.head else 0

        val right = mainAlgorithm(greaterDataset, cols , Option(currentNode), idx_label, depthTree+1, new_greaterCountLabel0, new_greaterCountLabel1)
        val left = mainAlgorithm(lowerDataset, cols, Option(currentNode), idx_label, depthTree+1, new_lowerCountLabel0, new_lowerCountLabel1)

        currentNode.addLeft(left)
        currentNode.addRight(right)

        currentNode

      case None =>
        Leaf(par, maxClass.toInt)
    }


  }

  private def calcEntropyTable(countLabel0: Double, countLabel1: Double): (Double, (String, Double), Double) = {

    val log2: Double => Double = (x: Double) =>
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)


    val roundDouble : Double => Double = (value: Double) =>
      BigDecimal(value).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble

    val calcEntropy: Double => Double = (p: Double) => -p * log2(p)

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


    (entropy, maxKey, allValue)
  }

  def dataPreparation(dataset: Array[Row],
                      cols: Array[(String, Int)])
  : (Map[(String, Int, Double, Boolean), Double], Array[((String, Double, Boolean), Double)]) = {

    /*
    * Map of splitpoints (mean of adiacent values) for attributes, if attr have only one value we discard it
    */


    //if (dataset.drop(Seq("ground_truth", "Index"): _*).distinct().head(2).length > 1) {

    val attrTable = dataset
      .flatMap {
        row =>
          cols.map {
            case (col, idx) =>
              ((col, row(idx_label).asInstanceOf[Int], row(idx + 1).asInstanceOf[Double]), 1.0)
          }
      }
      .groupBy(_._1)
      .mapValues(_.map(_._2).sum)
      .toArray
      .map {

        case ((attr, label, value), count) =>

          (attr, (label, value, count))
      }


    var splitPointsTable = attrTable
      .map(pair => (pair._1, pair._2._2))
      .groupBy(_._1)
      .mapValues(_.map(_._2).distinct.sorted.toSeq)
      .filter(_._2.length > 1)


    var countLabelSplit: Map[(String, Int, Double, Boolean), Double] = null
    var countTableSplit: Array[((String, Double, Boolean), Double)] = null

    splitPointsTable = splitPointsTable
      .mapValues(_.sliding(2).toList.map(pair => (pair.head + pair(1)) / 2.0))

    countLabelSplit = attrTable
      .map { case (key, (label, value, count)) =>
        (key, (label, value, count, splitPointsTable.getOrElse(key, Seq.empty[Double])))
      }
      .flatMap {
        case (attr, (label, value, count, val_list)) =>

          val_list.map( //divide in < and >= instances taking in account class label for each splitpoint of this attr

            value1 => ((attr, label, value1, if (value < value1) false else true), count)
          )

      }
      .groupBy(_._1)
      .mapValues(_.map(_._2).sum)

    countTableSplit = countLabelSplit
      .toArray
      .map {

        case ((attr, _, value, split), count) =>
          ((attr, value, split), count)
      }

    (countLabelSplit, countTableSplit)


  }


  private def findBestSplit(countTableValue: Array[((String, Double, Boolean), Double)], //Set[Int])],
                            entropyAll: Double, allTable: Double): Option[(String, Double)] = {


    val log2: Double => Double = (x: Double) =>
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)

    val roundDouble: Double => Double = (value: Double) =>
      BigDecimal(value).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble

    val calcEntropy: Double => Double = (p: Double) => -p * log2(p) //roundDouble(-p * log2(p))

    val allTableSplit = countTableValue
      .groupBy(_._1)
      .mapValues(_.map(_._2).sum)//.coalesce(2)

    //join between countTable for splitvalues and allTable for splitvalues
    val infoTable = countTableValue
      .map { case (key, count) =>
        (key, (count, allTableSplit.getOrElse(key, 0.0)))
      }
      .map {

        case (key,(count, all)) => (key,(all, calcEntropy(count / all)))
      }

    //gainratio table
    val gainRatioTable = infoTable
      .groupBy(_._1)
      .mapValues { values =>

        (values.map(_._2).head._1, values.map(_._2._2).sum)

      }
      .toArray
      .map {
        case ((attr, value, _), (allSplit, entropySplit)) =>

          // compute info and splitinfo for each (attr, splitvalue)
          val p = allSplit / allTable
          val info = p * entropySplit
          val splitinfo = calcEntropy(p)


          ((attr, value), (allSplit, info, splitinfo))

      }
      .groupBy(_._1)
      .mapValues { values =>


        (values.map(_._2._1), values.map(_._2._2).sum, values.map(_._2._3).sum)

      }
      .filter(_._2._1.forall(_ > 100))
      .mapValues { case (all, info, split) =>

        (all, (entropyAll - info) / split) // it's gain / splitinfo
      }

    println("gainRatioTable")

    var argmax: Option[(String, Double)] = None

    if (gainRatioTable.nonEmpty) {

      val result = gainRatioTable.reduce((x1, x2) => if (x1._2._2 > x2._2._2) x1 else x2)._1
      argmax = Option(result._1, result._2)


    }

    argmax

  }

}
