package decisiontreealg

import decisiontree.{Leaf, Node, TreeNode}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.math.log10

class MapReduceAlgorithm() {

  var maxDepth = 0


  var numPartitions: Int = 0
  def startAlgorithm(dataset: DataFrame, max: Int, countLabel0: Double, countLabel1: Double): TreeNode = {

    //  idx of label column in dataframe
    val idx_label = dataset.columns.length-1

    val sparkContext = dataset.rdd.sparkContext

    numPartitions = sparkContext.getConf.get("spark.sql.shuffle.partitions").toInt
    val cols: Broadcast[Array[(String, Int)]] = sparkContext.broadcast(dataset.columns.drop(1).dropRight(1).zipWithIndex)

    // max depth of tree
    maxDepth = max

    mainAlgorithm(dataset, cols, None, idx_label, 0, countLabel0, countLabel1)

  }

  private def mainAlgorithm(dataset: DataFrame,
                            cols: Broadcast[Array[(String,Int)]],
                            par: Option[TreeNode],
                            idx_label: Int,
                            depthTree: Int,
                            countLabel0: Double,
                            countLabel1: Double): TreeNode = {

    println("start calc entropy...")

    //entropy to understand if we arrived in the situation of most instances of a class. (0 is purity)
    val (entropy, (maxClass, _), allTable) = calcEntropyTable(dataset, countLabel0, countLabel1)

    if (entropy <= 0.3f || depthTree >= maxDepth) { //stop check
      return Leaf(par, maxClass.toInt)
    }

    println("start data preparation...")

    val (countLabel, countTable) = dataPreparation(dataset, cols)

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

        // update countLabel0 and countLabel1 for greaterDataset and LowerDataset
        var countSeq = countLabel.lookup(bestAttr,0,bestValue,true)

        val new_greaterCountLabel0 = if(countSeq.nonEmpty) countSeq.head else 0

        countSeq = countLabel.lookup(bestAttr,1,bestValue,true)
        val new_greaterCountLabel1 = if (countSeq.nonEmpty) countSeq.head else 0

        countSeq = countLabel.lookup(bestAttr,0,bestValue,false)
        val new_lowerCountLabel0 = if (countSeq.nonEmpty) countSeq.head else 0

        countSeq = countLabel.lookup(bestAttr,1,bestValue,false)
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

  private def calcEntropyTable(dataset: DataFrame, countLabel0: Double, countLabel1: Double): (Double, (String, Double), Double) = {

    val log2: Double => Double = (x: Double) =>
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)


    val roundDouble : Double => Double = (value: Double) =>
      BigDecimal(value).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble

    val calcEntropy: Double => Double = (p: Double) => -p * log2(p)

    val allValue = countLabel0 + countLabel1

    // Label to mark leaf
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

  def dataPreparation(dataset: DataFrame,
                      cols: Broadcast[Array[(String,Int)]])
  : (RDD[((String, Int, Double, Boolean), Double)], RDD[((String, Double, Boolean), Double)]) = {

    /**
      * attrTable - for each word we count the instances with same label and value
      *             (multiple value with same key in RDD)
    *
    * splitPointsTable - we calculate each split point as the mean of adjacent pairs of values
    *                    for each attribute
    *
    * countLabelSplit - I want to have all information i need (count of labels 0 and 1) to choose later
    *                   the best word and its best split point. left (<) and right (>=) split
     *                  for each split point.
    *
    * countTableSplit - we get rid of label in key to compute later the sum of labels 0 and 1 and calculate
    *                   entropy of that split
    *
    */


    val idx_label = dataset.columns.length - 1


    val attrTable = dataset.rdd
      .flatMap {
        row =>
          cols.value.map {
            case (col, idx) =>
              ((col, row(idx_label).asInstanceOf[Int], row(idx + 1).asInstanceOf[Double]), 1.0)
          }
      }
      .reduceByKey(_ + _)
      .map {

        case ((attr, label, value), count) =>

          (attr, (label, value, count))
      }
      .persist()


    val splitPointsTable = attrTable.aggregateByKey(Seq.empty[Double])(
        (seq, tuple) => seq :+ tuple._2,
        (seq1, seq2) => seq1 ++ seq2
      )
      .mapValues(_.distinct.sorted)
      .filter(_._2.length > 1)
      .mapValues(_.sliding(2).toList.map(pair => (pair.head + pair(1)) / 2.0))

    var countLabelSplit: RDD[((String, Int, Double, Boolean), Double)] = null
    var countTableSplit: RDD[((String, Double, Boolean), Double)] = null

    countLabelSplit = attrTable
      .rightOuterJoin(splitPointsTable)
      .flatMap {

        case (attr, (joined: Option[(Int, Double, Double)], splitval_list)) =>

          joined match {
            // dal join so che avrò sempre la chiave in attrTable
            case Some((label, value, count)) =>

              splitval_list.map( //divide in < and >= instances taking in account class label for each splitpoint of this attr

                split_val => ((attr, label, split_val, if (value < split_val) false else true), count)
              )

          }
      }
      .reduceByKey(_ + _)
      .persist()

    countTableSplit = countLabelSplit
      .map {

        case ((attr, label, split_val, split), count) =>
          ((attr, split_val, split), count)
      }
      .persist()

    (countLabelSplit, countTableSplit)

  }


  private def findBestSplit(countTableValue: RDD[((String, Double, Boolean), Double)],
                            entropyAll: Double, allTable: Double): Option[(String, Double)] = {

    /**
     *
     * allTableSplit - Sum count of labels 0 and 1 for each possible split
     *
     * infoTable - Calculate entropy of each split
     *
     * gainRatioTable - calculate gain ratio for each split value, choosing the greater we have the
     *                  best attribute and its best split value
     * */
    val log2: Double => Double = (x: Double) =>
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)


    val calcEntropy: Double => Double = (p: Double) => -p * log2(p)

    val allTableSplit = countTableValue.reduceByKey(_ + _)

    //join between countTable for splitvalues and allTable for splitvalues
    val infoTable = countTableValue
      .join(allTableSplit)
      .mapValues {

        case (count, all) => (all, calcEntropy(count / all))
      }
      .reduceByKey { case ((all, entropyAttr1), (_, entropyAttr2)) => (all, entropyAttr1 + entropyAttr2) }


    val gainRatioTable = infoTable
      .map {
        case ((attr, value, _), (allSplit, entropySplit)) =>

          // compute info and splitinfo for each (attr, splitvalue)
          val p = allSplit / allTable
          val info = p * entropySplit
          val splitinfo = calcEntropy(p)

          ((attr, value), ((allSplit, 0.0), info, splitinfo))

      }
      .reduceByKey {
        case (((all1, _), info1, splitinfo1), ((all2, _), info2, splitinfo2)) =>

          ((all1, all2), info1 + info2, splitinfo1 + splitinfo2)
      }
      // we want our split to have at least 100 instances (reduce overfitting)
      .filter(x => x._2._1._1 > 100.0 && x._2._1._2 > 100.0)
      .mapValues { case ((all1, all2), info, split) =>

        ((all1, all2), (entropyAll - info) / split) // gain / splitinfo
      }

    println("gainRatioTable")

    var argmax: Option[(String, Double)] = None

    if (gainRatioTable.take(1).nonEmpty) {

      val result = gainRatioTable.reduce((x1, x2) => if (x1._2._2 > x2._2._2) x1 else x2)._1
      argmax = Option(result._1, result._2)
    }

    argmax

  }

}
