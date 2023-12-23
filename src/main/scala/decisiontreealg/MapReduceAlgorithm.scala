package decisiontreealg

import decisiontree.{Leaf, Node, TreeNode}
import org.apache.spark
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.:+
import scala.math.log10

class MapReduceAlgorithm() {

  var maxDepth = 0


  def startAlgorithm(dataset: DataFrame, max: Int, countLabel0: Double, countLabel1: Double): TreeNode = {


    val idx_label = dataset.columns.length-1

    val sparkContext = dataset.rdd.sparkContext
    val cols: Broadcast[Array[(String, Int)]] = sparkContext.broadcast(dataset.columns.drop(1).dropRight(1).zipWithIndex)

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


    //println(s"dataset repartition: ${dataset.rdd.partitions.length}")
    println("start calc entropy...")
    //entropy to understand if we arrived in the situation of only or most of instance of a class. (0 is purity)
    val (entropy, (maxClass, _), allTable) = calcEntropyTable(dataset, countLabel0, countLabel1)

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

        var countSeq = countLabel.lookup(bestAttr,0,bestValue,true)

        println(countLabel.lookup(bestAttr,0,bestValue,true))

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

    /*
    var counts = dataset.groupBy("ground_truth").count().collect()

    val countLabel0 = counts.find(row => row.getAs[Int]("ground_truth") == 0).map(_.getAs[Long]("count")).getOrElse(0L).toDouble
    val countLabel1 = counts.find(row => row.getAs[Int]("ground_truth") == 1).map(_.getAs[Long]("count")).getOrElse(0L).toDouble
    */
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

    //counts = null

    (entropy, maxKey, allValue)
  }

  def dataPreparation(dataset: DataFrame,
                      cols: Broadcast[Array[(String,Int)]])
  : (RDD[((String, Int, Double, Boolean), Int)], RDD[((String, Double, Boolean), Int)]) = {

    /*
    * Map of splitpoints (mean of adiacent values) for attributes, if attr have only one value we discard it
    */


    if (dataset.drop(Seq("ground_truth", "Index"): _*).distinct().head(2).length > 1) {


      val idx_label = dataset.columns.length - 1

      val attrTable = dataset.rdd.flatMap {
          row =>
            cols.value.map {
              case (col, idx) =>
                ((col, row(idx_label).asInstanceOf[Int], row(idx + 1).asInstanceOf[Double]), 1)
            }
        }
        .reduceByKey(_ + _)
        .map {

          case ((attr, label, value), count) =>

            (attr, (label, value, count))
        }.persist()

      //attrTable = attrTable.partitionBy(new HashPartitioner(attrTable.partitions.length)).persist()

      val splitPointsTable = attrTable.mapValues(values => Seq(values._2))
        .reduceByKey(_ ++ _)
        .mapValues(_.distinct.sorted)
        .filter(_._2.length > 1)
        .mapValues(_.sliding(2).toList.map(pair => (pair.head + pair(1)) / 2.0))

      // count table respect to (attr splitvalue, label)
      val countLabelSplit = attrTable
        .rightOuterJoin(splitPointsTable)
        .flatMap {

          case (attr, (joined: Option[(Int, Double, Int)], val_list)) => //(joined: Option[(Int, Double, Set[Int])], val_list)) =>

            joined match {

              case Some((label, value, count)) =>

                val_list.map( //divide in < and >= instances taking in account class label for each splitpoint of this attr

                  value1 => ((attr, label, value1, if (value < value1) false else true), count)
                )
            }
        }
        .reduceByKey(_ + _)
        .persist()

      val countTableSplit = countLabelSplit
        .map {

          case ((attr, _, value, split), count) =>
            ((attr, value, split), count)
        }
        .persist()

      (countLabelSplit, countTableSplit)
    }
    else
      (null, null)

  }


  private def findBestSplit(countTableValue: RDD[((String, Double, Boolean), Int)], //Set[Int])],
                            entropyAll: Double, allTable: Int): Option[(String, Double)] = {


    val log2: Double => Double = (x: Double) =>
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)

    val roundDouble: Double => Double = (value: Double) =>
      BigDecimal(value).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble

    val calcEntropy: Double => Double = (p: Double) => -p * log2(p) //roundDouble(-p * log2(p))

    val allTableSplit = countTableValue.reduceByKey(_ + _) //.coalesce(2)

    //join between countTable for splitvalues and allTable for splitvalues
    val infoTable = countTableValue
      .join(allTableSplit)

    println(s"infotable partitions: ${infoTable.partitions.length}")
    //gainratio table
    val gainRatioTable = infoTable
      .mapValues {

        case (count, all) => (all, calcEntropy(count.toDouble / all.toDouble))
      }
      .reduceByKey { case ((all, entropyAttr1), (_, entropyAttr2)) => (all, entropyAttr1 + entropyAttr2) }
      .map {
        case ((attr, value, _), (allSplit, entropySplit)) =>

          // compute info and splitinfo for each (attr, splitvalue)
          val p = allSplit.toDouble / allTable.toDouble
          val info = p * entropySplit
          val splitinfo = calcEntropy(p)


          ((attr, value), ((allSplit, 0), info, splitinfo))

      }
      .reduceByKey {
        case (((all1, _), info1, splitinfo1), ((all2, _), info2, splitinfo2)) =>

          ((all1, all2), info1 + info2, splitinfo1 + splitinfo2)
      }

      .filter(x => x._2._1._1 > 100 && x._2._1._2 > 100)
      .mapValues { case ((all1, all2), info, split) =>

        ((all1, all2), (entropyAll - info) / split) // it's gain / splitinfo
      }

    println("gainRatioTable")


    var argmax: Option[(String, Double)] = None

    if (gainRatioTable.take(1).nonEmpty) {
      val result = gainRatioTable.reduce((x1, x2) => if (x1._2._2 > x2._2._2) x1 else x2)._1
      argmax = Some(result._1, result._2)
    }

    argmax

  }

}
