package decisiontreealg

import decisiontree.{DecisionTree, Leaf, Node}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.math.log10


class MapReduceAlgorithm3() {

  private var maxDepth: Int = 0
  def startAlgorithm(dataset: DataFrame, maxDepth: Int): DecisionTree = {

    val cols = dataset.columns.dropRight(1) //discard class column

    this.maxDepth = maxDepth
    val idx_label = dataset.columns.length-1

    mainAlgorithm(dataset, cols, idx_label,None, 0)
  }

  private def mainAlgorithm(dataset: DataFrame, cols: Array[String], idx_label: Int, par: Option[Node], depthTree: Int): DecisionTree = {


    println("start calc entropy...")
    //entropy to understand if we arrived in the situation of only or most of instance of a class. (0 is purity)
    val (entropy, (maxClass, _), allTable) = calcEntropyTable(dataset)

    if (entropy <= 0.3f || depthTree >= this.maxDepth) { //stop check
      return Leaf(label = maxClass, parent = par)
    }

    println("start data preparation...")

    val countTableSplit = dataPreparation(dataset, cols, idx_label)

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
    }

    val lowerDataset = dataset.filter {

      row =>
        val index = row.schema.fieldIndex(bestAttr)

        row(index).asInstanceOf[Double] < bestValue
    }

    val currentNode = Node(bestAttr, bestValue, null, null, par)

    println("iterate right and left...")


    val right = mainAlgorithm(greaterDataset, cols, idx_label, Option(currentNode), depthTree+1)
    val left = mainAlgorithm(lowerDataset, cols, idx_label, Option(currentNode), depthTree+1)

    dataset.unpersist()

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

  def dataPreparation(dataset: DataFrame, cols: Array[String], idx_label: Int) :RDD[((String, String),( Int, Int))] = {

    /*
    * Map of splitpoints (mean of adiacent values) for attributes, if attr have only one value we discard it
    */


    if (dataset.drop("ground_truth").distinct().head(2).length > 1) {


      println(s"is cached: ${dataset.storageLevel.useMemory}")

      val attrTable = dataset.rdd
        .flatMap {
        row =>
          cols.zipWithIndex.map{
            case (col, idx) =>
              (col,  (row(idx_label).asInstanceOf[Int], row(idx).asInstanceOf[Double]))
          }
      }.persist()

      println(s"attrtable partition: ${attrTable.partitions.length}")

      val splitPointsTable = attrTable.combineByKey(
        createCombiner = (value: (Int, Double)) => Seq(value._2),
        mergeValue = (accumulator: Seq[Double], value: (Int, Double)) => accumulator :+ value._2,
        mergeCombiners = (accumulator1: Seq[Double],accumulator2: Seq[Double]) => accumulator1 ++ accumulator2
      )
      .mapValues(_.distinct.sorted)
      .filter(_._2.length > 1)
      .mapValues(_.sliding(2).toList.map(pair => (pair.head + pair(1)) / 2.0))

      // count table respect to (attr splitvalue, label)
      val countTableSplit = attrTable.rightOuterJoin(splitPointsTable)
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
        .reduceByKey(_ + _)
        .filter(_._2 >= 8)
        .map {

          case ((attr, label, value), count) =>
            ((attr, value), (label, count))
        }
        .persist()


        if(countTableSplit.isEmpty())
          null
        else
          countTableSplit

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

    val allTableSplit = countTableValue
      .mapValues{case (_,count) => count}
      .reduceByKey(_+_)//.coalesce(2)

    //join between countTable for splitvalues and allTable for splitvalues
    val infoTable = countTableValue
      .join(allTableSplit)

    println((s"infotable partitions: ${infoTable.partitions.length}"))
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

        (entropyAll-info)/split // it's gain / splitinfo
      }

    println("gainRatioTable")

    val argmax = gainRatioTable.reduce((x1, x2) => if (x1._2 > x2._2) x1 else x2)._1

    println(argmax)

    argmax

  }

}
