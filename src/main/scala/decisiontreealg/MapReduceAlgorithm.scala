package decisiontreealg

import decisiontree.{DecisionTree, Leaf, Node}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, collect_set, countDistinct}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import spire.compat.ordering

import scala.collection.mutable
import scala.math.log10


class MapReduceAlgorithm() {

  def startAlgorithm(table : DataFrame): DecisionTree = {

    val cols: Array[String] = table.columns.dropRight(1) //discard class column

    val idx_label = table.columns.length-1

    mainAlgorithm(table, cols, None, idx_label, 0)
  }

  private def mainAlgorithm(rddTable: DataFrame, cols: Array[String], par: Option[Node], idx_label: Int, depthTree: Int): DecisionTree = {

    println("start calc entropy...")
    //entropy to understand if we arrived in the situation of only or most of instance of a class. (0 is purity)
    val (entropy, (maxClass, _), allTable) = calcEntropyTable(rddTable, idx_label)

    if (entropy <= 0.3f || depthTree >= 100) { //stop check
      return Leaf(label = maxClass, parent = par)
    }

    println("start data preparation...")
    val countTableSplit = dataPreparation(rddTable, cols, idx_label)

    if (countTableSplit == null) { //stop check
      return Leaf(label = maxClass, parent = par)
    }

    println("find best split...")
    val (bestAttr, bestValue) = findBestSplit(countTableSplit, entropy, allTable.toInt)

    // create 2 child table filtering from parent for each split (2)
    val greaterAttrTable = rddTable.filter {

      row =>
        val index = row.schema.fieldIndex(bestAttr)

        row(index).toString.toDouble >= bestValue.toDouble
    }

    val lowerAttrTable = rddTable.filter {

      row =>
        val index = row.schema.fieldIndex(bestAttr)

        row(index).toString.toDouble < bestValue.toDouble
    }

    val currentNode = Node(bestAttr, bestValue.toDouble, null, null, par)

    println("iterate right and left...")
    val right = mainAlgorithm(greaterAttrTable, cols, Option(currentNode), idx_label, depthTree+1)
    val left = mainAlgorithm(lowerAttrTable, cols, Option(currentNode), idx_label, depthTree+1)

    currentNode.insertLeftChild(left)
    currentNode.insertRightChild(right)

    currentNode
  }

  private def calcEntropyTable(rddTable: DataFrame, idx_label: Int): (Double, (String, Double), Double) = {

    val log2: Double => Double = (x: Double) => {
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)
    }

    val calcEntropy: Double => Double = (p: Double) => {


      BigDecimal(-p * log2(p)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
    }

    /*
    val countLabel0 = rddTable.filter(row => row(idx_label).toString.toInt == 0).count().toDouble
    val countLabel1 = rddTable.filter(row => row(idx_label).toString.toInt == 1).count().toDouble
    */

    val countLabel0 = rddTable.filter(col("ground_truth") === "0").count().toDouble
    val countLabel1 = rddTable.filter(col("ground_truth") === "1").count().toDouble
    val allValue = countLabel0 + countLabel1

    var maxKey: (String, Double) = null
    if (countLabel0 > countLabel1)
      maxKey = ("0", countLabel0)
    else
      maxKey = ("1", countLabel1)

    var entropy = 0.0

    if (allValue > 0.0)
      entropy = calcEntropy(countLabel0 / allValue) + calcEntropy(countLabel1 / allValue).abs

    println("entropy: " + entropy)
    println("0: " + countLabel0, "1: " + countLabel1)

    (entropy, maxKey, allValue)
  }

  def dataPreparation(rddTable: DataFrame, cols: Array[String], idx_label: Int) :RDD[((String, String),( Int, Int))] = {

    /*
    * Map of splitpoints (mean of adiacent values) for attributes, if attr have only one value we discard it
    */

    // Calculate distinct values for each column
    /*val distinctValues = cols.map(c => collect_set(col(c)).alias(s"distinct_$c"))
    val distinctTable = rddTable.agg(distinctValues.head, distinctValues.tail: _*)*/

    val attrTable =  rddTable.rdd.flatMap {

      row =>
        val cols = row.schema.fieldNames.dropRight(1)
        val pairs = cols.map(col => (col, (row(idx_label).toString.toInt, row(row.schema.fieldIndex(col)).toString.toDouble)))
        pairs
    }

    var splitPointsTable = attrTable.combineByKey(
      createCombiner = (value: (Int, Double)) => Seq(value._2),
      mergeValue = (accumulator: Seq[Double], value: (Int, Double)) => accumulator :+ value._2,
      mergeCombiners = (accumulator1: Seq[Double],accumulator2: Seq[Double]) => (accumulator1 ++ accumulator2).distinct.sorted
    )
    .filter(_._2.length > 1)

    if (!splitPointsTable.isEmpty()) {

      splitPointsTable = splitPointsTable.mapValues(
        listval => {
          if (listval.length > 1)
            listval.sliding(2).toList.map(pair => (pair.head + pair(1)) / 2.0)
          else
            listval
        }
      )

      // count table respect to (attr splitvalue, label)
      val countTableSplit = attrTable.rightOuterJoin(splitPointsTable).flatMap {

        case (attr, (label_and_val: Option[(Int, Double)], val_list)) =>

          val (label, value) = label_and_val.get
          val pairs = val_list.map( //divide in < and >= instances taking in account class label for each splitpoint of this attr

            value1 => {

              if (value < value1.toString.toDouble)
                ((attr, label, value1.toString + "<"), 1)
              else
                ((attr, label, value1.toString + ">="), 1)

            }
          )
          pairs

      }.reduceByKey(_ + _).map {
         case ((attr, label, value), count) =>
           ((attr, value), (label, count))
      }

      countTableSplit
    }
    else
      null

  }


  private def findBestSplit(countTableValue: RDD[((String, String), (Int, Int))], entropyAll : Double, allTable: Int): (String, String) = {

    val log2: Double => Double = (x: Double) => {
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)
    }

    val calcEntropy: Double => Double = (p: Double) => {
      BigDecimal(-p * log2(p)).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
    }

    val allTableSplit = countTableValue.mapValues{case (_,count) => count}
      .reduceByKey(_+_)


    //join between countTable for splitvalues and allTable for splitvalues
    val infoTable = countTableValue
      .join(allTableSplit)

    //gainratio table
    val gainRatioTable = infoTable.map {

        case ((attr, value), ((_, count), all)) =>

          val p: Double = count.toDouble / all.toDouble

          val entropyAttr: Double = calcEntropy(p)

            ((attr, value), (all,entropyAttr))

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

        //println(entropy+" "+info)
        val gain :Double = entropyAll-info

        BigDecimal(gain/split).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
      }


    println("gainRatioTable")
    //gainRatioTable.foreach(println)

    val argmax = gainRatioTable.sortBy(_._2, ascending = false).first()._1

    println(argmax)

    argmax

  }

}
