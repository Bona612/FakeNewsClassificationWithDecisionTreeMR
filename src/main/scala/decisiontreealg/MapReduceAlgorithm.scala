package decisiontreealg

import decisiontree.{DecisionTree, Leaf, Node}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import spire.compat.ordering

import scala.collection.mutable
import scala.math.log10


class MapReduceAlgorithm() {


  def startAlgorithm(table : DataFrame): DecisionTree = {

    val cols: Array[String] = table.columns.dropRight(1) //discard class column
    val rddTable = table.rdd
    mainAlgorithm(rddTable, cols, None)
  }

  def dataPreparation(rddTable1: RDD[Row], cols: Array[String]) :RDD[((String, String),( Int, Int))] = {

    /*
    * Map of splitpoints (mean of adiacent values) for attributes, if attr have only one value we discard it
    */
    val rddTable = rddTable1.repartition(8)

    val attrTable = rddTable.flatMap { row =>

      val cols = row.schema.fieldNames.dropRight(1)
      val pairs = cols.map(col => (col, (row(row.schema.fieldIndex("ground_truth")).toString.toInt, row(row.schema.fieldIndex(col)).toString.toDouble)))

      pairs
    }

    val splitPointsTable = attrTable.combineByKey(
      createCombiner = (value: (Int, Double)) => Seq(value._2),
      mergeValue = (accumulator: Seq[Double], value: (Int, Double)) => accumulator :+ value._2,
      mergeCombiners = (accumulator1: Seq[Double],accumulator2: Seq[Double]) => (accumulator1 ++ accumulator2).distinct.sorted
    )
      .filter(pair => pair._2.length > 1)
      .mapValues( listval =>{
        if(listval.length > 1)
          listval.sliding(2).toList.map(pair => (pair.head+pair(1))/2.0)
        else
          listval
        }
      )

    // count table respect to (attr splitvalue, label)
    val countTableSplit = attrTable.rightOuterJoin(splitPointsTable)
      .flatMap { case (attr, (label_and_val : Option[(Int,Double)],val_list)) =>

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


    }.reduceByKey(_ + _).map { case ((attr, label, value), count) => ((attr, value), (label, count)) }

    countTableSplit

  }

  private def mainAlgorithm(rddTable: RDD[Row], cols: Array[String],par: Option[Node]): DecisionTree = {

    println("start data preparation...")
    val countTableSplit = dataPreparation(rddTable, cols)

    //entropy to understand if we arrived in the situation of only or most of instance of a class. (0 is purity)
    println("start calc entropy...")
    val (entropy,(maxClass,maxValue), allTable) = calcEntropyTable(rddTable)

    if(entropy < 0.03) { //stop check
      return Leaf(label = maxClass, parent = par)
    }

    println("find best split...")
    val (bestAttr, bestValue) = findBestSplit(countTableSplit, entropy, allTable)

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
    val right = mainAlgorithm(greaterAttrTable, cols, Option(currentNode))
    val left = mainAlgorithm(lowerAttrTable, cols, Option(currentNode))

    currentNode.insertLeftChild(left)
    currentNode.insertRightChild(right)

    currentNode
  }

  private def calcEntropyTable(rddTable1: RDD[Row]): (Double, (String, Int), Int) = {


    val allValue = rddTable1.count().toInt

    val rddTable = rddTable1.repartition(8)

    val log2: Double => Double = (x: Double) => {
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)
    }

    val calcEntropy: Double => Double = (p: Double) => {
      -p * log2(p)
    }

    val countLabelClass = rddTable.map{row =>

      val index = row.schema.fieldIndex("ground_truth")
      (row(index).toString, 1)
    }.reduceByKey(_+_)

    println("max calculating...")
    /*val maxKey: (String, Int) = countLabelClass.max()(new Ordering[(String, Int)]() {
      override def compare(x: (String, Int), y: (String, Int)): Int =
        Ordering[Int].compare(x._2, y._2)
    })*/

    val maxKey = countLabelClass.sortBy(_._2, ascending = false).first()

    println("max finished...")

    val countLabelAll = countLabelClass.map{ case(label,count) => (label,(count,allValue))}

    val entropy = countLabelAll.map{
      case(_,(count,allValue)) =>

        val p = count.toDouble / allValue.toDouble

        val entropy = calcEntropy(p)
        entropy
    }.reduce(_+_)

    println("entropy: "+entropy)
    println("maxkey: "+maxKey)
    (entropy.abs, maxKey, allValue)
  }

  private def findBestSplit(countTableValue: RDD[((String, String), (Int, Int))], entropyAll : Double, allTable: Int): (String, String) = {

    val log2: Double => Double = (x: Double) => {
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)
    }

    val calcEntropy: Double => Double = (p: Double) => {
      -p * log2(p)
    }

    val allTableSplit = countTableValue.map{case ((attr,value),(_,count)) => ((attr,value),count)}
      .reduceByKey(_+_)


    //join between countTable for splitvalues and allTable for splitvalues
    val infoTable = countTableValue
      .join(allTableSplit)

    //gainratio table
    val gainRatioTable = infoTable
      .map {
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
      .map{ case ((attr,value), (info,split)) =>

        //println(entropy+" "+info)
        val gain :Double = entropyAll-info

        ((attr,value), gain/split)
      }


    println("gainRatioTable")
    gainRatioTable.foreach(println)

    val argmax = gainRatioTable.reduce{ case ((attr1,gainRatio1),(attr2, gainRatio2)) =>

      if(gainRatio1 > gainRatio2)
        (attr1,gainRatio1)
      else
        (attr2, gainRatio2)

    }._1

    println(argmax)


    argmax

  }

}
