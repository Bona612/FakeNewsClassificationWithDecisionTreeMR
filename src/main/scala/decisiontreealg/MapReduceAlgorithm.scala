package decisiontreealg

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import spire.compat.ordering

import scala.collection.mutable
import scala.math.log10


class MapReduceAlgorithm(table : DataFrame){


  def initAlgorithm() :Unit = {
    val cols: Array[String] = table.columns.dropRight(1) //discard class column
    val rddTable = table.rdd
    mainAlgorithm(rddTable, cols)
  }

  def dataPreparation(rddTable: RDD[Row], cols: Array[String]) :RDD[((String, String),( Int, Int))] = {

    /*
    * Map of splitpoints (mean of adiacent values) for attributes, if attr have only one value we discard it
    */
    val attrSplitPoints = mutable.Map[String, Array[Double]]()

    var col_toDrop = Array[String]()  //attr to drop because have only one value

    for ((col, index) <- cols.zipWithIndex) {

      val values = rddTable.map(row => row(index)).distinct.map(_.toString.toDouble).collect()
      scala.util.Sorting.quickSort(values) //sort array

      var i = 1
      var j = i-1
      var splitpoints = Array[Double]()

      while(i < values.length){

        val mean = (values(j)+values(i))/2.0

        splitpoints :+= mean
        i += 1
        j = i-1
      }

      if(splitpoints.length > 0)  //if attr have more than 1 value
        attrSplitPoints += (col -> splitpoints)
      else
        col_toDrop :+= col    //add attr if have only 1 value

    }


    //creation of attribute table
    val attribTable = rddTable.zipWithIndex().flatMap { case (row, rowIndex) =>

      val label_id = row.schema.fieldIndex("label")

      val label = row.getInt(label_id)
      val cols = row.schema.fieldNames.filter(col => !col_toDrop.contains(col)) //discard attr with only 1 value
      val pairs = cols.dropRight(1).map { //drop label col, (attr, (row_id, labelclass, value))

        col =>

          (col, (rowIndex.intValue(), label, row(row.schema.fieldIndex(col)).toString.toDouble))
      }

      pairs
    }

    // count table respect to (attr splitvalue, label)
    val countTableSplit = attribTable.flatMap { case (attr, (row_id, label, value)) =>


      val valuesAttr = attrSplitPoints(attr)

      val pairs = valuesAttr.map( //divide in < and >= instances taking in account class label for each splitpoint of this attr

        value1 => {

          if (value < value1.toString.toDouble)
            ((attr, label, value1.toString + "<"), 1)
          else
            ((attr, label, value1.toString + ">="), 1)

        }
      )
      pairs


    }.reduceByKey(_ + _).map { case ((attr, label, value), count) => ((attr, value), (label, count)) }



    //count table respect to (attr, label) - count for each attr the instances with same label
    val countTable = attribTable.map { case (attr, (row_id, label, value)) => ((attr, label), 1)}
      .reduceByKey(_ + _).map { case ((attr, label), count) => (attr, (label, count)) }


    countTableSplit


  }

  private def mainAlgorithm(rddTable: RDD[Row], cols: Array[String]): Unit = {

    val countTableValue = dataPreparation(rddTable, cols)

    //entropy to understand if we arrived in the situation of only or most of instance of a class. (0 is purity)
    val (entropy, max_class) = calcEntropyTable(rddTable)

    if(entropy < 0.03) //stop check
      return

    val (bestAttr, bestValue) = findBestSplit(countTableValue, entropy)

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

    mainAlgorithm(greaterAttrTable, cols)
    mainAlgorithm(lowerAttrTable, cols)
  }


  private def calcEntropyTable(rddTable: RDD[Row]): (Double, (String, Int)) = {

    val allValue = rddTable.count().toInt
    val log2: Double => Double = (x: Double) => {
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)
    }

    val calcEntropy: Double => Double = (p: Double) => {
      -p * log2(p)
    }

    val countLabelClass = rddTable.map{row =>

      val index = row.schema.fieldIndex("label")
      (row(index).toString, 1)
    }.reduceByKey(_+_)

    val maxKey: (String, Int) = countLabelClass.max()(new Ordering[(String, Int)]() {
      override def compare(x: (String, Int), y: (String, Int)): Int =
        Ordering[Int].compare(x._2, y._2)
    })

    val countLabelAll = countLabelClass.map{ case(label,count) => (label,(count,allValue))}

    val entropy = countLabelAll.map{
      case(_,(count,allValue)) =>

        val p = count.toDouble / allValue.toDouble

        val entropy = calcEntropy(p)
        entropy
    }.reduce(_+_)

    println("entropy: "+entropy)
    (entropy.abs, maxKey)
  }
  private def findBestSplit(countTableValue: RDD[((String, String), (Int, Int))], entropyAll : Double): (String, String) = {

    val log2: Double => Double = (x: Double) => {
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)
    }

    val calcEntropy: Double => Double = (p: Double) => {
      -p * log2(p)
    }

    val allTableSplit = countTableValue.map{case ((attr,value),(label,count)) => ((attr,value),count)}
      .reduceByKey(_+_)

    val allTableValue = allTableSplit.map { case ((attr, value), allSplit) =>

      if (value.contains(">="))
        ((attr, value.split(">=").apply(0)),allSplit)
      else
        ((attr, value.split("<").apply(0)), allSplit)

    }.reduceByKey(_ + _)


    //join between countTable for splitvalues and allTable for splitvalues
    val infoTable = countTableValue
      .join(allTableSplit)

    //gainratio table
    val gainRatioTable = infoTable
      .map {
        case ((attr, value), ((_, count), all)) =>

          val p: Double = count.toDouble / all.toDouble

          val entropyAttr: Double = calcEntropy(p)

          if (value.contains(">="))
            ((attr, value.split(">=").apply(0)), (all,entropyAttr))
          else
            ((attr, value.split("<").apply(0)), (all,entropyAttr))

      }
      .reduceByKey{ case((all,entropy1),(_,entropy2)) => (all, entropy1 + entropy2)}
      .join(allTableValue)
      .map{
        case((attr,value),((allSplit, entropySplit),allValue)) =>

          // compute info and splitinfo for each (attr, splitvalue)
          val p = allSplit.toDouble / allValue.toDouble
          val info = p * entropySplit
          val splitinfo = -p * log2(p)
          ((attr,value),(info,splitinfo))
      }
      .reduceByKey{ case((info1, splitinfo1), (info2, splitinfo2)) => (info1+info2, splitinfo1+splitinfo2)}
      .map{ case ((attr,value), (info,split)) =>

        //println(entropy+" "+info)
        val gain :Double = entropyAll-info

        ((attr,value), gain/split)
    }


    println("gainRatioTable")
    gainRatioTable.collect().foreach(println)

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
