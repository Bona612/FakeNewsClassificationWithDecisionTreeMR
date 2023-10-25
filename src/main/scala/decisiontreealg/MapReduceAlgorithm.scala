package decisiontreealg

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import spire.compat.ordering

import scala.collection.mutable
import scala.math.log10


class MapReduceAlgorithm(table : DataFrame){


  private var hashTable : mutable.HashMap[Int,List[Int]] = _


  def initAlgorithm() :Unit = {
    val cols: Array[String] = table.columns.dropRight(1) //discard class column
    val rddTable = table.rdd
    mainAlgorithm(rddTable, cols)
  }

  def dataPreparation(rddTable: RDD[Row], cols: Array[String]) : (RDD[(String, (Int, Int, Double))], RDD[(String, (Int, Int))], RDD[((String, String),( Int, Int))]) = {

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
    val countTableValue = attribTable.flatMap { case (attr, (row_id, label, value)) =>


      var valuesAttr = attrSplitPoints(attr)


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


    (attribTable, countTable, countTableValue)


  }

  private def mainAlgorithm(rddTable: RDD[Row], cols: Array[String]): Unit ={

    val (attribTable, countTable, countTableValue) = dataPreparation(rddTable, cols)

    //entropy to understand if we arrived in the situation of only or most of instance of a class. (0 is purity)
    val entropy = calcEntropyTable(rddTable)

    if(entropy < 0.03)    //stop check
      return

    val (bestAttr, bestValue) = findBestSplit(attribTable, countTable, countTableValue,rddTable.count().toInt)

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


  private def calcEntropyTable(rddTable: RDD[Row]): Double = {

    val allValue = rddTable.count().toInt
    val log2: Double => Double = (x: Double) => {
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)
    }

    val calcEntropy: Double => Double = (p: Double) => {
      -p * log2(p)
    }
    val countLabelClass = rddTable.map{row =>

      val index = row.schema.fieldIndex("label")
      (row(index).toString.toDouble, 1)
    }.reduceByKey(_+_)


    val countLabelAll = countLabelClass.map{ case(label,count) => (label,(count,allValue))}

    val entropy = countLabelAll.map{
      case(_,(count,allValue)) =>

        val p = count.toDouble / allValue.toDouble

        val entropy = calcEntropy(p)
        entropy
    }.reduce(_+_)

    println("entropy: "+entropy)
    entropy.abs
  }
  private def findBestSplit(attribTable: RDD[(String, (Int, Int, Double))], countTable: RDD[(String, (Int, Int))],
                            countTableValue: RDD[((String, String), (Int, Int))], allValue: Int): (String, String) = {

    val log2: Double => Double = (x: Double) => {
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)
    }

    val calcEntropy: Double => Double = (p: Double) => {
      -p * log2(p)
    }
    val allTableValue = countTableValue.map{case ((attr,value),(label,count)) => ((attr,value),count)}
      .reduceByKey(_+_)

    //join between countTable for splitvalues and allTable for splitvalues
    val infoTable = countTableValue
      .join(allTableValue)


    val infoTableFlat = countTable.map{case(attr,(label,count)) => (attr,(label,count,allValue))}

    //compute entropy for each attribute
    val entropyTable = infoTableFlat.map{ case(attr,(label,count,all)) =>

      val p: Double = count.toDouble / all.toDouble

      val entropy :Double = calcEntropy(p)

      (attr, entropy)
    }.reduceByKey(_+_)

    // compute info and splitinfo for each (attr, splitvalue)
    def computeInfoSplit(rdd: ((String, String),((Int,Int),Int))) : ((String,String),(Double, Double)) = {

      rdd match {
        case ((attr: String, value: String),((label: Int, count: Int), all: Int)) =>

          val p: Double = count.toDouble / all.toDouble
          val info: Double = p * calcEntropy(p)
          val splitInfo: Double = -p * log2(p)

          if (value.contains(">="))   //we want to merge info and splitinfo for a splitvalue (left < and right >= split)
            ((attr, value.split(">=").apply(0)),(info: Double, splitInfo: Double))
          else{
            ((attr, value.split("<").apply(0)),(info: Double, splitInfo: Double))
          }


      }
    }

    //gainratio table
    val gainRatioTable = infoTable
      .map(computeInfoSplit)
      .reduceByKey({
        case ((info1: Double, split1: Double), (info2: Double, split2: Double)) =>

          (info1 + info2: Double, split1 + split2: Double)
      })
      .map{ case ((attr,value), (info,split)) => (attr, (value,info,split))}
      .join(entropyTable)
      .map{ case (attr,((value: String, info: Double, split: Double), entropy:Double)) =>

        //println(entropy+" "+info)
        val gain :Double = entropy-info

        if(split.abs == 0.0)
          ((attr,value), 0.0)
        else
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
