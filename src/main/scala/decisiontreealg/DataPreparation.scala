package decisiontreealg

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import spire.compat.ordering

import scala.collection.mutable

import scala.math.log10


class DataPreparation (table : DataFrame){

  private var attribTable : RDD[(String, (Int, Int, Double))] = _
  private var countTable : RDD[(String, (Int, Int))] = _
  private var hashTable : mutable.HashMap[Int,List[Int]] = _

  def createAttribTable(): Unit = {

    val cols: Array[String] = table.columns.dropRight(1) //discard class column

    var col_values = mutable.Map[String, Array[Double]]()

    // map for store values of attributes
    for( col <- cols){
      val columnIndex = table.columns.indexOf(col)
      val values = table.rdd.map(row => row(columnIndex)).distinct.map(_.toString.toDouble).collect()
      scala.util.Sorting.quickSort(values) //sort array
      val split_points = values.drop(1).dropRight(1)  //remove rightmost and leftmost value
      col_values += (col -> split_points)
    }


    //creation of attribute table
    attribTable = table.rdd.zipWithIndex().flatMap{ case (row, rowIndex) =>


      val label_id = row.schema.fieldIndex("label")

      val label = row.getInt(label_id)
      val pairs = row.schema.fieldNames.dropRight(1).map{   //drop label col, (attr, (row_id, labelclass, value))

        col =>(col,(rowIndex.intValue(), label, row(row.schema.fieldIndex(col)).toString.toDouble))
      }

      pairs
    }

    // count table respect to (attr splitvalue, label)
    val countTableValue = attribTable.flatMap { case (attr, (row_id, label, value)) =>

      val pairs = col_values.apply(attr).map(   //compare attr value for each distinct values of attribute

        value1 => {

          if (value < value1.toString.toDouble)
            ((attr, label, value1.toString + "<"), 1)
          else
            ((attr, label, value1.toString + ">="), 1)

        }
      )

      pairs
    }.reduceByKey(_ + _).map { case ((attr, label, value), count) => (attr, (value, label, count)) }


    //count table respect to (attr, label) - count for each attr the instances with same label
    countTable = attribTable.map { case (attr, (row_id, label, value)) =>

      ((attr, label), 1)
    }.reduceByKey(_ + _).map { case ((attr, label), count) => (attr, (label, count)) }


    val allTable : RDD[(String,Int)] = countTable.map{case (attr,(_,count)) => (attr,count)}.reduceByKey(_+_)

    val allTableValue = countTableValue.map{case (attr,(value,label,count)) => ((attr,value),count)}
      .reduceByKey(_+_)

    //join between countTable for splitvalues and allTable for splitvalues
    val infoTable = countTableValue
      .map{case (attr,(value,label,count)) => ((attr,value),(label,count))}
      .join(allTableValue)
      .map{case ((attr,value),((label,count),all)) => (attr,(value,label,count,all))}



    val infoTableFlat = countTable.join(allTable)

    val log2 = (x: Double) => { if( x.abs == 0.0) 0.0 else log10(x)/log10(2.0)}

    val calcEntropy = (p: Double) => {-p * log2(p)}

    //compute entropy for each attribute
    val entropyTable = infoTableFlat.map{ case(attr,((label,count),all)) =>

      val p: Double = count.toDouble / all.toDouble

      val entropy :Double = calcEntropy(p)

      (attr, entropy)
    }.reduceByKey(_+_)

    // compute info and splitinfo for each (attr, splitvalue)
    def computeInfoSplit(rdd: ((String, String),(Int,Int,Int))) : ((String,String),(Double, Double)) = {

      rdd match {
        case ((attr: String, value: String),(label: Int, count: Int, all: Int)) =>

          val p: Double = count.toDouble / all.toDouble
          val info: Double = p * calcEntropy(p)
          val splitInfo: Double = -p * log2(p)

          if (value.contains(">="))   //we want to merge info and splitinfo for a splitvalue (left < and right >= split)
            ((attr, value.split(">=").apply(0)),(info: Double, splitInfo: Double))
          else {
            ((attr, value.split("<").apply(0)),(info: Double, splitInfo: Double))
          }

      }
    }

    //gainratio table
    val gainRatioTable = infoTable
      .map{ case (attr, (value,label,count,all)) => ((attr, value),(label,count,all))}
      .map(computeInfoSplit)
      .reduceByKey({
      (V1, V2) => (V1, V2) match {
        case ((info1: Double, split1: Double), (info2: Double, split2: Double)) =>

          (info1 + info2: Double, split1 + split2: Double)
      }
    }).map{ case ((attr,value), (info,split)) => (attr, (value,info,split))}
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

    val greaterAttrTable = table.filter{

      row =>
        val index = row.schema.fieldIndex(argmax._1)
        row(index).toString.toDouble >= argmax._2.toDouble
    }

    val lowerAttrTable = table.filter {

      row =>
        val index = row.schema.fieldIndex(argmax._1)
        row(index).toString.toDouble < argmax._2.toDouble
    }

    greaterAttrTable.show()


  }

}
