package decisiontreealg

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable
import scala.math.log10


class DataPreparation (table : DataFrame){

  private var attribTable : RDD[(String, (Int, Int, Double))] = _
  private var countTable : RDD[(String, (Int, Int))] = _
  private var hashTable : mutable.HashMap[Int,List[Int]] = _


  def createAttribTable(): Unit = {

    val cols: Array[String] = table.columns.dropRight(1)

    val col_values = mutable.Map[String, Array[_]]()

    for( col <- cols){
      val columnIndex = table.columns.indexOf(col)
      col_values += (col -> table.rdd.map(row => row(columnIndex)).distinct.collect())

    }



    attribTable = table.rdd.zipWithIndex().flatMap{ case (row, rowIndex) =>


      val label_id = row.schema.fieldIndex("label")

      val label = row.getInt(label_id)
      val pairs = row.schema.fieldNames.dropRight(1).map{ col =>


          (col,(rowIndex.intValue(), label, row(row.schema.fieldIndex(col)).toString.toDouble))
      }

      pairs
    }

    //attribTable.collect().foreach(println)
    /*
    val countLabelAttr = attribTable.map{ case (attr, (row_id1, label, values1)) =>

      (attr, label)
    }.groupByKey().mapValues(_.groupBy(identity).view.mapValues(_.size))
    */

    val countTableValue2 = attribTable.map { case (attr, (row_id, label, value)) =>

      ((attr, label, value.toString.toDouble),1)
    }.reduceByKey(_ + _).map{ case ((attr,label,value),count) => (attr,(value,label,count))}


    val countTableValue = attribTable.flatMap { case (attr, (row_id, label, value)) =>

      val pairs = col_values.apply(attr).map(

        value1 => {

          if(value1.toString.toDouble < value)
            ((attr,label,value.toString+"<"),1)
          else
            ((attr,label,value.toString+">="),1)

        }
      )

      pairs
    }.reduceByKey(_+_).map{ case ((attr,label,value),count) => (attr,(value,label,count))}


    countTable = attribTable.map { case (attr, (row_id, label, value)) =>

      ((attr, label, value), 1)
    }.reduceByKey(_ + _).map { case ((attr, label, value), count) => (attr, (label, count)) }


    val countTableFlat = countTable.map { case (attr, (label, count)) =>

      ((attr, label), count)
    }.reduceByKey(_ + _).map { case ((attr, label), count) => (attr, (label, count)) }


    val allTable : RDD[(String,Int)] = countTableFlat.map{case (attr,(_,count)) => (attr,count)}.reduceByKey(_+_)

    val allTableValue = countTableValue.map{case (attr,(value,label,count)) => ((attr,value),count)}
      .reduceByKey(_+_)

    val infoTable = countTableValue
      .map{case (attr,(value,label,count)) => ((attr,value),(label,count))}
      .join(allTableValue)
      .map{case ((attr,value),((label,count),all)) => (attr,(value,label,count,all))}



    val infoTableFlat = countTableFlat.join(allTable)

    val log2 = (x: Double) => { if( x.abs == 0.0) 0.0 else log10(x)/log10(2.0)}

    val calcEntropy = (p: Double) => {-p * log2(p)}

    val entropyTable = infoTableFlat.map{ case(attr,((label,count),all)) =>

      val p: Double = count.toDouble / all.toDouble

      val entropy :Double = calcEntropy(p)

      (attr, entropy)
    }.reduceByKey(_+_)



    def computeInfoSplit(rdd: (Int,Int,Int)) : (Double, Double) = {

      rdd match {
        case (label: Int, count: Int, all: Int) =>

          val p: Double = count.toDouble / all.toDouble
          val info: Double = p * calcEntropy(p)
          val splitInfo: Double = -p * log2(p)

          println("p: "+p)
          println("info: "+info)
          println("splitinfo: "+splitInfo)

          (info: Double, splitInfo: Double)
      }
    }

    val gainRatioTable = infoTable
      .map{ case (attr, (value,label,count,all)) => ((attr, value),(label,count,all))}
      .mapValues(computeInfoSplit)
      .reduceByKey({
      (V1, V2) => (V1, V2) match {
        case ((info1: Double, split1: Double), (info2: Double, split2: Double)) =>

          (info1 + info2: Double, split1 + split2: Double)
      }
    }).map{ case ((attr,value), (info,split)) => (attr, (value,info,split))}
        .join(entropyTable).map{ case (attr,((value: String, info: Double, split: Double), entropy:Double)) =>

        //println(entropy+" "+info)
        val gain :Double = entropy-info

        if(split.abs == 0.0)
          ((attr,value), 0.0)
        else
          ((attr,value), (gain/split))
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


  }


}
