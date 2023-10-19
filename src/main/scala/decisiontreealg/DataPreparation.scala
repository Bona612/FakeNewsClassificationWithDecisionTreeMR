package decisiontreealg

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable
import scala.math.log10


class DataPreparation (table : DataFrame){

  private var attribTable : RDD[(String, (Int, Int, Option[Dataset[Float]]))] = _
  private var countTable : RDD[(String, (Int, Int))] = _
  private var hashTable : mutable.HashMap[Int,List[Int]] = _


  def createAttribTable(): Unit = {

    val cols: Array[String] = table.columns



    val col_values = mutable.Map[String, Dataset[Float]]()
    /*for( col <- cols){
      col_values += col, table.select(col).distinct()
    }*/


    attribTable = table.rdd.zipWithIndex().flatMap{ case (row, rowIndex) =>


      val label_id = row.schema.fieldIndex("label")
      val label = row.getInt(label_id)
      val pairs = row.schema.fieldNames.map{ col =>

        (col,(rowIndex.intValue(), label, col_values.get(col)))
      }

      pairs
    }

    /*
    val countLabelAttr = attribTable.map{ case (attr, (row_id1, label, values1)) =>

      (attr, label)
    }.groupByKey().mapValues(_.groupBy(identity).view.mapValues(_.size)) */

    countTable = attribTable.map { case (attr, (row_id, label, values)) =>

      ((attr, label),1)
    }.reduceByKey(_ + _).map{ case ((attr,label),count) => (attr,(label,count))}


    val allTable : RDD[(String,Int)] = countTable.mapValues {case (_,count) => count}.reduceByKey(_+_)

    val infoTable = countTable.join(allTable)

    val log2 = (x: Double) => log10(x)/log10(2.0)

    val entropyTable = infoTable.mapValues{ case((label, count),all) =>
      val p :Double = count.toDouble / all.toDouble
      val entropy :Double = -(p * log2(p))
      entropy
    }.reduceByKey(_+_)

    println(entropyTable.collect().mkString("Array(", ", ", ")"))

    def computeInfoSplit(rdd: (((Int,Int), Int), Double)) : (Double, Double) = {

      val count = rdd._1._1._2
      val all = rdd._1._2
      val entropy = rdd._2

      val p: Double = count.toDouble / all.toDouble
      val info: Double = p * entropy
      val splitInfo: Double = -p * log2(p)
      (info: Double, splitInfo: Double)
    }

    val gainRatioTable = infoTable.join(entropyTable).mapValues(computeInfoSplit).reduceByKey({
      (V1, V2) => (V1, V2) match {
        case ((info1: Double, split1: Double), (info2: Double, split2: Double)) =>

          (info1 + info2: Double, split1 + split2: Double)
      }
    }).join(entropyTable).mapValues{ case((info: Double, split: Double), entropy:Double) =>

      val gain :Double = entropy-info

      gain/split : Double
    }

    val argmax = gainRatioTable.reduce{ case ((attr1,gainRatio1),(attr2, gainRatio2)) =>

      if(gainRatio1 > gainRatio2)
        (attr1,gainRatio1)
      else
        (attr2, gainRatio2)

    }._1

    println(argmax)


  }




}
