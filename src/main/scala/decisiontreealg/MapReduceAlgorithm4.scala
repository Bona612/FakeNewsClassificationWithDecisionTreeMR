package decisiontreealg

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.sql.DataFrame

import scala.math.log10


class MapReduceAlgorithm4() {
  def startAlgorithm(dataset: DataFrame, maxDepth: Int): DecisionTree = {

    val cols = dataset.columns.dropRight(1) //discard class column

    val idx_label = dataset.columns.length-1

    mainAlgorithm(dataset, cols, idx_label,None, 0)
  }

  private def mainAlgorithm(dataset: DataFrame, cols: Array[String], idx_label: Int, par: Option[Node], depthTree: Int): DecisionTree = {


    println("start calc entropy...")
    //entropy to understand if we arrived in the situation of only or most of instance of a class. (0 is purity)
    val (entropy, (maxClass, _), allTable) = calcEntropyTable(dataset)

    if (entropy <= 0.3f || depthTree >= 100) { //stop check
      return Leaf(label = maxClass, parent = par)
    }

    println("start data preparation...")

    val countTableSplit = dataPreparation(dataset, cols, idx_label)

    val startTimeMillis = System.currentTimeMillis()

    if (countTableSplit == null) { //stop check
      return Leaf(label = maxClass, parent = par)
    }

    println("find best split...")
    val bestSplit = findBestSplit(countTableSplit, entropy, allTable.toInt)

    val (bestAttr, bestValue) = (bestSplit._1, bestSplit._2)
    println(bestAttr, bestValue)

    println(s"num partition dataset: ${dataset.rdd.getNumPartitions}")

    val bestAttr1: Option[String] = bestAttr.lift(0)
    val bestAttr2: Option[String] = bestAttr.lift(1)

    val bestValue1: Option[Double] = bestValue.lift(0).map(_.asInstanceOf[Double])
    val bestValue2: Option[Double] = bestValue.lift(1).map(_.asInstanceOf[Double])

    val ggDataset = dataset.filter { row =>
      val index1 = bestAttr1.flatMap(attr => Option(row.schema.fieldIndex(attr)))
      val index2 = bestAttr2.flatMap(attr => Option(row.schema.fieldIndex(attr)))

      val condition1 = index1.exists(idx => row.getDouble(idx) >= bestValue1.get)

      val condition2 = bestValue2.fold(true) { value2 =>
        index2.exists(idx => row.getDouble(idx) >= value2)
      }

      condition1 && condition2
    }

    val glDataset = dataset.filter { row =>
      val index1 = bestAttr1.flatMap(attr => Option(row.schema.fieldIndex(attr)))
      val index2 = bestAttr2.flatMap(attr => Option(row.schema.fieldIndex(attr)))

      val condition1 = index1.exists(idx => row.getDouble(idx) >= bestValue1.get)

      val condition2 = bestValue2.fold(true) { value2 =>
        index2.exists(idx => row.getDouble(idx) < value2)
      }

      condition1 && condition2
    }

    val llDataset = dataset.filter { row =>
      val index1 = bestAttr1.flatMap(attr => Option(row.schema.fieldIndex(attr)))
      val index2 = bestAttr2.flatMap(attr => Option(row.schema.fieldIndex(attr)))

      val condition1 = index1.exists(idx => row.getDouble(idx) < bestValue1.get)

      val condition2 = bestValue2.fold(true) { value2 =>
        index2.exists(idx => row.getDouble(idx) < value2)
      }

      condition1 && condition2
    }

    val lgDataset = dataset.filter { row =>
      val index1 = bestAttr1.flatMap(attr => Option(row.schema.fieldIndex(attr)))
      val index2 = bestAttr2.flatMap(attr => Option(row.schema.fieldIndex(attr)))

      val condition1 = index1.exists(idx => row.getDouble(idx) < bestValue1.get)

      val condition2 = bestValue2.fold(true) { value2 =>
        index2.exists(idx => row.getDouble(idx) >= value2)
      }

      condition1 && condition2
    }

    val currentNode = Node(bestAttr(0), bestValue(0), null, null, par)

    val endTimeMillis = System.currentTimeMillis()
    // Calculate the elapsed time
    val elapsedTimeMillis = endTimeMillis - startTimeMillis
    // Print the result
    println(s"Elapsed Time: $elapsedTimeMillis milliseconds")
    println("iterate right and left...")


    val rightright = mainAlgorithm(ggDataset, cols, idx_label, Option(currentNode), depthTree+1)
    val rightleft = mainAlgorithm(glDataset, cols, idx_label, Option(currentNode), depthTree+1)
    val leftleft = mainAlgorithm(llDataset, cols, idx_label, Option(currentNode), depthTree + 1)
    val leftright = mainAlgorithm(lgDataset, cols, idx_label, Option(currentNode), depthTree + 1)

    dataset.unpersist()

    currentNode.insertLeftChild(leftleft)
    currentNode.insertRightChild(rightright)

    currentNode
  }

  private def calcEntropyTable(dataset: DataFrame): (Double, (String, Double), Double) = {

    val log2: Double => Double = (x: Double) =>
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)


    /*val roundDouble : Double => Double = (value: Double) =>
      BigDecimal(value).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
*/
    val calcEntropy: Double => Double = (p: Double) => -p * log2(p)

    /*
    val countLabel0 = dataset.filter(row => row(idx_label).toString.toInt == 0).count().toDouble
    val countLabel1 = dataset.filter(row => row(idx_label).toString.toInt == 1).count().toDouble


    val countLabel0 = dataset.filter(col("ground_truth") === "0").count().toDouble
    val countLabel1 = dataset.filter(col("ground_truth") === "1").count().toDouble
    */

    var counts = dataset.cache()
      .groupBy("ground_truth").count().collect()

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
      entropy = (calcEntropy(countLabel0 / allValue) + calcEntropy(countLabel1 / allValue)).abs

    println("entropy: " + entropy)
    println("0: " + countLabel0, "1: " + countLabel1)

    counts = null

    (entropy, maxKey, allValue)
  }

  def dataPreparation(dataset: DataFrame, cols: Array[String], idx_label: Int) :RDD[((Seq[String], Seq[String]), (Int, Int))] = {

    /*
    * Map of splitpoints (mean of adiacent values) for attributes, if attr have only one value we discard it
    */

    val startTimeMillis = System.currentTimeMillis()
    if (dataset.drop("ground_truth").distinct().head(2).length > 1) {


      println(s"is cached: ${dataset.storageLevel.useMemory}")

      val attrTable: RDD[(Seq[String], (Int, Seq[Double]))] = dataset.rdd
        .flatMap {
          row =>
            cols.zipWithIndex.flatMap {
              case (col1, idx1) =>
                cols.zipWithIndex.map {
                  case (col2, idx2) =>
                    if (!col1.equals(col2)) {
                      (Seq(col1, col2), (row(idx_label).asInstanceOf[Int], Seq(row(idx1).asInstanceOf[Double], row(idx2).asInstanceOf[Double])))
                    }
                    else {
                      (Seq(col1), (row(idx_label).asInstanceOf[Int], Seq(row(idx1).asInstanceOf[Double])))
                    }
                }
            }
        }.partitionBy(new HashPartitioner(6)).persist()
      //attrTable.foreach(println)

      // Define functions for combineByKey
      def createCombiner(value: (Int, Seq[Double])): Seq[Seq[Double]] = {
        if (value._2.length > 1) {
          Seq(Seq(value._2(0)), Seq(value._2(1)))
        }
        else {
          Seq(Seq(value._2(0)))
        }
      }

      def mergeValue(comb: Seq[Seq[Double]], value: (Int, Seq[Double])): Seq[Seq[Double]] = {
        if (value._2.length > 1) {
          Seq(comb(0) :+ value._2(0), comb(1) :+ value._2(1))
        }
        else {
          Seq(comb(0) :+ value._2(0))
        }
      }

      def mergeCombiners(comb1: Seq[Seq[Double]], comb2: Seq[Seq[Double]]): Seq[Seq[Double]] = {
        comb1 ++ comb2
      }

      // Apply combineByKey
      val splitPointsTable: RDD[(Seq[String], Seq[Seq[Double]])] = attrTable
        .combineByKey(createCombiner, mergeValue, mergeCombiners)
        .mapValues((values: Seq[Seq[Double]]) =>
          if (values.length > 1 && values(1).nonEmpty) {
            Seq(values(0).distinct.sorted, values(1).distinct.sorted)
          }
          else {
            Seq(values(0).distinct.sorted)
          }
        )
        .filter {
          case (_, arrays) =>
            arrays.length > 0 && arrays(0).length > 1 && (arrays.length > 1 && arrays(1).length > 1)
        }
        .mapValues((values: Seq[Seq[Double]]) =>
          if (values.length > 1 && values(1).nonEmpty) {
            Seq(values(0).sliding(2).toList.map(pair => (pair.head + pair(1)) / 2.0), values(1).sliding(2).toList.map(pair => (pair.head + pair(1)) / 2.0))
          }
          else {
            Seq(values(0).sliding(2).toList.map(pair => (pair.head + pair(1)) / 2.0))
          }
        )
      //splitPointsTable.foreach(println)

      val countTableSplit = attrTable.rightOuterJoin(splitPointsTable) // : RDD[((Seq[String], String, Seq[String]), Int)]
        .flatMap {
          case (attrs, (label_and_val: Option[(Int, Seq[Double])], val_list: Seq[Seq[Double]])) =>

            val resultTuples = for {
              value1 <- val_list.lift(0).getOrElse(Seq.empty)
              value2Seq <- val_list.lift(1).getOrElse(Seq.empty).headOption.toSeq
              (label, labelAndVal) <- label_and_val.toSeq
            } yield {
              val condition1 = labelAndVal(0) < value1
              val condition2 = labelAndVal(1) < value2Seq

              val sequence = Seq(
                value1.toString + (if (condition1) "<" else ">="),
                value2Seq.toString + (if (condition2) "<" else ">=")
              )

              // Your logic using value2, e.g., value2.map(...)

              ((attrs, label, sequence), 1)
            }

            resultTuples

          /*val_list(0).map(
              value1 => {
                if (val_list(1).nonEmpty) {
                  val_list(1).map(
                    value2 => {
                      if (label_and_val.get._2(0) < value1) {
                        if (label_and_val.get._2(1) < value2) {
                          ((attrs, label_and_val.get._1, Seq(value1.toString + "<", value2.toString + "<")), 1)
                        }
                        else {
                          ((attrs, label_and_val.get._1, Seq(value1.toString + "<", value2.toString + ">=")), 1)
                        }
                      }
                      else {
                        if (label_and_val.get._2(1) < value2) {
                          ((attrs, label_and_val.get._1, Seq(value1.toString + ">=", value2.toString + "<")), 1)
                        }
                        else {
                          ((attrs, label_and_val.get._1, Seq(value1.toString + ">=", value2.toString + ">=")), 1)
                        }
                      }
                    }
                  )
                }
                else {
                  if (label_and_val.get._2(0) < value1) {
                    ((attrs, label_and_val.get._1, Seq(value1.toString + "<")), 1)
                  }
                  else {
                    ((attrs, label_and_val.get._1, Seq(value1.toString + ">=")), 1)
                  }
                }
              }
            )*/
        }
        .reduceByKey(_ + _)
        .map {
          case ((attr, label, value), count) =>
            ((attr, value), (label, count))
        }
        .partitionBy(new HashPartitioner(6))
        .persist()


      val endTimeMillis = System.currentTimeMillis()
      // Calculate the elapsed time
      val elapsedTimeMillis = endTimeMillis - startTimeMillis
      println("dionoiaaaaaaaaaaaaaaaaaaaaa " + elapsedTimeMillis)

      countTableSplit
    }
    else

      null

  }


  private def findBestSplit(countTableValue: RDD[((Seq[String], Seq[String]), (Int, Int))], entropyAll : Double, allTable: Int): (Seq[String], Seq[Double]) = {

    val log2: Double => Double = (x: Double) =>
      if (x.abs == 0.0) 0.0 else log10(x) / log10(2.0)


    val roundDouble : Double => Double = (value: Double) =>
      BigDecimal(value).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble

    val calcEntropy: Double => Double = (p: Double) => -p * log2(p) //roundDouble(-p * log2(p))

    val allTableSplit = countTableValue
      .mapValues{case (_,count) => count}
      .reduceByKey(_+_)

    //join between countTable for splitvalues and allTable for splitvalues
    val infoTable = countTableValue
      .join(allTableSplit)

    //infoTable.foreach(println)

    //gainratio table
    val gainRatioTable = infoTable.mapValues {
        case ((_, count), all) => (all, calcEntropy(count.toDouble / all.toDouble))
      }
      .reduceByKey { case ((all,entropyAttr1),(_,entropyAttr2)) => (all, entropyAttr1+entropyAttr2)}
      .map{
        case((attr,values),(allSplit,entropySplit)) =>

          // compute info and splitinfo for each (attr, splitvalue)
          val p = allSplit.toDouble / allTable.toDouble
          val info = p * entropySplit
          val splitinfo = calcEntropy(p)

          val valueList: Seq[Double] = values.map(value =>
            if (value.contains(">="))
              value.split(">=").apply(0).toDouble
            else
              value.split("<").apply(0).toDouble
          )

          ((attr, valueList), (info,splitinfo))
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
