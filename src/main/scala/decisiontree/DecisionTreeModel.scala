package decisiontree

import org.apache.spark.ml.Model
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.types.{ArrayType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

class NewDecisionTreeModel(override val uid: String) extends Model[NewDecisionTreeModel]
  with DefaultParamsWritable {

  // Define parameters for your decision tree, e.g., depth, impurity, etc.
  val decisionTree: Param[NewDecisionTree] = new Param[NewDecisionTree](this, "decisionTree", "Decision tree")

  def getDecisionTree: NewDecisionTree = $(decisionTree)

  def setDecisionTree(value: NewDecisionTree): this.type = set(decisionTree, value)

  def this() = this(Identifiable.randomUID("decisionTreeModel"))

  // Implement the transform method for making predictions
  override def transform(dataset: Dataset[_]): DataFrame = {
    // Your prediction logic here
    // For simplicity, let's return the input dataset as is
    val windowSpec = Window.orderBy("ground_truth")

    val dataframe: DataFrame = dataset.toDF

    // Aggiungi una colonna con l'indice a partire da 0
    val dfWithIndex = dataframe.withColumn("Index", row_number().over(windowSpec))
    val predictionsArray: Array[(Int, Int)] = $(decisionTree).predict(dfWithIndex)

    // Define the schema for the DataFrame
    val schema = StructType(Seq(StructField("Index", IntegerType, true), StructField("Prediction", IntegerType, true)))

    // Create a Seq of Rows with the array elements
    val rows = predictionsArray.map(pair => Row(pair._1, pair._2))

    // Create the DataFrame using createDataFrame with the explicit schema and Seq of Rows
    val predictionsCol = dataset.sparkSession.createDataFrame(dataset.sparkSession.sparkContext.parallelize(rows), schema)

    // Perform the join based on the indices
    val joinedDF = predictionsCol.join(dfWithIndex, "Index")
    joinedDF.show()

    // Reorder the result based on the original order
    val finalDF = joinedDF.sort("Index").drop("Index")

    finalDF
  }

  // Override transformSchema to specify the input and output schema of your decision tree model
  override def transformSchema(schema: StructType): StructType = {
    // Your implementation here
    StructType(schema :+ StructField("Prediction", ArrayType(IntegerType)))
  }

  override def copy(extra: ParamMap): NewDecisionTreeModel = defaultCopy(extra)
}


abstract class TreeNode extends Serializable
case class Node(value: Double, feature: String, var left: TreeNode, var right: TreeNode, parent: Option[TreeNode]) extends TreeNode {

  def getFeature: String = feature
  def getValue: Double = value

  def getParent: Option[TreeNode] = parent
  def getRight: TreeNode = right
  def addRight(node: TreeNode): Unit = right = node
  def getLeft: TreeNode = left
  def addLeft(node: TreeNode): Unit = left = node
}
case class Leaf(parent: Option[TreeNode], label: Int) extends TreeNode {

  def getLabel: Int = label
  def getParent: Option[TreeNode] = parent

}
class NewDecisionTree(tree: TreeNode) extends Serializable {

  def printAllNodes(decisionTree: TreeNode): Unit = {

    decisionTree match {
      case Node(value, feature, right, left, _) =>
        println(s"Node: $value, Feature: $feature")


        print("Right: ")
        printAllNodes(right)
        print("Left: ")
        printAllNodes(left)

      case Leaf(_, label) => println(s"Leaf: $label")
    }

  }

  def getTreeNode: TreeNode = tree

  // Methods to save and load decision tree
  def saveToFile(filePath: String): Unit = {
    val fileOutputStream = new FileOutputStream(filePath)
    val objectOutputStream = new ObjectOutputStream(fileOutputStream)
    objectOutputStream.writeObject(this)
    objectOutputStream.close()
    fileOutputStream.close()
  }

  def loadFromFile(filePath: String): NewDecisionTree = {
    val fileInputStream = new FileInputStream(filePath)
    val objectInputStream = new ObjectInputStream(fileInputStream)
    val loadedTree = objectInputStream.readObject().asInstanceOf[NewDecisionTree]
    objectInputStream.close()
    fileInputStream.close()
    loadedTree
  }

  def predict(test: DataFrame): Array[(Int, Int)] = {


    // Convert DataFrame to RDD and use zipWithIndex
    val resultArray = test.rdd.map {
      row =>
        val index = row.getAs[Int]("Index")
        var currentNode = tree
        var stop: Boolean = false
        var valueToRet = -1

        while (!stop) {
          currentNode match {

            case Leaf(_, label) =>

              valueToRet = label
              stop = true

            case Node(value, feature, right, left, _) =>

              val current_val = row.getAs[Double](feature)

              currentNode = if(value < current_val) left else right
          }
        }

        // Return the result along with the index
        (index, valueToRet)
    }.collect()

    resultArray
  }

}


