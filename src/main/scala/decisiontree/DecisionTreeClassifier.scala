package decisiontree

import decisiontreealg.{MapReduceAlgorithm, SequentialAlgorithm}
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType


class DecisionTreeClassifier(override val uid: String) extends Estimator[DecisionTreeModel]
  with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("decisionTree"))

  // Define parameters for your decision tree, e.g., depth, impurity, etc.
  val maxDepth: Param[Int] = new Param[Int](this, "maxDepth", "Maximum depth of the decision tree")

  def getMaxDepth: Int = $(maxDepth)

  def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  val countLabels: Param[(Double, Double)] = new Param[(Double, Double)](this, "countLabels", "count for each label in train set")

  def setCountLabel(value: (Double, Double)): this.type = set(countLabels, value)

  def getCountLabel: (Double, Double) = $(countLabels)
  // Override transformSchema to specify the input and output schema of your decision tree model
  override def transformSchema(schema: StructType): StructType = {
    // Your implementation here
    schema
  }

  // Implement the fit method to train your decision tree model
  override def fit(dataset: Dataset[_]): DecisionTreeModel = {
    // Your training logic here
    // For simplicity, let's assume you have a feature column called "features" and a label column called "label"

    // val alg3: MapReduceAlgorithm = new MapReduceAlgorithm()
    val alg: SequentialAlgorithm = new SequentialAlgorithm()
    val countLabel0 = dataset.filter(col("ground_truth") === 0).count().toDouble
    val countLabel1 = dataset.filter(col("ground_truth") === 1).count().toDouble

    set(countLabels, (countLabel0, countLabel1))
    //val decTree = alg3.startAlgorithm(dataset.toDF, $(maxDepth), countLabel0, countLabel1)
    val decTree = alg.startAlgorithm(dataset.toDF, $(maxDepth), $(countLabels)._1, $(countLabels)._2)
    // For now, let's create a dummy model
    val model = new DecisionTreeModel(uid).setDecisionTree(new DecisionTree(decTree))

    // Return the trained model
    model
  }

  override def copy(extra: ParamMap): DecisionTreeClassifier = defaultCopy(extra)
}
