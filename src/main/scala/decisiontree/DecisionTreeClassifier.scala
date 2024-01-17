package decisiontree

import decisiontreealg.MapReduceAlgorithm
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

  def getCountLabel: (Double, Double) = $(countLabels)
  override def transformSchema(schema: StructType): StructType = {

    schema
  }


  override def fit(dataset: Dataset[_]): DecisionTreeModel = {


    val alg3: MapReduceAlgorithm = new MapReduceAlgorithm()

    val countLabel0 = dataset.filter(col("ground_truth") === 0).count().toDouble
    val countLabel1 = dataset.filter(col("ground_truth") === 1).count().toDouble

    set(countLabels, (countLabel0, countLabel1))
    val decTree = alg3.startAlgorithm(dataset.toDF, $(maxDepth), countLabel0, countLabel1)

    val model = new DecisionTreeModel(uid).setDecisionTree(new DecisionTree(decTree))

    // Return the trained model
    model
  }

  override def copy(extra: ParamMap): DecisionTreeClassifier = defaultCopy(extra)
}
