package decisiontree

import decisiontreealg.MapReduceAlgorithm
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType


class DecisionTreeClassifier(override val uid: String) extends Estimator[NewDecisionTreeModel]
  with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("decisionTree"))

  // Define parameters for your decision tree, e.g., depth, impurity, etc.
  val maxDepth: Param[Int] = new Param[Int](this, "maxDepth", "Maximum depth of the decision tree")

  def getMaxDepth: Int = $(maxDepth)

  def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  // Override transformSchema to specify the input and output schema of your decision tree model
  override def transformSchema(schema: StructType): StructType = {
    // Your implementation here
    schema
  }

  // Implement the fit method to train your decision tree model
  override def fit(dataset: Dataset[_]): NewDecisionTreeModel = {
    // Your training logic here
    // For simplicity, let's assume you have a feature column called "features" and a label column called "label"

    val alg3: MapReduceAlgorithm = new MapReduceAlgorithm()
    //val alg: MapReduceAlgorithm_vDF = new MapReduceAlgorithm_vDF()

    val startTimeMillis = System.currentTimeMillis()

    val decTree = alg3.startAlgorithm(dataset.toDF, $(maxDepth))
    //val decTree: DecisionTree = alg.startAlgorithm_vDF(dataset.toDF, $(maxDepth))

    // Record the end time
    val endTimeMillis = System.currentTimeMillis()

    // Calculate the elapsed time
    val elapsedTimeMillis = endTimeMillis - startTimeMillis
    // Print the result
    println(s"Elapsed Time: $elapsedTimeMillis milliseconds")



    // For now, let's create a dummy model
    val model = new NewDecisionTreeModel(uid).setDecisionTree(new NewDecisionTree(decTree))

    // Return the trained model
    model
  }

  override def copy(extra: ParamMap): DecisionTreeClassifier = defaultCopy(extra)
}