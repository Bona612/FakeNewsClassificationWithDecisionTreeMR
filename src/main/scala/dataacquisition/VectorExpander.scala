package dataacquisition

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

class VectorExpander(override val uid: String) extends Transformer {

  def this() = this(Identifiable.randomUID("vectorExpander"))

  val inputCol: Param[String] = new Param(this, "inputCol", "Input column with sparse vectors")
  val outputCol: Param[String] = new Param(this, "outputCol", "Output column with expanded values")
  val vocabulary: Param[Seq[String]] = new Param(this, "vocabulary", "Vocabulary of the dataset")
  val vocabSize: Param[Integer] = new Param(this, "vocabSize", "Size of the vocabulary")

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setVocabulary(value: Seq[String]): this.type = set(vocabulary, value)

  def setVocabSize(value: Integer): this.type = set(vocabSize, value)

  def copy(extra: ParamMap): VectorExpander = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val inputColName = $(inputCol)
    val outputColName = $(outputCol)
    val vocab = $(vocabulary)

    val expandUDF = udf { (v: Vector) => v.toArray }



    val vectorToArray = udf((v: Vector) => v.toArray.map(BigDecimal(_).setScale(5, BigDecimal.RoundingMode.HALF_UP).toDouble))

    // Apply the UDF to the "features" column
    val rescaledDataWArray = dataset.withColumn("features_array", vectorToArray(col(inputColName)))
    //val rescaledDataWArray2 = dataset.withColumn("features_array", expandUDF(col(inputColName)))

    // (0 until $(vocabSize))
    val newColumns = vocab.zipWithIndex.map {
      case (alias, idx) => col("features_array").getItem(idx).alias(alias) // s"word_$alias"
    }

    rescaledDataWArray.select(newColumns :+ col("ground_truth"): _*)
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val outputColName = $(outputCol)

    val inputColField = schema(inputColName)
    val outputColField = StructField(outputColName, ArrayType(DoubleType))

    val newColumnsStructField = (0 until $(vocabSize)).zipWithIndex.map {
      case (alias, idx) => StructField(s"word_$alias", ArrayType(DoubleType))
    }.toArray

    // schema.fields ++
    StructType(newColumnsStructField :+ StructField("ground_truth", ArrayType(IntegerType)))
  }

}
