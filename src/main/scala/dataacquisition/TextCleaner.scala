package dataacquisition

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions.{col, lower, regexp_replace, trim}
import org.apache.spark.sql.{Column, DataFrame, Dataset}

class TextCleaner(override val uid: String) extends Transformer with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("textCleaner"))

  val inputCol: Param[String] = new Param(this, "inputCol", "Input column to clean")
  val outputCol: Param[String] = new Param(this, "outputCol", "Output column after cleaning")

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def getOutputCol: String = $(outputCol)

  def copy(extra: ParamMap): TextCleaner = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val inputColName = $(inputCol)
    val outputColName = $(outputCol)

    // Define a regular expression pattern to match Twitter user mentions
    val mentionPattern = "@[a-zA-Z0-9_]+"
    // Define a regular expression pattern to match URLs
    val urlPattern = """\b(?:https?|ftp|com):\/\/\S+"""

    // Define a list of transformations as functions
    val transformations: List[Column => Column] = List(
      trim, // Trim whitespaces
      lower, // Convert to lowercase
      c => regexp_replace(c, mentionPattern, ""), // Remove Twitter user mentions
      c => regexp_replace(c, urlPattern, ""), // Remove URLs
      c => regexp_replace(c, "\n", ""), // Remove newline characters
      c => regexp_replace(c, "\t", ""), // Remove tab characters
      c => regexp_replace(c, "\\d+", ""), // Remove numbers
      c => regexp_replace(c, "\\s+", " "), // Remove consecutive whitespace characters
      c => regexp_replace(c, "[^a-zA-Z0-9\\s]", "") // Remove punctuation
    )

    val dataframe: DataFrame = dataset.toDF()

    // Apply the transformations to the specified column
    val cleanedDF: DataFrame = transformations.foldLeft(dataframe)((df, transformation) =>
      df.withColumn(outputColName, transformation(col(inputColName)))
    )

    // DA CAMBIARE QUESTO, è LEGGERMENTE DIVERSO DA COSì
    // Filter out empty strings and rows with only whitespace characters
    val dfWoutWhitespace: DataFrame = cleanedDF.filter(trim(col(inputColName)) =!= "")

    val dfWoutDup: DataFrame = dfWoutWhitespace.dropDuplicates() // Drop duplicates

    dfWoutDup
  }

  override def transformSchema(schema: org.apache.spark.sql.types.StructType): org.apache.spark.sql.types.StructType = {
    // Assuming the input and output columns are of the same type and structure
    schema
  }
}

