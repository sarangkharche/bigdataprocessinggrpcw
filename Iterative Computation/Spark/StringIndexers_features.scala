import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{Bucketizer, StringIndexer, VectorAssembler}
val input = sc.textFile("/data/h1b/h1b_kaggle.csv")

val caseIndexer = new StringIndexer().setInputCol("CASE_STATUS").setOutputCol("caseIndex")

val snameIndexer = new StringIndexer().setInputCol("SOC_NAME").setOutputCol("s_nameIndex")

val ftIndexer = new StringIndexer().setInputCol("FULL_TIME_POSITION").setOutputCol("ftIndex")

val wageIndexer = new StringIndexer().setInputCol("PREVAILING_WAGE").setOutputCol("wageIndex")

val yearIndexer = new StringIndexer().setInputCol("YEAR").setOutputCol("yearIndex")

val assembler = new VectorAssembler().setInputCols(Array(caseIndexer.getOutputCol,snameIndexer.getOutputCol,ftIndexer.getOutputCol,wageIndexer.getOutputCol,yearIndexer.getOutputCol))

val pipeline = new Pipeline().setStages(Array(caseIndexer,snameIndexer,ftIndexer,wageIndexer,yearIndexer,assembler))

val model = pipeline.fit(input)

model.transform(input).show()
