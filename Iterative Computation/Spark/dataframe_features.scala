import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType};
import org.apache.spark.sql.Row;

val csv = sc.textFile("/data/h1b/h1b_kaggle.csv")
val rows = csv.map(line => line.split(",").map(_.trim))
val header = rows.first
val data = rows.filter(_(0) != header(0))
val rdd = data.map(row => Row(row(0),row(1).toInt))

val schema = new StructType()
.add(StructField("id", IntegerType, true))
.add(StructField("CASE_STATUS", StringType, true))
.add(StructField("EMPLOYER_NAME", StringType, true))
.add(StructField("SOC_NAME", StringType, true))
.add(StructField("JOB_TITLE", StringType, true))
.add(StructField("FULL_TIME_POSITION", StringType, true))
.add(StructField("PREVAILING_WAGE", IntegerType, true))
.add(StructField("YEAR", IntegerType, true))

val df = sqlContext.createDataFrame(rdd, schema)

df.printSchema()
