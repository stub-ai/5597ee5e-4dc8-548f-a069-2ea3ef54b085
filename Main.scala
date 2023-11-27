import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Usage: Main <file_path> <column_name>")
      System.exit(1)
    }

    val filePath = args(0)
    val columnName = args(1)

    val spark = SparkSession.builder.appName("CSV Reader").getOrCreate()

    val df = spark.read.format("csv").option("header", "true").load(filePath)

    val cleanedDf = df.na.drop()

    val avgValue = cleanedDf.select(columnName).rdd.map(_(0).toString.toDouble).mean()

    println(s"Average value of $columnName: $avgValue")

    spark.stop()
  }
}