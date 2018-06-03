import org.apache.spark.sql.SparkSession

object CsvTest {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName(this.getClass.getSimpleName)
      .enableHiveSupport()
      .getOrCreate()

    val sqlContext = sparkSession.sqlContext

    import sqlContext.implicits._

    val statArticleValues = sqlContext.createDataset(Seq(StatArticleValue(1, 100), StatArticleValue(2, 50), StatArticleValue(3, 45)))
    val dailyArticleValues = sqlContext.createDataset(Seq(DailyArticleValue(1, 10), DailyArticleValue(3, 5), DailyArticleValue(2, 15)))

    statArticleValues.show()
    dailyArticleValues.show()

    val joinArticleValue = statArticleValues.join(dailyArticleValues, "questionId")

    joinArticleValue.show()
    joinArticleValue.printSchema()

    joinArticleValue.createOrReplaceTempView("join_article_value")

    sqlContext.sql("SELECT questionId, wizCoinSum + dailyArticleValue AS total FROM join_article_value").show()
  }
}

case class StatArticleValue(questionId: Long, wizCoinSum: BigDecimal)
case class DailyArticleValue(questionId: Long, dailyArticleValue: BigDecimal)