import org.apache.spark.dxy.monitor.ErrorMsgCollectorListener
import org.apache.spark.sql.SparkSession

/**
 * Created by fchen on 2017/9/11.
 */
object Test{
  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    val spark = SparkSession
      .builder()
      .appName("Spark count example")
      .master("local[4]")
      .getOrCreate()
//    spark.sparkContext.addSparkListener(new ErrorMsgCollectorListener())

    println(spark.sparkContext.parallelize(1 to 1000).map(x => {
      throw new RuntimeException("test msg collect")
    }).count())

  }
}
