import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import CONSTANTS._
import LastTouch_CONSTANTS._

//args[0] - input folder
//args[1] - output folder
//args[2] - date format


object LastTouch {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Last Touch").getOrCreate()
    import spark.implicits._

    val unix_to_date_udf   = spark.udf.register("unix_to_date_udf",unix_to_date)
    val last_touch_udf     = spark.udf.register("last_touch_udf",last_touch)

    val format_template = args(2).toLowerCase match {
      case "year"       => "yyyy"
      case "month"      => "yyyy-MM"
      case "day"        => "yyyy-MM-dd"
      case "hour"       => "yyyy-MM-dd:HH"
      case "minute"     => "yyyy-MM-dd:HH-mm"
      case "second"     => "yyyy-MM-dd:HH-mm-ss"
      case "milisecond" => "yyyy-MM-dd:HH-mm-ss-ms"
      case e@_          => throw new Exception(s"`$e` format does not exist")

    }

    val data = spark.read.
      format("csv").
      option("inferSchema","false").
      option("header","true").
      option("mergeSchema","true").
      load(args(0))

    val data_seq = data.
      withColumn("channels",split(col(USER_PATH),TRANSIT)).
      withColumn("last_touch",last_touch_udf($"channels")).
      withColumn("date_touch",unix_to_date_udf(col(TIMELINE),lit(TRANSIT),lit(format_template))).
      select($"channels",$"last_touch",$"date_touch")

//    data_seq.show()


    val data_explode = data_seq.
      withColumn("touch_data",explode(arrays_zip($"last_touch",$"date_touch",$"channels"))).
      select(
        $"touch_data.last_touch".as("last_touch"),
        $"touch_data.date_touch".as("date_touch"),
        $"touch_data.channels".as("channels")
      )

    val data_touch_agg = data_explode.
      groupBy($"channels",$"date_touch").
      agg(sum($"last_touch").as("last_touch"))

//    val data_general = data_explode.
//      groupBy($"channels").
//      agg(sum($"last_touch").as("last_touch"))
    data_touch_agg.show()



//    data_touch_agg.
//      write.format("csv").
//      option("header","true").
//      mode("overwrite").
//      save(args(1))









  }
}
