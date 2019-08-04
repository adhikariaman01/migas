import org.apache.spark.{SparkConf, SparkContext}

object FirstSparkApplication {
  def main(args:Array[String ]) : Unit = {
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(Array(1,2,3,4,5,6))
    rdd1.collect().foreach(println)
  }
}
