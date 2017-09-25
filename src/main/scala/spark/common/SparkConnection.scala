package spark.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkConnection {
  val conf: SparkConf = new SparkConf().setAppName("MyTest").setMaster("local")
  val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  session.sparkContext.setLogLevel("WARN")
}
