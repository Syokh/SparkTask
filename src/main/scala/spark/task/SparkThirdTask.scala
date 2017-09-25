package spark.task

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import spark.Config.pathToCar
import spark.common.ProcessingHandling
import spark.common.SparkConnection._

object SparkThirdTask extends App with ProcessingHandling{
 
  errorHandlingDF(getDataFrame(pathToCar))(res => res.show())
  session.stop()

  def getDataFrame(path: String): DataFrame = {
    val df = session.read.option("header", "true").csv(path).groupBy("type")
    df.agg(avg("number").alias("avg"), min("number").alias("min"), max("number").alias("max"))
  }
}
