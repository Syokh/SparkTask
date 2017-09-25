package spark.task

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import spark.common.SparkConnection._
import spark.Config._
import spark.common.ProcessingHandling
import spark.model.{Cars, Users}

object SparkFirstTask extends App with ProcessingHandling{
  import session.implicits._
  val sc = session.sparkContext
  
  errorHandlingDF(join(getDataset(pathToCar), rddGetIdANDName(pathToUser))){
    result =>
      errorHandlingOP(outputParquet, result)(saveParquet)
      errorHandlingOP(outputCSV, result)(saveCSV)
  }
  
  sc.stop()
  
  def join(ds: Dataset[Cars], rdd: RDD[Users]): DataFrame = {
    val userDF = rdd.toDF("ids", "name")
    val joinDF = ds.join(userDF, ds("user_id") === userDF("ids"), "left")
    joinDF.select($"id", $"model", $"user_id", $"name")
  }
  
  def getDataset(path: String): Dataset[Cars] =
    session.read.option("header", "true").csv(path).as[Cars].filter($"valid" === 0)
  
  def saveParquet(path: String, df: DataFrame): Unit = df.write.mode("overwrite").partitionBy("id").parquet(path)
  
  def saveCSV(path: String, df: DataFrame): Unit =
    df.coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("overwrite")
      .save(path)
  
  def rddGetIdANDName(path: String): RDD[Users] = {
    sc.textFile(path).collect {
      case line if line.split(",").last == "0" =>
        line.split(",").toList match {
          case id :: name :: _ => Users(id, name)
        }
    }
  }
}
