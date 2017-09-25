package spark.task

import org.apache.spark.rdd.RDD
import spark.Config.pathToCar
import spark.common.SparkConnection.session
import spark.model.CSVHeader

object SparkSecondTask extends App {
  val sc = session.sparkContext
  
  try{
    val rdd = rddGetTypeANDNumber(pathToCar).groupBy(x => x._1)
    val result = rdd.map(res =>(res._1, getAverage(res._2)))
  
    println("type, number")
    result.foreach(res => println(res._1 + ", " + res._2))
  } catch {
    case ex: Throwable => println(s"wrong path to file or data in file msg: ${ex.getMessage}")
  } finally {
    sc.stop()
  }
  
  def getAverage(iterable: Iterable[(String, String)]): Double ={
    val list = iterable.toList
    list.map(_._2.toInt).sum / list.size.toDouble
  }
 
  def rddGetTypeANDNumber(path: String): RDD[(String, String)] = {
    val data = sc.textFile(path).map(line => line.split(","))
    val header = new CSVHeader(data.take(1)(0))
    val rows = data.filter(line => header(line,"type") != "type")
    rows.map(row => (header(row, "type"), header(row, "number")))
  }
}
