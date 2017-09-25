package spark.common

import org.apache.spark.sql._
import scala.util.{Failure, Success, Try}

trait ProcessingHandling {
  def errorHandlingDF(df: => DataFrame)(res: DataFrame => Unit): Unit ={
    Try(df) match {
      case Success(result) => res(result)
      case Failure(ex) => println(s"wrong path to file or data in file, msg: ${ex.getMessage}")
    }
  }
  
  def errorHandlingOP(path: String, df: DataFrame)(f: (String, DataFrame) => Unit): Unit ={
    Try(f(path, df)) match {
      case Success(result) => println("success result")
      case Failure(ex) => println(s"wrong path to write the file: ${ex.getMessage}")
    }
  }
}
