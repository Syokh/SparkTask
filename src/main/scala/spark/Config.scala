package spark

import com.typesafe.config.ConfigFactory

object Config  {
  private val configFactory = ConfigFactory.load().getConfig("app")
  
  val pathToCar = configFactory.getString("pathToCar")
  val pathToUser = configFactory.getString("pathToUser")
  val outputCSV = configFactory.getString("outputCSV")
  val outputParquet = configFactory.getString("outputParquet")
}
