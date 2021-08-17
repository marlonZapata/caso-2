package Common

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Spark {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def createSession(): SparkSession = {
    logger.info("Creating session...")

    setHadoopProperties()

    val sparkSession = SparkSession
      .builder()
      .appName(name = "Caso2")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    logger.info("Session created")

    sparkSession
  }

  def setHadoopProperties(): Unit = {
    logger.info("Setting Hadoop properties...")
    System.setProperty("hadoop.home.dir", "D:\\BIgDataSetUp\\Hadoop")
    logger.info("Hadoop properties set")
  }

}
