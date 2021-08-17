import org.apache.spark.sql.{DataFrame, SparkSession}
import Common.Spark
import Common.Printer
import org.slf4j.LoggerFactory

object Main {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    /*
    val PATH: String = args(0)
    */
    logger.info("Main method started...")
    logger.warn("Caso 2 application starts - Jovenes Prosefionales")

    val PATH: String = "D:\\Indra\\Chamba\\BCP\\Capacitación y seguimiento\\Ejercicios\\m_desmoneda\\000000_0.snappy.parquet"

    val STATS_LEVEL: Int = 4

    val PARTITION_ID: String = "fecactualizacionregistro"

    val sparkSession: SparkSession = Spark.createSession()

    val df = sparkSession.read.parquet(PATH)

    dfStats(df, STATS_LEVEL, PARTITION_ID)

    logger.warn("Caso 2 application ends. GOOD BYE")

  }

  def dfStats(df: DataFrame, STATS_LEVEL: Int, PARTITION_ID: String):Unit = {

    val PARTITION_NUM: Int = getNumPartition(df)

    if (isGreaterThanZero(STATS_LEVEL)) {
      Printer.greaterThanZero(df, PARTITION_NUM)
    }

    if (isTwoOrFour(STATS_LEVEL)) {
      Printer.twoOrFour(df)
    }

    // check the partitions data
    if (isThreeOrFour(STATS_LEVEL)) {
      Printer.threeOrFour(df, PARTITION_ID, PARTITION_NUM)
    }

    if (isEight(STATS_LEVEL)) {
      Printer.eight(df)
    }
  }

  def getNumPartition(df: DataFrame): Int = {
    df.rdd.getNumPartitions
  }

  def isGreaterThanZero(STATS_LEVEL: Int): Boolean = {
    STATS_LEVEL > 0
  }

  def isTwoOrFour(STATS_LEVEL: Int): Boolean = {
    STATS_LEVEL == 2 || STATS_LEVEL== 4
  }

  def isThreeOrFour(STATS_LEVEL: Int): Boolean = {
    STATS_LEVEL == 3 || STATS_LEVEL ==4
  }

  def isEight(STATS_LEVEL: Int) = {
    STATS_LEVEL == 8
  }
}