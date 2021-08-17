import org.apache.spark.sql.{DataFrame, SparkSession}
import Common.Spark
import Common.Printer
import org.slf4j.LoggerFactory

object Main {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Main method started...")
    logger.warn("Caso 2 application starts - Jovenes Prosefionales")

    /* val PATH: String = args(0)*/
    val PATH: String = path.route

    val STATS_LEVEL: Int = statsLevel.number

    val PARTITION_ID: String = partitionId.column

    val sparkSession: SparkSession = Spark.createSession()

    val df = Spark.readParquet(sparkSession, PATH)

    dfStats(df, STATS_LEVEL, PARTITION_ID)

    logger.warn("Caso 2 application ends. GOOD BYE")

  }

  case class Path(route: String)
  val path = Path("D:\\Indra\\Chamba\\BCP\\Capacitación y seguimiento\\Ejercicios\\m_desmoneda\\000000_0.snappy.parquet")

  case class PartitionId(column: String)
  val partitionId = PartitionId("fecactualizacionregistro")

  case class StatsLevel(number: Int)
  val statsLevel = StatsLevel(4)

  def dfStats(df: DataFrame, STATS_LEVEL: Int, PARTITION_ID: String):Unit = {

    val PARTITION_NUM: Int = getNumPartition(df)

    if (isGreaterThanZero(STATS_LEVEL)) {
      Printer.greaterThanZero(df, PARTITION_NUM)
    }

    STATS_LEVEL match {
      case 2 => Printer.two(df)
      case 3 => Printer.three(df, PARTITION_ID, PARTITION_NUM)
      case 4 => Printer.four(df, PARTITION_ID, PARTITION_NUM)
      case 8 => Printer.eight(df)
    }
  }


  def getNumPartition(df: DataFrame): Int = {
    df.rdd.getNumPartitions
  }

  def isGreaterThanZero(STATS_LEVEL: Int): Boolean = {
    STATS_LEVEL > 0
  }

}
