package Common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc

object Printer {

  def greaterThanZero(df: DataFrame, PARTITION_NUM: Int): Unit = {
    val dfSizeByte: BigInt = Size.inBytes(df)
    val sizeMb: Float = Size.inMb(df)
    val sizeEstimator: Float = Size.estimatorMb(df)

    printStats(sizeMb, sizeEstimator, PARTITION_NUM)
  }

  def two(df: DataFrame): Unit = {
    printStats(df)
  }

  def three(df: DataFrame, PARTITION_ID: String, PARTITION_NUM: Int): Unit = {
    println(s"*** STATS *** Df Partitioon Info")
    printStats(df, PARTITION_ID, PARTITION_NUM)
  }

  def four(df: DataFrame, PARTITION_ID: String, PARTITION_NUM: Int) = {
    two(df)
    three(df, PARTITION_ID, PARTITION_NUM)
  }
  def eight(df: DataFrame): Unit = {
    println(s"*** STATS *** Show data:")
    df.show()
  }

  def printStats(sizeMb: Float , sizeEstimator: Float,  PARTITION_NUM: Int): Unit = {
    println("*** STATS *** Df Stats PlanSizeMB:"+sizeMb+",  EstimatorSizeMB:"+sizeEstimator+", PartitionNum: "+PARTITION_NUM
      +", PartitionSizeMB:"+sizeMb / PARTITION_NUM+", PartitionSizeMB:"+sizeEstimator / PARTITION_NUM)
  }

  def printStats(df: DataFrame): Unit  = {
    println(s"*** STATS *** Df Partition record count ${df.count()}")
  }

  def printStats(df: DataFrame, PARTITION_ID: String, PARTITION_NUM: Int): Unit = {
    println(s"*** STATS *** Df Partitioon Info")
    showPartitionInfo(df, PARTITION_ID, PARTITION_NUM)
  }

  def showPartitionInfo(df: DataFrame, PARTITION_ID: String, PARTITION_NUM: Int): Unit = {
    df
      .groupBy(PARTITION_ID)
      .count()
      .orderBy(desc("count"))
      .show(validatePartition(PARTITION_NUM)): Unit
  }

  def validatePartition(PARTITION_NUM: Int): Int = {
    if (isGreaterThanZero(PARTITION_NUM)) {
      PARTITION_NUM
    }
    else {
      200
    }
  }

  def isGreaterThanZero(PARTITION_NUM: Int): Boolean = {
    PARTITION_NUM > 0
  }

}
