package Common

import org.apache.spark.sql.DataFrame
import org.apache.spark.util.SizeEstimator

object Size {

  val DIVIDER: Float = 1024.toFloat / 1024.toFloat

  def inBytes(df: DataFrame): BigInt = {
    val sizeInBytes = getOptimizedPlanInBytes(df)
    sizeInBytes
  }

  def getOptimizedPlanInBytes(df: DataFrame): BigInt = {
    df.queryExecution.optimizedPlan.stats.sizeInBytes
  }

  def inMb(df: DataFrame): Float = {
    val sizeInBytes = inBytes(df)
    val sizeMb = turnIntoMb(sizeInBytes)
    sizeMb
  }

  def turnIntoMb(sizeInBytes: BigInt): Float = {
    val mbSize = sizeInBytes.toFloat / DIVIDER
    mbSize
  }

  def estimatorMb(df: DataFrame): Float = {
    val estimatorBytes = SizeEstimator.estimate(df)
    val estimatorMb = turnIntoMb(estimatorBytes)
    estimatorMb
  }

}
