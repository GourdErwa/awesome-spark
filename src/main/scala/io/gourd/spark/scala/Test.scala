package io.gourd.spark.scala

/**
  * @author Li.Wei by 2019/11/21
  */
object Test {

  def main(args: Array[String]): Unit = {

    println(getAllValidBatches(2, 3))
    println(nextCompactionBatchId(2, 3))
  }

  def getAllValidBatches(batchId: Long, compactInterval: Long): Seq[Long] = {
    assert(batchId >= 0)
    val start = math.max(0, (batchId + 1) / compactInterval * compactInterval - 1)
    start to batchId
  }

  /**
    * Returns the next compaction batch id after `batchId`.
    */
  def nextCompactionBatchId(batchId: Long, compactInterval: Long): Long = {
    (batchId + compactInterval + 1) / compactInterval * compactInterval - 1
  }
}
