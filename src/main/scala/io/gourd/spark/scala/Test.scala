package io.gourd.spark.scala

import org.apache.spark.sql.execution.streaming.CompactibleFileStreamLog.isCompactionBatch

/**
  * @author Li.Wei by 2019/11/21
  */
object Test {

  def main(args: Array[String]): Unit = {

    val compactionBatchId = 5609
    val compactInterval = 10

    println(getValidBatchesBeforeCompactionBatch(compactionBatchId, compactInterval))
    println(getAllValidBatches(compactionBatchId, compactInterval))
    println(nextCompactionBatchId(compactionBatchId, compactInterval))
  }

  def getValidBatchesBeforeCompactionBatch(
                                            compactionBatchId: Long,
                                            compactInterval: Int): Seq[Long] = {
    assert(isCompactionBatch(compactionBatchId, compactInterval),
      s"$compactionBatchId is not a compaction batch")
    (math.max(0, compactionBatchId - compactInterval)) until compactionBatchId
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
