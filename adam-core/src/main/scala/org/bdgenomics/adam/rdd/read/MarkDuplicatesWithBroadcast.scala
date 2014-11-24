package org.bdgenomics.adam.rdd.read

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.DuplicateReadInfo
import org.bdgenomics.formats.avro.AlignmentRecord
import org.apache.spark.rdd.PairRDDFunctions


object MarkDuplicatesWithBroadcast {

  def apply(rdd: RDD[AlignmentRecord]): RDD[AlignmentRecord] = {

    val duplicateReads = findDuplicateReads(rdd)
    rdd.keyBy(_.getReadName).join(duplicateReads.keyBy(_.getReadName))

  }

  def findDuplicateReads(rdd: RDD[AlignmentRecord]) : RDD[DuplicateReadInfo] = {

  }

}
