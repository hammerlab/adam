/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.rdd.read

import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ ReferencePositionPair, ReferencePositionWithOrientation, SingleReadBucket }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.AlignmentRecord

object DuplicateReadInfo {
  def apply(record: AlignmentRecord): DuplicateReadInfo = {
    DuplicateReadInfo(
      if (record.getContig != null) record.getContig.getContigName else null,
      if (record.getStart != null) record.getStart else -1,
      record.getPrimaryAlignment,
      record.getRecordGroupName,
      record.getRecordGroupLibrary,
      record.getMateMapped,
      record.getReadNegativeStrand,
      record.getReadPaired,
      record.getMateMapped,
      record.getReadName,
      record.getQual.toCharArray.map(q => q - 33)
    )
  }
}

case class DuplicateReadInfo(contigName: String,
                             getStart: Long,
                             getPrimaryAlignment: Boolean,
                             getRecordGroupName: String,
                             getRecordGroupLibrary: String,
                             getReadMapped: Boolean,
                             getReadNegativeStrand: Boolean,
                             getReadPaired: Boolean,
                             getMateMapped: Boolean,
                             getReadName: String,
                             qualityScores: Array[Int]) {

  // Return the 5 prime position.
  def fivePrimePosition: Option[Long] = {
    if (getReadMapped) {
      if (getReadNegativeStrand) Some(getStart + qualityScores.length) else Some(getStart)
    } else {
      None
    }
  }
}

object DuplicateReadSingleReadBucket extends Logging {
  def apply(rdd: RDD[DuplicateReadInfo]): RDD[DuplicateReadSingleReadBucket] = {
    rdd.groupBy(p => (p.getRecordGroupName, p.getReadName))
      .map(kv => {
        val (_, reads) = kv

        // split by mapping
        val (mapped, unmapped) = reads.partition(_.getReadMapped)
        val (primaryMapped, secondaryMapped) = mapped.partition(_.getPrimaryAlignment)

        // TODO: consider doing validation here (e.g. read says mate mapped but it doesn't exist)
        new DuplicateReadSingleReadBucket(primaryMapped, secondaryMapped, unmapped)
      })
  }
}

case class DuplicateReadSingleReadBucket(primaryMapped: Iterable[DuplicateReadInfo] = Seq.empty,
                                         secondaryMapped: Iterable[DuplicateReadInfo] = Seq.empty,
                                         unmapped: Iterable[DuplicateReadInfo] = Seq.empty) {
  // Note: not a val in order to save serialization/memory cost
  def allReads = {
    primaryMapped ++ secondaryMapped ++ unmapped
  }
}

private[rdd] object MarkDuplicatesWithBroadcast extends Serializable {

  private def markReadsInBucket(bucket: SingleReadBucket, primaryAreDups: Boolean, secondaryAreDups: Boolean) {
    bucket.primaryMapped.foreach(read => {
      read.setDuplicateRead(primaryAreDups)
    })
    bucket.secondaryMapped.foreach(read => {
      read.setDuplicateRead(secondaryAreDups)
    })
    bucket.unmapped.foreach(read => {
      read.setDuplicateRead(false)
    })
  }

  // Calculates the sum of the phred scores that are greater than or equal to 15
  def score(record: DuplicateReadInfo): Int = {
    record.qualityScores.filter(15 <=).sum
  }

  private def scoreBucket(bucket: DuplicateReadSingleReadBucket): Int = {
    bucket.primaryMapped.map(score).sum
  }

  private def markReads(reads: Iterable[(ReferencePositionPair, SingleReadBucket)], areDups: Boolean) {
    markReads(reads, primaryAreDups = areDups, secondaryAreDups = areDups, ignore = None)
  }

  private def markReads(reads: Iterable[(ReferencePositionPair, SingleReadBucket)], primaryAreDups: Boolean, secondaryAreDups: Boolean,
                        ignore: Option[(ReferencePositionPair, SingleReadBucket)] = None) {
    reads.foreach(read => {
      if (ignore.isEmpty || read != ignore.get) {
        markReadsInBucket(read._2, primaryAreDups, secondaryAreDups)
      }

    })
  }

  def apply(rdd: RDD[AlignmentRecord]): RDD[AlignmentRecord] = {

    // Group by library and left position
    def leftPositionAndLibrary(p: (ReferencePositionPair, DuplicateReadSingleReadBucket)): (Option[ReferencePositionWithOrientation], String) = {
      (p._1.read1refPos, p._2.allReads.head.getRecordGroupLibrary)
    }

    // Group by right position
    def rightPosition(p: (ReferencePositionPair, DuplicateReadSingleReadBucket)): Option[ReferencePositionWithOrientation] = {
      p._1.read2refPos
    }
    //    val duplicateReadInfoProjection = Projection(
    //      AlignmentRecordField.readName,
    //      AlignmentRecordField.readName,
    //      AlignmentRecordField.start,
    //      AlignmentRecordField.readMapped,
    //
    //      AlignmentRecordField.recordGroupName,
    //      AlignmentRecordField.recordGroupLibrary,
    //      AlignmentRecordField.readPaired,
    //      AlignmentRecordField.mateNegativeStrand,
    //      AlignmentRecordField.mateMapped
    //    )

    val projectedReads = rdd.map(DuplicateReadInfo(_))
    val duplicateReads = DuplicateReadSingleReadBucket(projectedReads)
      .keyBy(ReferencePositionPair(_)).groupBy(leftPositionAndLibrary).flatMap(kv => {

        val leftPos: Option[ReferencePositionWithOrientation] = kv._1._1
        val readsAtLeftPos: Iterable[(ReferencePositionPair, DuplicateReadSingleReadBucket)] = kv._2

        leftPos match {

          // These are all unmapped reads. There is no way to determine if they are duplicates
          case None => Seq.empty
          // These reads have their left position mapped
          case Some(leftPosWithOrientation) =>

            val readsByRightPos = readsAtLeftPos.groupBy(rightPosition)
            val groupCount = readsByRightPos.size
            readsByRightPos.flatMap(e => {

              val rightPos = e._1
              val reads = e._2

              val groupIsFragments = rightPos.isEmpty

              // We have no pairs (only fragments) if the current group is a group of fragments
              // and there is only one group in total
              val onlyFragments = groupIsFragments && groupCount == 1

              // If there are only fragments then score the fragments. Otherwise, if there are not only
              // fragments (there are pairs as well) mark all fragments as duplicates.
              // If the group does not contain fragments (it contains pairs) then always score it.
              if (onlyFragments || !groupIsFragments) {
                // Find the highest-scoring read and mark it as not a duplicate. Mark all the other reads in this group as duplicates.
                val highestScoringRead = reads.max(ScoreOrdering)
                reads.flatMap(_._2.allReads.filter(r => r != highestScoringRead._2.primaryMapped.head))
              } else {
                reads.flatMap(_._2.allReads)
              }
            })

        }
      })
    val keyedDuplicateReads = duplicateReads.keyBy(read => (read.getRecordGroupName, read.getReadName)).collectAsMap()
    val broadcastDuplicateReads = rdd.sparkContext.broadcast(keyedDuplicateReads)
    //    val readsJoinedDuplicates = rdd
    //      .keyBy(r => (r.getRecordGroupName, r.getReadName))
    //      .leftOuterJoin(keyedDuplicateReads)

    rdd.map(read => {
      val duplicateOpt = broadcastDuplicateReads.value.get((read.getRecordGroupName, read.getReadName))
      duplicateOpt match {
        case None => read
        case Some(duplicate) => {
          read.setDuplicateRead(true)
          read
        }
      }
    }
    )
  }

  private object ScoreOrdering extends Ordering[(ReferencePositionPair, DuplicateReadSingleReadBucket)] {
    override def compare(x: (ReferencePositionPair, DuplicateReadSingleReadBucket), y: (ReferencePositionPair, DuplicateReadSingleReadBucket)): Int = {
      // This is safe because scores are Ints
      scoreBucket(x._2) - scoreBucket(y._2)
    }
  }

}