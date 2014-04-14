package org.bdgenomics.adam.rdd

import org.bdgenomics.adam.rich.DecadentRead
import org.bdgenomics.adam.models.ReferencePosition
import org.bdgenomics.adam.avro.{ ADAMRecord, ADAMContig, ADAMResiduePileup, ADAMResidue }
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext._

private[rdd] class Reads2ResiduePileup(createSecondaryAlignments: Boolean = false) extends Serializable with Logging {

  def buildResiduePileup(reference: (String, ReferencePosition), residues: Seq[ADAMResidue]): ADAMResiduePileup = {

    val referenceBase = reference._1
    val referencePos = reference._2
    val contig = ADAMContig.newBuilder
      .setContigName(referencePos.referenceName)
      .build
    ADAMResiduePileup.newBuilder
      .setReferenceBase(referenceBase)
      .setPosition(referencePos.pos)
      .setResidues(residues.toList)
      .setCountAtPosition(residues.size)
      .setContig(contig)
      .build()
  }

  /*
   * Convert reads to pileup by grouping residues by reference position
   */
  def readToPileups(reads: RDD[ADAMRecord]): RDD[ADAMResiduePileup] = {
    val richReads = DecadentRead.cloy(reads)
    val residues = richReads
      .flatMap(read =>
        read.residues.map(b =>
          ((b.referenceBase.get.toString, b.referencePosition), b.toPileupResidue())))

    residues
      .groupBy(_._1) //group by reference base and position
      .map(kv => (kv._1, kv._2.map(_._2))) //drop key fields
      .map(Function.tupled(buildResiduePileup _))

  }

  /*
   * An optimized function to convert reads to pileup by grouping residues by reference position
   * If the reads are already sorted, we don't need to reshuffle all of the residues by reference position
   * Rather, this will broadcast the overhanging residues and create pileups from residues on a single partition
   */

  def readToPartitionedPileups(reads: RDD[ADAMRecord]): RDD[ADAMResiduePileup] = {
    val sortedReads: RDD[(ReferencePosition, DecadentRead)] =
      if (reads.partitioner.isEmpty) {
        // Sort reads to partition by genomic region
        // We are not using
        reads.keyBy(r => ReferencePosition(r).get).sortByKey().mapValues(DecadentRead.apply)
        //reads.adamSortReadsByReferencePosition()
      } else {
        reads.keyBy(r => ReferencePosition(r).get).mapValues(DecadentRead.apply)
      }

    val orderedPartitioner = sortedReads.partitioner.get

    val residues =
      sortedReads.flatMapValues(
        read =>
          read.residues.map(b =>
            (orderedPartitioner.getPartition(b.referencePosition), b.referenceBase.get.toString, b.referencePosition, b.toPileupResidue())))

    val partitionAndResidues =
      residues.mapPartitions(_.map(_._2), preservesPartitioning = true) // drop read reference position key

    /*
     * Collect residues where the read partition does not match the locus partition
     */
    val overhangResidues =
      partitionAndResidues.mapPartitionsWithIndex(
        (currentPartition: Int, residues: Iterator[(Int, String, ReferencePosition, ADAMResidue)]) => {
          residues.filter(_._1 != currentPartition).map(r => (r._1, r))
        }).collect()

    /*
     * Create a broadcasted map from partition to the overhanging partitions
     */
    val overhangByPosition: Broadcast[Map[Int, Seq[(Int, String, ReferencePosition, ADAMResidue)]]] =
      reads.sparkContext.broadcast(
        overhangResidues
          .groupBy(_._1) // Group by partition
          .map(kv => (kv._1, kv._2.map(_._2).toSeq))) // Create map from position to list of residues

    def buildPartitionedPileups(currentPartition: Int, residues: Iterator[(Int, String, ReferencePosition, ADAMResidue)]): Iterator[ADAMResiduePileup] = {

      val properResidues = residues.filter(_._1 == currentPartition) ++ overhangByPosition.value.getOrElse(currentPartition, Seq.empty)
      properResidues.toSeq
        .groupBy(b => (b._2, b._3)) // group by reference base and reference positions
        .map(kv => (kv._1, kv._2.map(_._4))) //drop grouping keys
        .map(Function.tupled(buildResiduePileup _)).toIterator
    }

    partitionAndResidues.mapPartitionsWithIndex(buildPartitionedPileups, preservesPartitioning = true)

  }
}

