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
package org.bdgenomics.adam.rdd

import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceDictionary, SequenceRecord }
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig }
import org.hammerlab.genomics.reference.test.ClearContigNames
import org.hammerlab.genomics.reference.test.LociConversions.intToLocus

class InnerShuffleRegionJoinSuite
  extends ADAMFunSuite
    with ClearContigNames {

  val partitionSize = 3
  var seqDict: SequenceDictionary = _

  before {
    seqDict = SequenceDictionary(
      SequenceRecord("chr1", 5, url = "test://chrom1"),
      SequenceRecord("chr2", 5, url = "tes=t://chrom2"))
  }

  test("Overlapping reference regions") {
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(5L)
      .setReferenceURL("test://chrom1")
      .build

    val built = AlignmentRecord.newBuilder()
      .setContigName(contig.getContigName)
      .setStart(1L)
      .setReadMapped(true)
      .setCigar("1M")
      .setEnd(2L)
      .build()

    val record1 = built
    val record2 = AlignmentRecord.newBuilder(built).setStart(3L).setEnd(4L).build()
    val baseRecord = AlignmentRecord.newBuilder(built).setCigar("4M").setEnd(5L).build()

    val baseRdd = sc.parallelize(Seq(baseRecord)).keyBy(ReferenceRegion.unstranded)
    val recordsRdd = sc.parallelize(Seq(record1, record2)).keyBy(ReferenceRegion.unstranded)

    assert(
      InnerShuffleRegionJoin[AlignmentRecord, AlignmentRecord](seqDict, partitionSize, sc)
        .partitionAndJoin(
          baseRdd,
          recordsRdd
        )
        .aggregate(true)(
          InnerShuffleRegionJoinSuite.merge,
          InnerShuffleRegionJoinSuite.and
        )
    )

    ==(
      InnerShuffleRegionJoin[AlignmentRecord, AlignmentRecord](seqDict, partitionSize, sc)
        .partitionAndJoin(
          baseRdd,
          recordsRdd
        )
        .count(),
      2
    )
  }

  test("Multiple reference regions do not throw exception") {
    val contig1 = Contig.newBuilder
      .setContigName("chr1")
      .setContigLength(5L)
      .setReferenceURL("test://chrom1")
      .build

    val contig2 = Contig.newBuilder
      .setContigName("chr2")
      .setContigLength(5L)
      .setReferenceURL("test://chrom2")
      .build

    val builtRef1 = AlignmentRecord.newBuilder()
      .setContigName(contig1.getContigName)
      .setStart(1L)
      .setReadMapped(true)
      .setCigar("1M")
      .setEnd(2L)
      .build()
    val builtRef2 = AlignmentRecord.newBuilder()
      .setContigName(contig2.getContigName)
      .setStart(1)
      .setReadMapped(true)
      .setCigar("1M")
      .setEnd(2L)
      .build()

    val record1 = builtRef1
    val record2 = AlignmentRecord.newBuilder(builtRef1).setStart(3L).setEnd(4L).build()
    val record3 = builtRef2
    val baseRecord1 = AlignmentRecord.newBuilder(builtRef1).setCigar("4M").setEnd(5L).build()
    val baseRecord2 = AlignmentRecord.newBuilder(builtRef2).setCigar("4M").setEnd(5L).build()

    val baseRdd = sc.parallelize(Seq(baseRecord1, baseRecord2)).keyBy(ReferenceRegion.unstranded)
    val recordsRdd = sc.parallelize(Seq(record1, record2, record3)).keyBy(ReferenceRegion.unstranded)

    assert(
      InnerShuffleRegionJoin[AlignmentRecord, AlignmentRecord](seqDict, partitionSize, sc)
        .partitionAndJoin(
          baseRdd,
          recordsRdd
        )
        .aggregate(true)(
          InnerShuffleRegionJoinSuite.merge,
          InnerShuffleRegionJoinSuite.and
        )
    )

    ==(
      InnerShuffleRegionJoin[AlignmentRecord, AlignmentRecord](seqDict, partitionSize, sc)
        .partitionAndJoin(
          baseRdd,
          recordsRdd
        )
        .count(),
      3
    )
  }
}

object InnerShuffleRegionJoinSuite {
  def getReferenceRegion(record: AlignmentRecord): ReferenceRegion =
    ReferenceRegion.unstranded(record)

  def merge(prev: Boolean, next: (AlignmentRecord, AlignmentRecord)): Boolean =
    prev && getReferenceRegion(next._1).overlaps(getReferenceRegion(next._2))

  def count[T](prev: Int, next: (T, T)): Int =
    prev + 1

  def sum(value1: Int, value2: Int): Int =
    value1 + value2

  def and(value1: Boolean, value2: Boolean): Boolean =
    value1 && value2
}
