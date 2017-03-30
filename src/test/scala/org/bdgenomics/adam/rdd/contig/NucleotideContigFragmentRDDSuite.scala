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
package org.bdgenomics.adam.rdd.contig

import java.nio.file.Files.lines

import scala.collection.JavaConverters._
import org.bdgenomics.adam.models._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro._
import org.hammerlab.genomics.reference.test.ClearContigNames
import org.hammerlab.genomics.reference.test.LociConversions.intToLocus
import org.scalatest.Matchers

import scala.collection.mutable.ListBuffer

class NucleotideContigFragmentRDDSuite
  extends ADAMFunSuite {

  def chr1(length: Int = 7): Contig =
    Contig
      .newBuilder
      .setContigName("chr1")
      .setContigLength(length)
      .build

  def writeFastaLines(fragments: NucleotideContigFragment*): Seq[String] = {
    val rdd = NucleotideContigFragmentRDD(sc.parallelize(fragments))

    val outputFastaFile = tmpLocation(".fa")
    rdd.transform(_.coalesce(1)).saveAsFasta(outputFastaFile)

    lines(outputFastaFile.resolve("part-00000"))
      .iterator
      .asScala
      .toSeq
  }

  sparkTest("generate sequence dict from fasta") {
    val contig0 =
      Contig
        .newBuilder
        .setContigName("chr0")
        .setContigLength(1000L)
        .setReferenceURL("http://bigdatagenomics.github.io/chr0.fa")
        .build

    val ctg0 =
      NucleotideContigFragment
        .newBuilder()
        .setContig(contig0)
        .build()

    val ctg1 =
      NucleotideContigFragment
        .newBuilder()
        .setContig(chr1(900))
        .build()

    val rdd = NucleotideContigFragmentRDD(sc.parallelize(List(ctg0, ctg1)))

    rdd.sequences("chr0") should be(
      Some(
        SequenceRecord(
          "chr0",
          1000,
          url = "http://bigdatagenomics.github.io/chr0.fa"
        )
      )
    )

    rdd.sequences("chr1") should be(
      Some(
        SequenceRecord(
          "chr1",
          900
        )
      )
    )
  }

  sparkTest("recover reference string from a single contig fragment") {
    val sequence = "ACTGTAC"
    val fragment =
      NucleotideContigFragment
        .newBuilder()
        .setContig(chr1())
        .setFragmentSequence(sequence)
        .setFragmentNumber(0)
        .setFragmentStartPosition(0L)
        .setNumberOfFragmentsInContig(1)
        .build()

    val region = ReferenceRegion(fragment).get

    val rdd = NucleotideContigFragmentRDD(sc.parallelize(List(fragment)))

    assert(rdd.extract(region) === "ACTGTAC")
  }

  sparkTest("recover trimmed reference string from a single contig fragment") {
    val sequence = "ACTGTAC"
    val fragment =
      NucleotideContigFragment
        .newBuilder()
        .setContig(chr1())
        .setFragmentSequence(sequence)
        .setFragmentNumber(0)
        .setFragmentStartPosition(0L)
        .setNumberOfFragmentsInContig(1)
        .build()

    val region = new ReferenceRegion("chr1", 1L, 6L)

    val rdd = NucleotideContigFragmentRDD(sc.parallelize(List(fragment)))

    assert(rdd.extract(region) === "CTGTA")
  }

  sparkTest("recover reference string from multiple contig fragments") {
    val contig2 = Contig.newBuilder
      .setContigName("chr2")
      .setContigLength(11L)
      .build

    val sequence = "ACTGTACTC"
    val sequence0 = sequence.take(7) // ACTGTAC
    val sequence1 = sequence.slice(3, 8) // GTACT
    val sequence2 = sequence.takeRight(6).reverse // CTCATG
    val fragment0 =
      NucleotideContigFragment
        .newBuilder()
        .setContig(chr1())
        .setFragmentSequence(sequence0)
        .setFragmentNumber(0)
        .setFragmentStartPosition(0L)
        .setNumberOfFragmentsInContig(1)
        .build()

    val fragment1 =
      NucleotideContigFragment
        .newBuilder()
        .setContig(contig2)
        .setFragmentSequence(sequence1)
        .setFragmentNumber(0)
        .setFragmentStartPosition(0L)
        .setNumberOfFragmentsInContig(2)
        .build()

    val fragment2 =
      NucleotideContigFragment
        .newBuilder()
        .setContig(contig2)
        .setFragmentSequence(sequence2)
        .setFragmentNumber(1)
        .setFragmentStartPosition(5L)
        .setNumberOfFragmentsInContig(2)
        .build()

    val region0 = ReferenceRegion(fragment0).get
    val region1 = ReferenceRegion(fragment1).get.merge(ReferenceRegion(fragment2).get)

    val rdd = NucleotideContigFragmentRDD(sc.parallelize(List(fragment0,
      fragment1,
      fragment2)))

    assert(rdd.extract(region0) === "ACTGTAC")
    assert(rdd.extract(region1) === "GTACTCTCATG")
  }

  sparkTest("recover trimmed reference string from multiple contig fragments") {
    val contig2 =
      Contig
        .newBuilder
        .setContigName("chr2")
        .setContigLength(11L)
        .build

    val sequence = "ACTGTACTC"
    val sequence0 = sequence.take(7) // ACTGTAC
    val sequence1 = sequence.slice(3, 8) // GTACT
    val sequence2 = sequence.takeRight(6).reverse // CTCATG

    val fragment0 =
      NucleotideContigFragment
        .newBuilder()
        .setContig(chr1())
        .setFragmentSequence(sequence0)
        .setFragmentNumber(0)
        .setFragmentStartPosition(0L)
        .setNumberOfFragmentsInContig(1)
        .build()

    val fragment1 =
      NucleotideContigFragment
        .newBuilder()
        .setContig(contig2)
        .setFragmentSequence(sequence1)
        .setFragmentNumber(0)
        .setFragmentStartPosition(0L)
        .setNumberOfFragmentsInContig(2)
        .build()

    val fragment2 =
      NucleotideContigFragment
        .newBuilder()
        .setContig(contig2)
        .setFragmentSequence(sequence2)
        .setFragmentNumber(1)
        .setFragmentStartPosition(5L)
        .setNumberOfFragmentsInContig(2)
        .build()

    val region0 = new ReferenceRegion("chr1", 1L, 6L)
    val region1 = new ReferenceRegion("chr2", 3L, 9L)

    val rdd = NucleotideContigFragmentRDD(sc.parallelize(List(fragment0,
      fragment1,
      fragment2)))

    assert(rdd.extract(region0) === "CTGTA")
    assert(rdd.extract(region1) === "CTCTCA")
  }

  sparkTest("testing nondeterminism from reduce when recovering referencestring") {
    var fragments: ListBuffer[NucleotideContigFragment] = new ListBuffer[NucleotideContigFragment]()
    for (a ← 0 to 1000) {
      val seq = "A"
      fragments +=
        NucleotideContigFragment
          .newBuilder()
          .setContig(chr1(1000))
          .setFragmentStartPosition(a)
          .setFragmentSequence(seq)
          .build()
    }
    var passed = true
    val rdd = NucleotideContigFragmentRDD(sc.parallelize(fragments.toList))
    try {
      rdd.extract(new ReferenceRegion("chr1", 0L, 1000L))
    } catch {
      case _: AssertionError => passed = false
    }
    passed should be(true)
  }

  sparkTest("save single contig fragment as FASTA text file") {
    val fragment =
      NucleotideContigFragment
        .newBuilder()
        .setContig(chr1())
        .setFragmentSequence("ACTGTAC")
        .setFragmentNumber(0)
        .setNumberOfFragmentsInContig(1)
        .build

    writeFastaLines(fragment) should be(Seq(">chr1", "ACTGTAC"))
  }

  sparkTest("save single contig fragment with description as FASTA text file") {
    val fragment =
      NucleotideContigFragment
        .newBuilder()
        .setContig(chr1())
        .setDescription("description")
        .setFragmentSequence("ACTGTAC")
        .setFragmentNumber(0)
        .setNumberOfFragmentsInContig(1)
        .build

    writeFastaLines(fragment) should be(Seq(">chr1 description", "ACTGTAC"))
  }

  sparkTest("save single contig fragment with null fields as FASTA text file") {
    val fragment =
      NucleotideContigFragment
        .newBuilder()
        .setContig(chr1())
        .setFragmentSequence("ACTGTAC")
        .setFragmentNumber(null)
        .setFragmentStartPosition(null)
        .setFragmentEndPosition(null)
        .setNumberOfFragmentsInContig(null)
        .build

    writeFastaLines(fragment) should be(Seq(">chr1", "ACTGTAC"))
  }

  sparkTest("save single contig fragment with null fragment number as FASTA text file") {
    val fragment =
      NucleotideContigFragment
        .newBuilder()
        .setContig(chr1())
        .setFragmentSequence("ACTGTAC")
        .setFragmentNumber(null)
        .setFragmentStartPosition(null)
        .setFragmentEndPosition(null)
        .setNumberOfFragmentsInContig(1)
        .build

    writeFastaLines(fragment) should be(Seq(">chr1", "ACTGTAC"))
  }

  sparkTest("save single contig fragment with null number of fragments in contig as FASTA text file") {
    val fragment =
      NucleotideContigFragment
        .newBuilder()
        .setContig(chr1())
        .setFragmentSequence("ACTGTAC")
        .setFragmentNumber(0)
        .setFragmentStartPosition(null)
        .setFragmentEndPosition(null)
        .setNumberOfFragmentsInContig(null)
        .build

    writeFastaLines(fragment) should be(Seq(">chr1", "ACTGTAC"))
  }

  sparkTest("save multiple contig fragments from same contig as FASTA text file") {
    val contig = chr1(21)

    val fragment0 =
      NucleotideContigFragment
        .newBuilder()
        .setContig(contig)
        .setFragmentSequence("ACTGTAC")
        .setFragmentNumber(0)
        .setNumberOfFragmentsInContig(3)
        .build

    val fragment1 =
      NucleotideContigFragment
        .newBuilder()
        .setContig(contig)
        .setFragmentSequence("GCATATC")
        .setFragmentNumber(1)
        .setNumberOfFragmentsInContig(3)
        .build

    val fragment2 =
      NucleotideContigFragment
        .newBuilder()
        .setContig(contig)
        .setFragmentSequence("CTGATCG")
        .setFragmentNumber(2)
        .setNumberOfFragmentsInContig(3)
        .build

    writeFastaLines(fragment0, fragment1, fragment2) should be(
      Seq(
        ">chr1 fragment 1 of 3",
        "ACTGTAC",
        ">chr1 fragment 2 of 3",
        "GCATATC",
        ">chr1 fragment 3 of 3",
        "CTGATCG"
      )
    )
  }

  sparkTest("save multiple contig fragments with description from same contig as FASTA text file") {
    val contig = chr1(21)

    val fragment0 =
      NucleotideContigFragment
        .newBuilder()
        .setContig(contig)
        .setDescription("description")
        .setFragmentSequence("ACTGTAC")
        .setFragmentNumber(0)
        .setNumberOfFragmentsInContig(3)
        .build

    val fragment1 =
      NucleotideContigFragment
        .newBuilder()
        .setContig(contig)
        .setDescription("description")
        .setFragmentSequence("GCATATC")
        .setFragmentNumber(1)
        .setNumberOfFragmentsInContig(3)
        .build

    val fragment2 =
      NucleotideContigFragment
        .newBuilder()
        .setContig(contig)
        .setDescription("description")
        .setFragmentSequence("CTGATCG")
        .setFragmentNumber(2)
        .setNumberOfFragmentsInContig(3)
        .build

    writeFastaLines(fragment0, fragment1, fragment2) should be(
      Seq(
        ">chr1 description fragment 1 of 3",
        "ACTGTAC",
        ">chr1 description fragment 2 of 3",
        "GCATATC",
        ">chr1 description fragment 3 of 3",
        "CTGATCG"
      )
    )
  }

  sparkTest("merge single contig fragment null fragment number") {
    val fragment =
      NucleotideContigFragment
        .newBuilder()
        .setContig(chr1())
        .setFragmentSequence("ACTGTAC")
        .setFragmentNumber(null)
        .setFragmentStartPosition(null)
        .setFragmentEndPosition(null)
        .setNumberOfFragmentsInContig(null)
        .build

    val rdd = NucleotideContigFragmentRDD(sc.parallelize(List(fragment)))
    val merged = rdd.mergeFragments()

    assert(merged.rdd.map(_.getFragmentSequence()).collect === Array("ACTGTAC"))
  }

  sparkTest("merge single contig fragment number zero") {
    val fragment =
      NucleotideContigFragment
        .newBuilder()
        .setContig(chr1())
        .setFragmentSequence("ACTGTAC")
        .setFragmentNumber(0)
        .setFragmentStartPosition(0L)
        .setFragmentEndPosition(6L)
        .setNumberOfFragmentsInContig(1)
        .build

    val rdd = NucleotideContigFragmentRDD(sc.parallelize(List(fragment)))
    val merged = rdd.mergeFragments()

    assert(merged.rdd.count == 1L)
    assert(merged.rdd.first.getFragmentSequence() === "ACTGTAC")
  }

  sparkTest("merge multiple contig fragments") {
    val contig2 =
      Contig
        .newBuilder
        .setContigName("chr2")
        .setContigLength(11L)
        .build

    val sequence = "ACTGTACTC"
    val sequence0 = sequence.take(7)  // ACTGTAC
    val sequence1 = sequence.slice(3, 8)  // GTACT
    val sequence2 = sequence.takeRight(6).reverse  // CTCATG

    val fragment0 =
      NucleotideContigFragment
        .newBuilder()
        .setContig(chr1())
        .setFragmentSequence(sequence0)
        .setFragmentNumber(0)
        .setFragmentStartPosition(0L)
        .setFragmentEndPosition(sequence0.length - 1L)
        .setNumberOfFragmentsInContig(1)
        .build()

    val fragment1 =
      NucleotideContigFragment
        .newBuilder()
        .setContig(contig2)
        .setFragmentSequence(sequence1)
        .setFragmentNumber(0)
        .setFragmentStartPosition(0L)
        .setFragmentEndPosition(sequence1.length - 1L)
        .setNumberOfFragmentsInContig(2)
        .build()

    val fragment2 =
      NucleotideContigFragment
        .newBuilder()
        .setContig(contig2)
        .setFragmentSequence(sequence2)
        .setFragmentNumber(1)
        .setFragmentStartPosition(5L)
        .setFragmentEndPosition(sequence2.length - 1L)
        .setNumberOfFragmentsInContig(2)
        .build()

    val rdd =
      NucleotideContigFragmentRDD(
        sc.parallelize(
          List(
            fragment2,
            fragment1,
            fragment0
          )
        )
      )

    val merged = rdd.mergeFragments()

    assert(merged.rdd.count == 2L)

    merged.rdd.collect.map(_.getFragmentSequence) should be(
      Array(
        "ACTGTAC",
        "GTACTCTCATG"
      )
    )
  }
}
