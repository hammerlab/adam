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
package org.bdgenomics.adam.converters

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro._

class FragmentConverterSuite
  extends ADAMFunSuite {

  test("build a fragment collector and convert to a read") {
    val fcOpt = FragmentCollector(NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg").build())
      .setFragmentSequence("ACACACAC")
      .setFragmentStartPosition(0L)
      .build())
    assert(fcOpt.isDefined)
    val (builtContig, builtFragment) = fcOpt.get

    ==(builtContig.getContigName, "ctg")
    ==(builtFragment.fragments.length, 1)
    val (fragmentRegion, fragmentString) = builtFragment.fragments.head
    ==(fragmentRegion, ReferenceRegion("ctg", 0L, 8L))
    ==(fragmentString, "ACACACAC")

    val convertedReads = FragmentConverter.convertFragment((builtContig, builtFragment))
    ==(convertedReads.size, 1)
    val convertedRead = convertedReads.head

    ==(convertedRead.getSequence, "ACACACAC")
    ==(convertedRead.getContigName, "ctg")
    ==(convertedRead.getStart, 0)
    ==(convertedRead.getEnd, 8)
  }

  test("if a fragment isn't associated with a contig, don't get a fragment collector") {
    val fcOpt = FragmentCollector(NucleotideContigFragment.newBuilder().build())
    assert(fcOpt.isEmpty)
  }

  test("convert an rdd of discontinuous fragments, all from the same contig") {
    val rdd = sc.parallelize(Seq(NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg").build())
      .setFragmentSequence("ACACACAC")
      .setFragmentStartPosition(0L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg").build())
      .setFragmentSequence("AATTCCGGCCTTAA")
      .setFragmentStartPosition(14L)
      .build()))

    val reads = FragmentConverter.convertRdd(rdd)
      .collect()

    ==(reads.length, 2)
    val firstRead = reads.filter(_.getStart == 0L).head
    val secondRead = reads.filter(_.getStart != 0L).head

    ==(firstRead.getSequence, "ACACACAC")
    ==(firstRead.getContigName, "ctg")
    ==(firstRead.getStart, 0L)
    ==(firstRead.getEnd, 8L)
    ==(secondRead.getSequence, "AATTCCGGCCTTAA")
    ==(secondRead.getContigName, "ctg")
    ==(secondRead.getStart, 14L)
    ==(secondRead.getEnd, 28L)
  }

  test("convert an rdd of contiguous fragments, all from the same contig") {
    val rdd = sc.parallelize(Seq(NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg").build())
      .setFragmentSequence("ACACACAC")
      .setFragmentStartPosition(0L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg").build())
      .setFragmentSequence("TGTGTG")
      .setFragmentStartPosition(8L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg").build())
      .setFragmentSequence("AATTCCGGCCTTAA")
      .setFragmentStartPosition(14L)
      .build()))

    val reads = FragmentConverter.convertRdd(rdd)
      .collect()

    ==(reads.length, 1)
    val read = reads(0)
    ==(read.getSequence, "ACACACACTGTGTGAATTCCGGCCTTAA")
    ==(read.getContigName, "ctg")
    ==(read.getStart, 0L)
    ==(read.getEnd, 28L)
  }

  test("convert an rdd of varied fragments from multiple contigs") {
    val rdd = sc.parallelize(Seq(NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg1").build())
      .setFragmentSequence("ACACACAC")
      .setFragmentStartPosition(0L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg1").build())
      .setFragmentSequence("TGTGTG")
      .setFragmentStartPosition(8L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg1").build())
      .setFragmentSequence("AATTCCGGCCTTAA")
      .setFragmentStartPosition(14L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg2").build())
      .setFragmentSequence("ACACACAC")
      .setFragmentStartPosition(0L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg2").build())
      .setFragmentSequence("AATTCCGGCCTTAA")
      .setFragmentStartPosition(14L)
      .build(), NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder().setContigName("ctg3").build())
      .setFragmentSequence("AATTCCGGCCTTAA")
      .setFragmentStartPosition(14L)
      .build()))

    val reads = FragmentConverter.convertRdd(rdd)
      .collect()

    ==(reads.length, 4)

    val ctg1Reads = reads.filter(_.getContigName == "ctg1")
    ==(ctg1Reads.length, 1)

    val ctg1Read = ctg1Reads.head
    ==(ctg1Read.getSequence, "ACACACACTGTGTGAATTCCGGCCTTAA")
    ==(ctg1Read.getContigName, "ctg1")
    ==(ctg1Read.getStart, 0L)
    ==(ctg1Read.getEnd, 28L)

    val ctg2Reads = reads.filter(_.getContigName == "ctg2")
    ==(ctg2Reads.length, 2)

    val firstCtg2Read = ctg2Reads.filter(_.getStart == 0L).head
    val secondCtg2Read = ctg2Reads.filter(_.getStart != 0L).head

    ==(firstCtg2Read.getSequence, "ACACACAC")
    ==(firstCtg2Read.getContigName, "ctg2")
    ==(firstCtg2Read.getStart, 0L)
    ==(firstCtg2Read.getEnd, 8L)
    ==(secondCtg2Read.getSequence, "AATTCCGGCCTTAA")
    ==(secondCtg2Read.getContigName, "ctg2")
    ==(secondCtg2Read.getStart, 14L)
    ==(secondCtg2Read.getEnd, 28L)

    val ctg3Reads = reads.filter(_.getContigName == "ctg3")
    ==(ctg3Reads.length, 1)

    val ctg3Read = ctg3Reads.head
    ==(ctg3Read.getSequence, "AATTCCGGCCTTAA")
    ==(ctg3Read.getContigName, "ctg3")
    ==(ctg3Read.getStart, 14L)
    ==(ctg3Read.getEnd, 28L)
  }
}
