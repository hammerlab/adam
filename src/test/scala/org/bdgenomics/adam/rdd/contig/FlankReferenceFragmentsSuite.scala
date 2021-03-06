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

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{ Contig, NucleotideContigFragment }
import org.hammerlab.genomics.reference.test.ClearContigNames
import org.hammerlab.test.Suite

class FlankReferenceFragmentsSuite
  extends Suite
    with ClearContigNames {

  test("don't put flanks on non-adjacent fragments") {
    val testIter = Iterator((ReferenceRegion("chr1", 0L, 10L),
      NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder()
        .setContigName("chr1")
        .build())
      .setFragmentSequence("AAAAATTTTT")
      .setFragmentStartPosition(0L)
      .setFragmentEndPosition(9L)
      .build()), (ReferenceRegion("chr1", 20L, 30L),
      NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder()
        .setContigName("chr1")
        .build())
      .setFragmentSequence("CCCCCGGGGG")
      .setFragmentStartPosition(20L)
      .setFragmentEndPosition(29L)
      .build()))

    val fragments = FlankReferenceFragments.flank(testIter, 5).toSeq

    ==(fragments.size, 2)
    fragments.foreach(_.getFragmentSequence.length === 10)
    ==(fragments(0).getFragmentSequence, "AAAAATTTTT")
    ==(fragments(0).getFragmentStartPosition, 0L)
    ==(fragments(0).getFragmentEndPosition, 9L)
    ==(fragments(1).getFragmentSequence, "CCCCCGGGGG")
    ==(fragments(1).getFragmentStartPosition, 20L)
    ==(fragments(1).getFragmentEndPosition, 29L)
  }

  test("put flanks on adjacent fragments") {
    val testIter = Iterator((ReferenceRegion("chr1", 0L, 10L),
      NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder()
        .setContigName("chr1")
        .build())
      .setFragmentSequence("AAAAATTTTT")
      .setFragmentStartPosition(0L)
      .setFragmentEndPosition(9L)
      .build()), (ReferenceRegion("chr1", 10L, 20L),
      NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder()
        .setContigName("chr1")
        .build())
      .setFragmentSequence("NNNNNUUUUU")
      .setFragmentStartPosition(10L)
      .setFragmentEndPosition(19L)
      .build()), (ReferenceRegion("chr1", 20L, 30L),
      NucleotideContigFragment.newBuilder()
      .setContig(Contig.newBuilder()
        .setContigName("chr1")
        .build())
      .setFragmentSequence("CCCCCGGGGG")
      .setFragmentStartPosition(20L)
      .setFragmentEndPosition(29L)
      .build()))

    val fragments = FlankReferenceFragments.flank(testIter, 5).toSeq

    ==(fragments.size, 3)
    ==(fragments(0).getFragmentSequence, "AAAAATTTTTNNNNN")
    ==(fragments(0).getFragmentStartPosition, 0L)
    ==(fragments(0).getFragmentEndPosition, 14L)
    ==(fragments(1).getFragmentSequence, "TTTTTNNNNNUUUUUCCCCC")
    ==(fragments(1).getFragmentStartPosition, 5L)
    ==(fragments(1).getFragmentEndPosition, 24L)
    ==(fragments(2).getFragmentSequence, "UUUUUCCCCCGGGGG")
    ==(fragments(2).getFragmentStartPosition, 15L)
    ==(fragments(2).getFragmentEndPosition, 29L)
  }
}
