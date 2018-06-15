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
package org.bdgenomics.adam.algorithms.consensus

import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.AlignmentRecord

class ConsensusGeneratorFromKnownsSuite extends ADAMFunSuite {

  def cg(sc: SparkContext): ConsensusGenerator = {
    val path = testFile("random.vcf")
    ConsensusGenerator.fromKnownIndels(sc.loadVariants(path))
  }

  test("no consensuses for empty target") {
    val c = cg(sc)
    assert(c.findConsensus(Iterable.empty).isEmpty)
  }

  test("no consensuses for reads that don't overlap a target") {
    val c = cg(sc)
    val read = AlignmentRecord.newBuilder
      .setStart(1L)
      .setEnd(2L)
      .setContigName("notAContig")
      .build
    assert(c.findConsensus(Iterable(new RichAlignmentRecord(read))).isEmpty)
  }

  test("return a consensus for read overlapping a single target") {
    val c = cg(sc)
    val read = AlignmentRecord.newBuilder
      .setStart(19189L)
      .setEnd(19191L)
      .setContigName("2")
      .build
    val consensuses = c.findConsensus(Iterable(new RichAlignmentRecord(read)))
    ==(consensuses.size, 1)
    ==(consensuses.head.consensus, "")
    ==(consensuses.head.index.referenceName, "2")
    ==(consensuses.head.index.start, 19190L)
    ==(consensuses.head.index.end, 19192L)
  }
}

