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
package org.bdgenomics.adam.models

import org.bdgenomics.adam.algorithms.consensus.Consensus
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.Variant
import org.hammerlab.genomics.reference.ContigName
import org.hammerlab.genomics.reference.test.ClearContigNames
import org.scalatest.Matchers

class IndelTableSuite
  extends ADAMFunSuite
    with ClearContigNames {

  implicit def makeContigNameTuple[T](t: (String, T)): (ContigName, T) = (t._1, t._2)

  // indel table containing a 1 bp deletion at chr1, pos 1000
  lazy val indelTable = new IndelTable(Map("1" -> Iterable(Consensus("", ReferenceRegion("1", 1000L, 1002L)))))

  test("check for indels in a region with known indels") {
    indelTable.getIndelsInRegion(ReferenceRegion("1", 0L, 2000L)).length should === (1)
  }

  test("check for indels in a contig that doesn't exist") {
    indelTable.getIndelsInRegion(ReferenceRegion("0", 0L, 1L)).length should === (0)
  }

  test("check for indels in a region without known indels") {
    indelTable.getIndelsInRegion(ReferenceRegion("1", 1002L, 1005L)).length should === (0)
  }

  sparkTest("build indel table from rdd of variants") {
    val ins = Variant.newBuilder()
      .setContigName("1")
      .setStart(1000L)
      .setReferenceAllele("A")
      .setAlternateAllele("ATT")
      .build()
    val del = Variant.newBuilder()
      .setContigName("2")
      .setStart(50L)
      .setReferenceAllele("ACAT")
      .setAlternateAllele("A")
      .build()

    val rdd = sc.parallelize(Seq(ins, del))

    val table = IndelTable(rdd)

    // check insert
    val insT = table.getIndelsInRegion(ReferenceRegion("1", 1000L, 1010L))
    insT.length should === (1)
    insT.head.consensus should === ("TT")
    insT.head.index.referenceName should === ("1")
    insT.head.index.start should === (1001)
    insT.head.index.end should === (1002)

    // check delete
    val delT = table.getIndelsInRegion(ReferenceRegion("2", 40L, 60L))
    delT.length should === (1)
    delT.head.consensus should === ("")
    delT.head.index.referenceName should === ("2")
    delT.head.index.start should === (51)
    delT.head.index.end should === (54)
  }

}
