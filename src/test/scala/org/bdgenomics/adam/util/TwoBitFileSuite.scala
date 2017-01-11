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
package org.bdgenomics.adam.util

import java.io.File

import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.hammerlab.genomics.reference.test.{ ContigNameUtil, LocusUtil }
import org.scalactic.ConversionCheckedTripleEquals
import org.scalatest.Matchers

class TwoBitFileSuite
  extends ADAMFunSuite
    with Matchers
    with ConversionCheckedTripleEquals
    with LocusUtil
    with ContigNameUtil {

  test("correctly read sequence from .2bit file") {
    val file = new File(testFile("hg19.chrM.2bit"))
    val byteAccess = new LocalFileByteAccess(file)
    val twoBitFile = new TwoBitFile(byteAccess)
    twoBitFile.numSeq should === (1)
    twoBitFile.seqRecords.toSeq.length should === (1)
    twoBitFile.extract(ReferenceRegion("hg19_chrM", 0, 10)) should === ("GATCACAGGT")
    twoBitFile.extract(ReferenceRegion("hg19_chrM", 503, 513)) should === ("CATCCTACCC")
    twoBitFile.extract(ReferenceRegion("hg19_chrM", 16561, 16571)) should === ("CATCACGATG")
  }

  test("correctly return masked sequences from .2bit file") {
    val file = new File(testFile("hg19.chrM.2bit"))
    val byteAccess = new LocalFileByteAccess(file)
    val twoBitFile = new TwoBitFile(byteAccess)
    twoBitFile.extract(ReferenceRegion("hg19_chrM", 0, 10), true) should === ("GATCACAGGT")
    twoBitFile.extract(ReferenceRegion("hg19_chrM", 2600, 2610), true) should === ("taatcacttg")
  }

  test("correctly return Ns from .2bit file") {
    val file = new File(testFile("human_g1k_v37_chr1_59kb.2bit"))
    val byteAccess = new LocalFileByteAccess(file)
    val twoBitFile = new TwoBitFile(byteAccess)
    twoBitFile.extract(ReferenceRegion("1", 9990, 10010), true) should === ("NNNNNNNNNNTAACCCTAAC")
  }

  test("correctly calculates sequence dictionary") {
    val file = new File(testFile("hg19.chrM.2bit"))
    val byteAccess = new LocalFileByteAccess(file)
    val twoBitFile = new TwoBitFile(byteAccess)
    val dict = twoBitFile.sequences
    dict.records.length should === (1)
    dict.records.head.length should === (16571)
  }
}
