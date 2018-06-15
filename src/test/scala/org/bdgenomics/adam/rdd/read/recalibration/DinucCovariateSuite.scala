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
package org.bdgenomics.adam.rdd.read.recalibration

import org.bdgenomics.formats.avro.AlignmentRecord
import org.scalatest.FunSuite

class DinucCovariateSuite extends FunSuite {

  val dc = new DinucCovariate

  test("computing dinucleotide pairs for a single base sequence should return (N,N)") {
    val dinucs = dc.fwdDinucs("A")
    ==(dinucs.size, 1)
    ==(dinucs(0), ('N', 'N'))
  }

  test("compute dinucleotide pairs for a string of all valid bases") {
    val dinucs = dc.fwdDinucs("AGCGT")
    ==(dinucs.size, 5)
    ==(dinucs(0), ('N', 'N'))
    ==(dinucs(1), ('A', 'G'))
    ==(dinucs(2), ('G', 'C'))
    ==(dinucs(3), ('C', 'G'))
    ==(dinucs(4), ('G', 'T'))
  }

  test("compute dinucleotide pairs for a string with an N") {
    val dinucs = dc.fwdDinucs("AGNGT")
    ==(dinucs.size, 5)
    ==(dinucs(0), ('N', 'N'))
    ==(dinucs(1), ('A', 'G'))
    ==(dinucs(2), ('N', 'N'))
    ==(dinucs(3), ('N', 'N'))
    ==(dinucs(4), ('G', 'T'))
  }

  test("compute covariates for a read on the negative strand") {
    val read = AlignmentRecord.newBuilder()
      .setSequence("AGCCTNGT")
      .setQual("********")
      .setReadNegativeStrand(true)
      .setReadMapped(true)
      .setStart(10L)
      .setMapq(50)
      .setCigar("8M")
      .build
    val covariates = dc.compute(read)
    ==(covariates.size, 8)
    ==(covariates(0), ('C', 'T'))
    ==(covariates(1), ('G', 'C'))
    ==(covariates(2), ('G', 'G'))
    ==(covariates(3), ('A', 'G'))
    ==(covariates(4), ('N', 'N'))
    ==(covariates(5), ('N', 'N'))
    ==(covariates(6), ('A', 'C'))
    ==(covariates(7), ('N', 'N'))
  }

  test("compute covariates for a read on the positive strand") {
    val read = AlignmentRecord.newBuilder()
      .setSequence("ACNAGGCT")
      .setQual("********")
      .setReadNegativeStrand(false)
      .setReadMapped(true)
      .setStart(10L)
      .setMapq(50)
      .setCigar("8M")
      .build
    val covariates = dc.compute(read)
    ==(covariates.size, 8)
    ==(covariates(0), ('N', 'N'))
    ==(covariates(1), ('A', 'C'))
    ==(covariates(2), ('N', 'N'))
    ==(covariates(3), ('N', 'N'))
    ==(covariates(4), ('A', 'G'))
    ==(covariates(5), ('G', 'G'))
    ==(covariates(6), ('G', 'C'))
    ==(covariates(7), ('C', 'T'))
  }
}
