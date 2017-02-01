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

import org.scalatest.{ FunSuite, Matchers }
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig, Genotype, Variant }
import org.hammerlab.genomics.reference.test.ClearContigNames
import org.scalactic.ConversionCheckedTripleEquals

class ReferencePositionSuite
  extends FunSuite
    with Matchers
    with ConversionCheckedTripleEquals
    with ClearContigNames {

  test("create reference position from mapped read") {
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .build

    val read = AlignmentRecord.newBuilder()
      .setContigName(contig.getContigName)
      .setStart(1L)
      .setReadMapped(true)
      .build()

    val refPos = ReferencePosition(read)

    refPos.referenceName should === ("chr1")
    refPos.pos should === (1L)
  }

  test("create reference position from variant") {
    val variant = Variant.newBuilder()
      .setContigName("chr10")
      .setReferenceAllele("A")
      .setAlternateAllele("T")
      .setStart(10L)
      .build()

    val refPos = ReferencePosition(variant)

    refPos.referenceName should === ("chr10")
    refPos.pos should === (10L)
  }

  test("create reference position from genotype") {
    val variant = Variant.newBuilder()
      .setStart(100L)
      .setContigName("chr10")
      .setReferenceAllele("A")
      .setAlternateAllele("T")
      .build()
    val genotype = Genotype.newBuilder()
      .setVariant(variant)
      .setStart(100L)
      .setEnd(101L)
      .setContigName("chr10")
      .setSampleId("NA12878")
      .build()

    val refPos = ReferencePosition(genotype)

    refPos.referenceName should === ("chr10")
    refPos.pos should === (100L)
  }
}
