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
package org.bdgenomics.adam.projections

import com.google.common.collect.ImmutableList
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.projections.VariantAnnotationField._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.TestSaveArgs
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ TranscriptEffect, VariantAnnotation }

class VariantAnnotationFieldSuite extends ADAMFunSuite {

  test("Use projection when reading parquet variant annotations") {
    val path = tmpFile("variantAnnotations.parquet")
    val rdd = sc.parallelize(Seq(VariantAnnotation.newBuilder()
      .setAncestralAllele("T")
      .setAlleleCount(42)
      .setReadDepth(10)
      .setForwardReadDepth(4)
      .setReverseReadDepth(6)
      .setReferenceReadDepth(5)
      .setReferenceForwardReadDepth(2)
      .setReferenceReverseReadDepth(3)
      .setAlleleFrequency(20.0f)
      .setCigar("M")
      .setDbSnp(true)
      .setHapMap2(true)
      .setHapMap3(true)
      .setValidated(true)
      .setThousandGenomes(true)
      .setSomatic(false)
      .setTranscriptEffects(ImmutableList.of(TranscriptEffect.newBuilder()
        .setEffects(ImmutableList.of("SO:0002012"))
        .setFeatureType("transcript")
        .setFeatureId("ENST00000396634.5")
        .build()))
      .build()))
    rdd.saveAsParquet(TestSaveArgs(path))

    val projection = Projection(
      ancestralAllele,
      alleleCount,
      readDepth,
      forwardReadDepth,
      reverseReadDepth,
      referenceReadDepth,
      referenceForwardReadDepth,
      referenceReverseReadDepth,
      alleleFrequency,
      cigar,
      dbSnp,
      hapMap2,
      hapMap3,
      validated,
      thousandGenomes,
      somatic,
      transcriptEffects,
      attributes
    )

    val variantAnnotations: RDD[VariantAnnotation] = sc.loadParquet(path, projection = Some(projection))
    ==(variantAnnotations.count(), 1)
    ==(variantAnnotations.first.getAncestralAllele, "T")
    ==(variantAnnotations.first.getAlleleCount, 42)
    ==(variantAnnotations.first.getReadDepth, 10)
    ==(variantAnnotations.first.getForwardReadDepth, 4)
    ==(variantAnnotations.first.getReverseReadDepth, 6)
    ==(variantAnnotations.first.getReferenceReadDepth, 5)
    ==(variantAnnotations.first.getReferenceForwardReadDepth, 2)
    ==(variantAnnotations.first.getReferenceReverseReadDepth, 3)
    ==(variantAnnotations.first.getAlleleFrequency, 20.0f)
    ==(variantAnnotations.first.getCigar, "M")
    ==(variantAnnotations.first.getDbSnp, true)
    ==(variantAnnotations.first.getHapMap2, true)
    ==(variantAnnotations.first.getHapMap3, true)
    ==(variantAnnotations.first.getValidated, true)
    ==(variantAnnotations.first.getThousandGenomes, true)
    ==(variantAnnotations.first.getSomatic, false)
    ==(variantAnnotations.first.getTranscriptEffects.get(0).getFeatureId, "ENST00000396634.5")
  }
}
