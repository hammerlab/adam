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
import org.bdgenomics.adam.projections.TranscriptEffectField._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.TestSaveArgs
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ TranscriptEffect, VariantAnnotationMessage }

class TranscriptEffectFieldSuite extends ADAMFunSuite {

  test("Use projection when reading parquet transcript effects") {
    val path = tmpFile("transcriptEffects.parquet")
    val rdd = sc.parallelize(Seq(TranscriptEffect.newBuilder()
      .setAlternateAllele("A")
      .setEffects(ImmutableList.of("SO:0002012"))
      .setGeneName("HLA-A")
      .setGeneId("ENSG00000206503")
      .setFeatureType("transcript")
      .setFeatureId("ENST00000396634.5")
      .setBiotype("Protein_coding")
      .setRank(1)
      .setTotal(1)
      .setGenomicHgvs("gA>T")
      .setTranscriptHgvs("cA>U")
      .setProteinHgvs("pG>A")
      .setCdnaPosition(1)
      .setCdnaLength(100)
      .setCdsPosition(2)
      .setCdsLength(200)
      .setProteinPosition(3)
      .setProteinLength(300)
      .setDistance(42)
      .setMessages(ImmutableList.of(VariantAnnotationMessage.WARNING_TRANSCRIPT_INCOMPLETE))
      .build()))
    rdd.saveAsParquet(TestSaveArgs(path))

    val projection = Projection(
      alternateAllele,
      effects,
      geneName,
      geneId,
      featureType,
      featureId,
      biotype,
      rank,
      total,
      genomicHgvs,
      transcriptHgvs,
      proteinHgvs,
      cdnaPosition,
      cdnaLength,
      cdsPosition,
      cdsLength,
      proteinPosition,
      proteinLength,
      distance,
      messages
    )

    val transcriptEffects: RDD[TranscriptEffect] = sc.loadParquet(path, projection = Some(projection))
    ==(transcriptEffects.count(), 1)
    ==(transcriptEffects.first.getAlternateAllele, "A")
    ==(transcriptEffects.first.getEffects.get(0), "SO:0002012")
    ==(transcriptEffects.first.getGeneName, "HLA-A")
    ==(transcriptEffects.first.getGeneId, "ENSG00000206503")
    ==(transcriptEffects.first.getFeatureType, "transcript")
    ==(transcriptEffects.first.getFeatureId, "ENST00000396634.5")
    ==(transcriptEffects.first.getBiotype, "Protein_coding")
    ==(transcriptEffects.first.getRank, 1)
    ==(transcriptEffects.first.getTotal, 1)
    ==(transcriptEffects.first.getGenomicHgvs, "gA>T")
    ==(transcriptEffects.first.getTranscriptHgvs, "cA>U")
    ==(transcriptEffects.first.getProteinHgvs, "pG>A")
    ==(transcriptEffects.first.getCdnaPosition, 1)
    ==(transcriptEffects.first.getCdnaLength, 100)
    ==(transcriptEffects.first.getCdsPosition, 2)
    ==(transcriptEffects.first.getCdsLength, 200)
    ==(transcriptEffects.first.getProteinPosition, 3)
    ==(transcriptEffects.first.getProteinLength, 300)
    ==(transcriptEffects.first.getDistance, 42)
    assert(transcriptEffects.first.getMessages.contains(VariantAnnotationMessage.WARNING_TRANSCRIPT_INCOMPLETE))
  }
}
